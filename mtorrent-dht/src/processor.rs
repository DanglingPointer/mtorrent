use super::cmds::{self, Command};
use super::kademlia;
use super::msgs::*;
use super::peers::TokenManager;
use super::queries::{self, IncomingQuery};
use super::tasks::*;
use super::u160::U160;
use crate::Config;
use crate::peers::PeerTable;
use futures_util::StreamExt;
use local_async_utils::prelude::*;
use mtorrent_utils::connctrl::ConnectControl;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::rc::Rc;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio::{select, time};

macro_rules! define {
    ($name:ident, $handle:expr) => {
        macro_rules! $name {
            ($f:expr) => {
                $handle.with(
                    #[inline(always)]
                    $f,
                )
            };
        }
    };
}

type RoutingTable = kademlia::RoutingTable<16>;
pub(super) type BoxRoutingTable = Box<RoutingTable>;

pub struct Processor {
    nodes: LocalShared<BoxRoutingTable>,
    peers: PeerTable,
    token_mgr: TokenManager,
    peer_sender: local_channel::Sender<(SocketAddr, U160)>,
    peer_receiver: local_channel::Receiver<(SocketAddr, U160)>,
    _client: queries::Client,
    shutdown_signal: Rc<Notify>,
    cnt_ctrl: ConnectControl<PingCtx>,
    config: Config,
    config_dir: PathBuf,
}

impl Processor {
    pub fn new(config_dir: PathBuf, client: queries::Client) -> Self {
        let config = Config::load(&config_dir).unwrap_or_else(|e| {
            log::info!("Failed to load config ({e}), using defaults");
            Config::default()
        });
        let nodes = LocalShared::new(RoutingTable::new_boxed(config.local_id));
        let shutdown_signal = Rc::new(Notify::const_new());
        let ctx = PingCtx {
            nodes: nodes.clone(),
            client: client.clone(),
            shutdown_signal: shutdown_signal.clone(),
        };
        let (peer_sender, peer_receiver) = local_channel::channel();
        Self {
            nodes,
            peers: PeerTable::new(),
            token_mgr: TokenManager::new(),
            peer_sender,
            peer_receiver,
            _client: client,
            shutdown_signal,
            cnt_ctrl: ConnectControl::new(usize::MAX, ctx),
            config,
            config_dir,
        }
    }

    pub async fn run(mut self, mut queries: queries::Server, mut commands: cmds::Server) {
        // do all DNS resolution first because it's a blocking call
        let bootstrapping_nodes: Vec<SocketAddr> = self
            .config
            .nodes
            .iter()
            .filter_map(|node| node.to_socket_addrs().ok())
            .flatten()
            .filter(SocketAddr::is_ipv4)
            .collect();

        // do bootstrapping for the next 10 sec
        if !bootstrapping_nodes.is_empty() {
            let deadline = Instant::now() + sec!(10);

            for addr in &bootstrapping_nodes {
                if let Some(permit) = self.cnt_ctrl.try_acquire_permit(*addr) {
                    launch_periodic_ping(*addr, permit, self.cnt_ctrl.split_off());
                }
            }

            let _ = time::timeout_at(deadline, async {
                while let Some(incoming_query) = queries.0.next().await {
                    self.handle_query(incoming_query);
                }
            })
            .await;
        }

        // now we can handle commands
        loop {
            select! {
                biased;
                cmd = commands.next() => match cmd {
                    Some(cmd) => self.handle_command(cmd),
                    None => break,
                },
                query = queries.0.next() => match query {
                    Some(query) => self.handle_query(query),
                    None => break,
                },
                peer_target = self.peer_receiver.next() => match peer_target {
                    Some((peer, target)) => self.peers.add_record(target, peer),
                    None => unreachable!(),
                }
            }
        }
    }

    fn handle_query(&mut self, query: IncomingQuery) {
        define!(with_ctx, self.nodes);

        let addr = *query.source_addr();
        with_ctx!(|rt| respond_to_incoming_query(rt, &mut self.peers, &mut self.token_mgr, query));

        if let Some(permit) = self.cnt_ctrl.try_acquire_permit(addr) {
            launch_periodic_ping(addr, permit, self.cnt_ctrl.split_off());
        }
    }

    fn handle_command(&mut self, cmd: Command) {
        define!(with_rt, self.nodes);
        log::info!("Processing command: {cmd:?}");
        match cmd {
            Command::AddNode { addr } => {
                if let Some(permit) = self.cnt_ctrl.try_acquire_permit(addr) {
                    launch_periodic_ping(addr, permit, self.cnt_ctrl.split_off());
                }
            }
            Command::FindPeers {
                info_hash,
                callback,
                local_peer_port,
            } => {
                if self
                    .peers
                    .get_peers(info_hash)
                    .try_for_each(|addr| callback.try_send(*addr))
                    .is_ok()
                {
                    let ctx = Rc::new(SearchCtx {
                        target: info_hash,
                        local_peer_port,
                        cmd_result_sender: callback,
                        peer_sender: self.peer_sender.clone(),
                        cnt_ctrl: self.cnt_ctrl.split_off(),
                        queried_nodes: sealed::Set::new(),
                        discovered_peers: sealed::Set::new(),
                    });
                    let closest_nodes: Vec<_> = with_rt!(|rt| rt
                        .get_closest_nodes(&info_hash, RoutingTable::BUCKET_SIZE * 3)
                        .map(|node| node.addr)
                        .collect());
                    for node_addr in closest_nodes {
                        launch_peer_search(ctx.clone(), node_addr, 0);
                    }
                } else {
                    log::warn!("Not starting search as callback channel is closed");
                }
            }
        }
    }
}

impl Drop for Processor {
    fn drop(&mut self) {
        let connected_nodes: Vec<String> =
            self.nodes.with(|rt| rt.iter().map(|node| node.addr.to_string()).collect());

        log::info!("Processor shutting down, node_count = {}", connected_nodes.len());

        if !connected_nodes.is_empty() {
            self.config.nodes = connected_nodes;
            self.config.nodes.extend(Config::default().nodes);
        }
        if let Err(e) = self.config.save(&self.config_dir) {
            log::error!("Failed to save config: {e}");
        }

        self.shutdown_signal.notify_waiters();
    }
}

fn respond_to_incoming_query(
    rt: &BoxRoutingTable,
    peers: &mut PeerTable,
    token_mgr: &mut TokenManager,
    query: IncomingQuery,
) {
    let result = match query {
        IncomingQuery::Ping(ping) => ping.respond(PingResponse { id: *rt.local_id() }),
        IncomingQuery::FindNode(find_node) => {
            let mut nodes: Vec<_> = rt
                .get_closest_nodes(&find_node.args().target, 8)
                .filter_map(|node| match node.addr {
                    SocketAddr::V4(socket_addr_v4) => Some((node.id, socket_addr_v4)),
                    SocketAddr::V6(_) => None,
                })
                .take(8)
                .collect();
            if let Some(&exact_match) = nodes.iter().find(|(id, _)| *id == find_node.args().target)
            {
                nodes.clear();
                nodes.push(exact_match);
            }
            find_node.respond(FindNodeResponse {
                id: *rt.local_id(),
                nodes,
            })
        }
        IncomingQuery::GetPeers(get_peers) => {
            let token = token_mgr.generate_token_for(get_peers.source_addr());
            let peer_addrs: Vec<_> = peers
                .get_ipv4_peers(get_peers.args().info_hash)
                .take(128) // apprx to fit MTU
                .cloned()
                .collect();
            let response_data = if !peer_addrs.is_empty() {
                GetPeersResponseData::Peers(peer_addrs)
            } else {
                GetPeersResponseData::Nodes(
                    rt.get_closest_nodes(&get_peers.args().info_hash, 8)
                        .filter_map(|node| match node.addr {
                            SocketAddr::V4(socket_addr_v4) => Some((node.id, socket_addr_v4)),
                            SocketAddr::V6(_) => None,
                        })
                        .take(8)
                        .collect(),
                )
            };
            get_peers.respond(GetPeersResponse {
                id: *rt.local_id(),
                token: Some(token),
                data: response_data,
            })
        }
        IncomingQuery::AnnouncePeer(announce_peer) => {
            if !token_mgr
                .validate_token_from(announce_peer.source_addr(), &announce_peer.args().token)
            {
                announce_peer.respond_error(ErrorMsg {
                    error_code: ErrorCode::Generic,
                    error_msg: "Invalid token".to_owned(),
                })
            } else {
                let mut peer_addr = *announce_peer.source_addr();
                if let Some(port) = announce_peer.args().port {
                    peer_addr.set_port(port);
                }
                peers.add_record(announce_peer.args().info_hash, peer_addr);
                announce_peer.respond(AnnouncePeerResponse { id: *rt.local_id() })
            }
        }
    };
    if let Err(e) = result {
        log::warn!("Failed to respond to query: {e}");
    }
}
