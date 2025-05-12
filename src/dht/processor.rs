use super::cmds::{self, Command};
use super::health::{spawn_periodic_ping, Ctx};
use super::kademlia;
use super::msgs::*;
use super::peers::TokenManager;
use super::queries::{self, IncomingQuery};
use super::u160::U160;
use crate::dht::peers::PeerTable;
use crate::utils::connctrl::ConnectControl;
use futures::StreamExt;
use local_async_utils::local_sync::LocalShared;
use local_async_utils::shared::Shared;
use std::net::SocketAddr;
use std::rc::Rc;
use tokio::select;
use tokio::sync::Notify;

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

pub(super) type RoutingTable = Box<kademlia::RoutingTable<16>>;

pub struct Processor {
    nodes: LocalShared<RoutingTable>,
    peers: PeerTable,
    token_mgr: TokenManager,
    _client: queries::Client,
    shutdown_signal: Rc<Notify>,
    cnt_ctrl: ConnectControl<Ctx>,
}

impl Processor {
    pub fn new(local_id: U160, client: queries::Client) -> Self {
        let nodes = LocalShared::new(kademlia::RoutingTable::new_boxed(local_id));
        let shutdown_signal = Rc::new(Notify::const_new());
        let ctx = Ctx {
            nodes: nodes.clone(),
            client: client.clone(),
            shutdown_signal: shutdown_signal.clone(),
        };
        Self {
            nodes,
            peers: PeerTable::new(),
            token_mgr: TokenManager::new(),
            _client: client,
            shutdown_signal,
            cnt_ctrl: ConnectControl::new(usize::MAX, ctx),
        }
    }

    pub async fn run(mut self, mut queries: queries::Server, mut commands: cmds::Server) {
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
            }
        }
        self.shutdown_signal.notify_waiters();
        log::debug!(
            "Processor shutting down, node_count = {}",
            self.nodes.with(|rt| rt.node_count())
        );
    }

    fn handle_query(&mut self, query: IncomingQuery) {
        define!(with_ctx, self.nodes);

        let addr = *query.source_addr();
        with_ctx!(|rt| respond_to_incoming_query(rt, &mut self.peers, &mut self.token_mgr, query));

        if let Some(permit) = self.cnt_ctrl.try_acquire_permit(addr) {
            spawn_periodic_ping(addr, permit, self.cnt_ctrl.split_off());
        }
    }

    fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::AddNode { addr } => {
                if let Some(permit) = self.cnt_ctrl.try_acquire_permit(addr) {
                    spawn_periodic_ping(addr, permit, self.cnt_ctrl.split_off());
                }
            }
            Command::FindPeers {
                info_hash,
                callback,
            } => {
                for addr in self.peers.get_peers(&info_hash) {
                    let _ = callback
                        .try_send(*addr)
                        .inspect_err(|e| log::error!("Failed to respond to FindPeers cmd: {e}"));
                }

                todo!("spawn task finding peers")
            }
        }
    }
}

fn respond_to_incoming_query(
    rt: &RoutingTable,
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
            if nodes.first().is_some_and(|(id, _)| id == &find_node.args().target) {
                // exact match
                nodes.truncate(1);
            }
            find_node.respond(FindNodeResponse {
                id: *rt.local_id(),
                nodes,
            })
        }
        IncomingQuery::GetPeers(get_peers) => {
            let token = token_mgr.generate_token_for(get_peers.source_addr());
            let peer_addrs: Vec<_> =
                peers.get_ipv4_peers(&get_peers.args().info_hash).cloned().collect();
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
                peers.add_record(&announce_peer.args().info_hash, peer_addr);
                announce_peer.respond(AnnouncePeerResponse { id: *rt.local_id() })
            }
        }
    };
    if let Err(e) = result {
        log::error!("Failed to respond to query: {e:?}");
    }
}
