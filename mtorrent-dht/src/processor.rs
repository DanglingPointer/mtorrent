use super::cmds::{self, Command};
use super::kademlia;
use super::msgs::*;
use super::peers::TokenManager;
use super::queries::{self, IncomingQuery};
use super::tasks::*;
use super::u160::U160;
use crate::Config;
use crate::kademlia::Node;
use crate::peers::PeerTable;
use futures_util::StreamExt;
use local_async_utils::prelude::*;
use mtorrent_utils::{debug_stopwatch, warn_stopwatch};
use std::collections::HashSet;
use std::net::{SocketAddr, ToSocketAddrs};
use std::ops::ControlFlow::*;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::{select, task, time};
use tokio_util::sync::CancellationToken;

type RoutingTable = kademlia::RoutingTable<16>;
pub(super) type BoxRoutingTable = Box<RoutingTable>;

pub struct Processor {
    node_table: BoxRoutingTable,
    peers: PeerTable,
    token_mgr: TokenManager,
    known_nodes: HashSet<SocketAddr>,
    task_ctx: Rc<Ctx>,

    peer_sender: local_channel::Sender<(SocketAddr, U160)>,
    peer_receiver: local_channel::Receiver<(SocketAddr, U160)>,
    node_event_receiver: mpsc::Receiver<NodeEvent>,

    config: Config,
    config_dir: PathBuf,
    canceller: CancellationToken,
}

impl Processor {
    pub fn new(config_dir: PathBuf, client: queries::Client) -> Self {
        let (peer_sender, peer_receiver) = local_channel::channel();
        let (node_event_sender, node_event_receiver) = mpsc::channel(1024);

        let config = Config::load(&config_dir).unwrap_or_else(|e| {
            log::info!("Failed to load config ({e}), using defaults");
            Config::default()
        });
        let node_ctx = Rc::new(Ctx {
            client: client.clone(),
            event_reporter: node_event_sender,
            local_id: config.local_id,
        });
        let node_table = RoutingTable::new_boxed(config.local_id);

        Self {
            peers: PeerTable::new(),
            token_mgr: TokenManager::new(),
            peer_sender,
            peer_receiver,
            config,
            config_dir,
            task_ctx: node_ctx,
            node_event_receiver,
            node_table,
            known_nodes: HashSet::with_capacity(512),
            canceller: CancellationToken::new(),
        }
    }

    pub async fn run(mut self, mut queries: queries::Server, mut commands: cmds::Server) {
        macro_rules! handle_next_event {
            ($queries:expr $(,$commands:expr)?) => {
                select! {
                    biased;
                    $(cmd = $commands.next() => match cmd {
                        Some(cmd) => Continue(self.handle_command(cmd)),
                        None => Break(()),
                    },)?
                    event = self.node_event_receiver.recv() => match event {
                        Some(event) => Continue(self.handle_node_event(event)),
                        None => Break(()),
                    },
                    query = $queries.0.next() => match query {
                        Some(query) => Continue(self.handle_query(query)),
                        None => Break(()),
                    },
                    peer_target = self.peer_receiver.next() => match peer_target {
                        Some((peer, target)) => Continue(self.peers.add_record(target, peer)),
                        None => unreachable!(),
                    },
                }
            };
        }

        const BOOTSTRAP_TIMEOUT: Duration = sec!(10);
        const BOOTSTRAP_TARGET: usize = 200;

        // do all DNS resolution first because it will block the current thread
        let sw = warn_stopwatch!("DNS resolution of bootstrapping nodes");
        let bootstrapping_nodes: Vec<SocketAddr> = self
            .config
            .nodes
            .iter()
            .filter_map(|node| node.to_socket_addrs().ok())
            .flatten()
            .filter(SocketAddr::is_ipv4)
            .collect();
        drop(sw);

        // do bootstrapping for the next 10 sec
        if !bootstrapping_nodes.is_empty() {
            let _sw = debug_stopwatch!("Bootstrapping");

            for addr in bootstrapping_nodes {
                if self.known_nodes.insert(addr) {
                    task::spawn_local(
                        self.canceller
                            .clone()
                            .run_until_cancelled_owned(probe_node(addr, self.task_ctx.clone())),
                    );
                }
            }

            _ = time::timeout(BOOTSTRAP_TIMEOUT, async {
                while self.node_table.iter().count() < BOOTSTRAP_TARGET
                    && handle_next_event!(queries).is_continue()
                {}
            })
            .await;
        }

        while handle_next_event!(queries, commands).is_continue() {}
    }

    fn handle_node_event(&mut self, event: NodeEvent) {
        match event {
            NodeEvent::Discovered(node) => {
                if self.node_table.can_insert(&node.id) && self.known_nodes.insert(node.addr) {
                    task::spawn_local(
                        self.canceller.clone().run_until_cancelled_owned(probe_node(
                            node.addr,
                            self.task_ctx.clone(),
                        )),
                    );
                }
            }
            NodeEvent::Connected(node) => {
                if self.node_table.insert_node(&node.id, &node.addr) {
                    task::spawn_local(
                        self.canceller.clone().run_until_cancelled_owned(keep_alive_node(
                            node,
                            self.task_ctx.clone(),
                        )),
                    );
                } else {
                    self.known_nodes.remove(&node.addr);
                }
            }
            NodeEvent::Disconnected(node) => {
                self.known_nodes.remove(&node.addr);
                self.node_table.remove_node(&node.id);
            }
            NodeEvent::Unreachable(addr) => {
                self.known_nodes.remove(&addr);
            }
        }
    }

    fn handle_query(&mut self, query: IncomingQuery) {
        let node = Node {
            id: *query.node_id(),
            addr: *query.source_addr(),
        };

        let result = match query {
            IncomingQuery::Ping(ping) => ping.respond(PingResponse {
                id: self.task_ctx.local_id,
            }),
            IncomingQuery::FindNode(find_node) => {
                let mut nodes: Vec<_> = self
                    .node_table
                    .get_closest_nodes(&find_node.args().target, 8)
                    .filter_map(|node| match node.addr {
                        SocketAddr::V4(socket_addr_v4) => Some((node.id, socket_addr_v4)),
                        SocketAddr::V6(_) => None,
                    })
                    .take(8)
                    .collect();
                if let Some(&exact_match) =
                    nodes.iter().find(|(id, _)| *id == find_node.args().target)
                {
                    nodes.clear();
                    nodes.push(exact_match);
                }
                find_node.respond(FindNodeResponse {
                    id: self.task_ctx.local_id,
                    nodes,
                })
            }
            IncomingQuery::GetPeers(get_peers) => {
                let token = self.token_mgr.generate_token_for(get_peers.source_addr());
                let peer_addrs: Vec<_> = self
                    .peers
                    .get_ipv4_peers(get_peers.args().info_hash)
                    .take(128) // apprx to fit MTU
                    .cloned()
                    .collect();
                let response_data = if !peer_addrs.is_empty() {
                    GetPeersResponseData::Peers(peer_addrs)
                } else {
                    GetPeersResponseData::Nodes(
                        self.node_table
                            .get_closest_nodes(&get_peers.args().info_hash, 8)
                            .filter_map(|node| match node.addr {
                                SocketAddr::V4(socket_addr_v4) => Some((node.id, socket_addr_v4)),
                                SocketAddr::V6(_) => None,
                            })
                            .take(8)
                            .collect(),
                    )
                };
                get_peers.respond(GetPeersResponse {
                    id: self.task_ctx.local_id,
                    token: Some(token),
                    data: response_data,
                })
            }
            IncomingQuery::AnnouncePeer(announce_peer) => {
                if !self
                    .token_mgr
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
                    self.peers.add_record(announce_peer.args().info_hash, peer_addr);
                    announce_peer.respond(AnnouncePeerResponse {
                        id: self.task_ctx.local_id,
                    })
                }
            }
        };
        if let Err(e) = result {
            log::warn!("Failed to respond to query: {e}");
        }

        if self.node_table.can_insert(&node.id) && self.known_nodes.insert(node.addr) {
            task::spawn_local(
                self.canceller
                    .clone()
                    .run_until_cancelled_owned(probe_node(node.addr, self.task_ctx.clone())),
            );
        }
    }

    fn handle_command(&mut self, cmd: Command) {
        log::info!("Processing command: {cmd:?}");
        match cmd {
            Command::AddNode { addr } => {
                if self.known_nodes.insert(addr) {
                    task::spawn_local(
                        self.canceller
                            .clone()
                            .run_until_cancelled_owned(probe_node(addr, self.task_ctx.clone())),
                    );
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
                    .is_err()
                {
                    log::warn!("Not starting search as callback channel is closed");
                    return;
                }
                let search = SearchTask::new(
                    info_hash,
                    local_peer_port,
                    self.task_ctx.clone(),
                    callback,
                    self.peer_sender.clone(),
                );
                let initial_nodes: Vec<Node> = self
                    .node_table
                    .get_closest_nodes(&info_hash, RoutingTable::BUCKET_SIZE * 3)
                    .cloned()
                    .collect();
                task::spawn_local(
                    search.run(initial_nodes.into_iter(), self.canceller.child_token()),
                );
            }
        }
    }
}

impl Drop for Processor {
    fn drop(&mut self) {
        let connected_nodes: Vec<String> =
            self.node_table.iter().map(|node| node.addr.to_string()).collect();

        log::info!("Processor shutting down, node_count = {}", connected_nodes.len());

        if !connected_nodes.is_empty() {
            self.config.nodes = connected_nodes;
            self.config.nodes.extend(Config::default().nodes);
        }
        if let Err(e) = self.config.save(&self.config_dir) {
            log::error!("Failed to save config: {e}");
        }

        self.canceller.cancel();
    }
}
