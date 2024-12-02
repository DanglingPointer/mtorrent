use super::cmds::{self, Command};
use super::msgs::*;
use super::nodes::{Node, RoutingTable};
use super::peers::TokenManager;
use super::queries::{self, IncomingQuery};
use super::u160::U160;
use crate::dht::peers::PeerTable;
use crate::utils::local_sync::LocalShared;
use crate::utils::shared::Shared;
use crate::{debug_stopwatch, min};
use futures::StreamExt;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::{select, task, time};

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

struct SharedCtx {
    nodes: RoutingTable,
    peers: PeerTable,
}

pub struct Processor {
    ctx: LocalShared<SharedCtx>,
    token_mgr: TokenManager,
    client: queries::Client,
    shutdown_notify: Rc<Notify>,
}

impl Processor {
    pub fn new(local_id: U160, client: queries::Client) -> Self {
        Self {
            ctx: LocalShared::new(SharedCtx {
                nodes: RoutingTable::new(local_id),
                peers: PeerTable::new(),
            }),
            token_mgr: TokenManager::new(),
            client,
            shutdown_notify: Rc::new(Notify::const_new()),
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
        self.shutdown_notify.notify_waiters();
        log::debug!("Processor shutting down");
    }

    fn handle_query(&mut self, query: IncomingQuery) {
        define!(with_ctx, self.ctx);

        let existing_node = with_ctx!(|ctx| ctx
            .nodes
            .submit_query_from_known_node(query.node_id(), query.source_addr()));
        if !existing_node {
            self.spawn_periodic_ping(*query.source_addr());
        }
        with_ctx!(|ctx| respond_to_incoming_query(
            &ctx.nodes,
            &mut ctx.peers,
            &mut self.token_mgr,
            query
        ));
    }

    fn handle_command(&mut self, cmd: Command) {
        define!(with_ctx, self.ctx);

        match cmd {
            Command::AddNode { addr } => {
                if with_ctx!(|ctx| ctx.nodes.get_node_by_ip(&addr).is_none()) {
                    self.spawn_periodic_ping(addr);
                }
            }
            Command::FindPeers {
                info_hash,
                callback,
            } => {
                let peers: Vec<SocketAddr> =
                    with_ctx!(|ctx| ctx.peers.get_peers(&info_hash).cloned().collect());
                if !peers.is_empty() {
                    for addr in peers {
                        let _ = callback.try_send(addr).inspect_err(|e| {
                            log::error!("Failed to respond to FindPeers cmd: {e}")
                        });
                    }
                } else {
                    todo!("spawn task finding peers")
                }
            }
        }
    }

    fn spawn_periodic_ping(&self, node_addr: SocketAddr) {
        let shutdown_signal = self.shutdown_notify.clone();
        let ping_task =
            periodic_ping(self.ctx.project(|ctx| &mut ctx.nodes), self.client.clone(), node_addr);
        task::spawn_local(async move {
            select! {
                biased;
                _ = ping_task => (),
                _ = shutdown_signal.notified() => (),
            }
        });
    }
}

fn respond_to_incoming_query(
    rt: &RoutingTable,
    peers: &mut PeerTable,
    token_mgr: &mut TokenManager,
    query: IncomingQuery,
) {
    let result = match query {
        IncomingQuery::Ping(ping) => ping.respond(PingResponse {
            id: rt.local_id().clone(),
        }),
        IncomingQuery::FindNode(find_node) => {
            let mut nodes: Vec<_> = rt
                .closest_nodes(&find_node.args().target)
                .filter_map(|node| match node.addr() {
                    SocketAddr::V4(socket_addr_v4) => Some((node.id().clone(), *socket_addr_v4)),
                    SocketAddr::V6(_) => None,
                })
                .take(8)
                .collect();
            if nodes.first().is_some_and(|(id, _)| id == &find_node.args().target) {
                // exact match
                nodes.truncate(1);
            }
            find_node.respond(FindNodeResponse {
                id: rt.local_id().clone(),
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
                    rt.closest_nodes(&get_peers.args().info_hash)
                        .filter_map(|node| match node.addr() {
                            SocketAddr::V4(socket_addr_v4) => {
                                Some((node.id().clone(), *socket_addr_v4))
                            }
                            SocketAddr::V6(_) => None,
                        })
                        .take(8)
                        .collect(),
                )
            };
            get_peers.respond(GetPeersResponse {
                id: rt.local_id().clone(),
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
                announce_peer.respond(AnnouncePeerResponse {
                    id: rt.local_id().clone(),
                })
            }
        }
    };
    if let Err(e) = result {
        log::error!("Failed to respond to query: {e:?}");
    }
}

async fn periodic_ping(
    mut rt: impl Shared<Target = RoutingTable>,
    client: queries::Client,
    addr: SocketAddr,
) -> io::Result<()> {
    const PING_INTERVAL: Duration = min!(1);
    define!(with_rt, rt);

    macro_rules! send_ping {
        () => {
            client.ping(
                addr,
                PingArgs {
                    id: with_rt!(|rt| rt.local_id().clone()),
                },
            )
        };
    }
    let mut id = send_ping!().await?.id;
    debug_assert!(!with_rt!(|rt| rt.submit_response_from_known_node(&id, &addr)));

    if with_rt!(|rt| rt.add_responding_node(&id, &addr)) {
        log::debug!("Periodic ping of {addr} starting");
        let _sw = debug_stopwatch!("Periodic ping of {addr}");

        while let Some(last_active) =
            with_rt!(|rt| rt.get_node_by_id(&id).and_then(Node::last_active_time))
        {
            let time_since_last_activity = last_active.elapsed();

            if time_since_last_activity >= PING_INTERVAL {
                let response =
                    send_ping!().await.inspect_err(|_e| with_rt!(|rt| rt.remove_node(&id)))?;

                let submitted = with_rt!(|rt| if response.id != id {
                    log::warn!("Node id of {} changed: {:?} -> {:?}", addr, id, response.id);
                    rt.remove_node(&id);
                    id = response.id;
                    rt.add_responding_node(&id, &addr)
                } else {
                    rt.submit_response_from_known_node(&id, &addr)
                });

                if !submitted {
                    break;
                }
            } else {
                time::sleep(PING_INTERVAL - time_since_last_activity).await;
            }
        }
    }
    Ok(())
}
