use super::cmds::{self, Command};
use super::kademlia;
use super::msgs::*;
use super::peers::TokenManager;
use super::queries::{self, IncomingQuery};
use super::u160::U160;
use crate::debug_stopwatch;
use crate::dht::peers::PeerTable;
use futures::StreamExt;
use local_async_utils::local_sync::LocalShared;
use local_async_utils::min;
use local_async_utils::shared::Shared;
use std::collections::{hash_map, HashMap};
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;
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

type RoutingTable = kademlia::RoutingTable<16>;
type ActivityTable = HashMap<SocketAddr, Instant>;

struct SharedCtx {
    nodes: RoutingTable,
    peers: PeerTable,
    last_rx_time: ActivityTable,
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
                last_rx_time: Default::default(),
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
        log::debug!(
            "Processor shutting down, node_count = {}",
            self.ctx.with(|ctx| ctx.nodes.node_count())
        );
    }

    fn handle_query(&mut self, query: IncomingQuery) {
        define!(with_ctx, self.ctx);
        let addr = *query.source_addr();

        let existing_node = with_ctx!(|ctx| {
            let last_rx_time_entry = ctx.last_rx_time.entry(addr).and_modify(|last_active| {
                *last_active = Instant::now();
            });
            respond_to_incoming_query(&ctx.nodes, &mut ctx.peers, &mut self.token_mgr, query);
            matches!(last_rx_time_entry, hash_map::Entry::Occupied(_))
        });

        if !existing_node {
            spawn_periodic_ping(
                self.ctx.project(|ctx| &mut ctx.nodes),
                self.ctx.project(|ctx| &mut ctx.last_rx_time),
                self.client.clone(),
                addr,
                self.shutdown_notify.clone(),
            );
        }
    }

    fn handle_command(&mut self, cmd: Command) {
        define!(with_ctx, self.ctx);

        match cmd {
            Command::AddNode { addr } => {
                if !with_ctx!(|ctx| ctx.last_rx_time.contains_key(&addr)) {
                    spawn_periodic_ping(
                        self.ctx.project(|ctx| &mut ctx.nodes),
                        self.ctx.project(|ctx| &mut ctx.last_rx_time),
                        self.client.clone(),
                        addr,
                        self.shutdown_notify.clone(),
                    );
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

fn spawn_periodic_ping(
    rt: impl Shared<Target = RoutingTable> + 'static,
    at: impl Shared<Target = ActivityTable> + 'static,
    client: queries::Client,
    node_addr: SocketAddr,
    shutdown_signal: Rc<Notify>,
) {
    let ping_task = periodic_ping(rt, at, client.clone(), node_addr, shutdown_signal.clone());
    task::spawn_local(async move {
        select! {
            biased;
            _ = ping_task => (),
            _ = shutdown_signal.notified() => (),
        }
    });
}

async fn periodic_ping(
    mut rt: impl Shared<Target = RoutingTable> + 'static,
    mut at: impl Shared<Target = ActivityTable> + 'static,
    client: queries::Client,
    addr: SocketAddr,
    shutdown_signal: Rc<Notify>,
) -> io::Result<()> {
    const PING_INTERVAL: Duration = min!(5);
    define!(with_rt, rt);
    define!(with_at, at);

    if with_at!(|last_rx| last_rx.contains_key(&addr)) {
        // already pinginig this node
        return Ok(());
    }

    let local_id = with_rt!(|rt| *rt.local_id());

    // query nodes close to local ID
    let mut response = client
        .find_node(
            addr,
            FindNodeArgs {
                id: local_id,
                target: local_id,
            },
        )
        .await?;

    let mut id = response.id;

    // process response
    response.nodes.retain(|(node_id, addr)| {
        *node_id != local_id
            && *node_id != id
            && with_at!(|last_rx| !last_rx.contains_key(&SocketAddr::V4(*addr)))
            && with_rt!(|rt| rt.can_insert(node_id))
    });
    for addr in response.nodes.into_iter().map(|(_, addr)| SocketAddr::V4(addr)) {
        spawn_periodic_ping(rt.clone(), at.clone(), client.clone(), addr, shutdown_signal.clone());
    }

    // check if we should ignore this node
    if !with_rt!(|routing| routing.insert_node(&id, &addr)) {
        // no capacity in the routing table
        return Ok(());
    }

    with_at!(|last_rx| last_rx.insert(addr, Instant::now()));

    log::info!(
        "Periodic ping of {addr} starting; node_count = {}",
        with_rt!(|routing| routing.node_count())
    );

    let _sw = debug_stopwatch!("Periodic ping of {addr}");
    while let Some(last_active) = with_at!(|last_rx| last_rx.get(&addr).cloned()) {
        let time_since_last_activity = last_active.elapsed();
        if time_since_last_activity >= PING_INTERVAL {
            // send periodic ping
            let response =
                client.ping(addr, PingArgs { id: local_id }).await.inspect_err(|_e| {
                    with_rt!(|routing| routing.remove_node(&id));
                    with_at!(|last_rx| last_rx.remove(&addr));
                })?;
            // handle change of id
            if response.id != id {
                let inserted_new_id = with_rt!(|routing| {
                    routing.remove_node(&id);
                    routing.insert_node(&response.id, &addr)
                });
                if !inserted_new_id {
                    with_at!(|last_rx| last_rx.remove(&addr));
                    break;
                }
                id = response.id;
            }
            // update last rx time
            with_at!(|last_rx| last_rx.insert(addr, Instant::now()));
        } else {
            // sleep until next ping time
            time::sleep(PING_INTERVAL - time_since_last_activity).await;
        }
    }
    Ok(())
}
