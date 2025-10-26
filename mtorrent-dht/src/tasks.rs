use super::error::Error;
use super::msgs::*;
use super::queries::OutboundQueries;
use super::u160::U160;
use crate::kademlia::Node;
use local_async_utils::prelude::*;
use mtorrent_utils::info_stopwatch;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio::{select, task, time};
use tokio_util::sync::CancellationToken;

const PING_INTERVAL: Duration = min!(1);
const GET_PEERS_INTERVAL: Duration = sec!(30);

macro_rules! is_valid_addr {
    ($addr:expr) => {
        !$addr.ip().is_unspecified() && $addr.port() >= 1024
    };
}

fn validate_discovered_node(discovered: &Node, source: &Node, ctx: &Ctx) -> bool {
    is_valid_addr!(discovered.addr)
        && discovered.addr != source.addr
        && discovered.id != source.id
        && discovered.id != ctx.local_id
}

// --------------------

pub type Result<T> = std::result::Result<T, Error>;

pub enum NodeEvent {
    Discovered(Node),
    Connected(Node),
    Disconnected(Node),
    Unreachable(SocketAddr),
}

pub struct Ctx {
    pub client: OutboundQueries,
    pub event_reporter: mpsc::Sender<NodeEvent>,
    pub local_id: U160,
}

pub async fn probe_node(addr: SocketAddr, ctx: Rc<Ctx>) -> Result<()> {
    let response = match ctx
        .client
        .find_node(
            addr,
            FindNodeArgs {
                id: ctx.local_id,
                target: ctx.local_id,
            },
        )
        .await
    {
        Ok(response) => response,
        Err(e) => {
            ctx.event_reporter.send(NodeEvent::Unreachable(addr)).await?;
            return Err(e);
        }
    };

    let source = Node {
        id: response.id,
        addr,
    };

    for (node_id, node_addr) in response.nodes {
        let discovered = Node {
            id: node_id,
            addr: SocketAddr::V4(node_addr),
        };
        if validate_discovered_node(&discovered, &source, &ctx) {
            ctx.event_reporter.send(NodeEvent::Discovered(discovered)).await?;
        }
    }

    ctx.event_reporter
        .send(NodeEvent::Connected(Node {
            id: response.id,
            addr,
        }))
        .await?;
    Ok(())
}

pub async fn keep_alive_node(node: Node, ctx: Rc<Ctx>) -> Result<()> {
    loop {
        let response = match ctx.client.ping(node.addr, PingArgs { id: ctx.local_id }).await {
            Ok(response) => response,
            Err(e) => {
                ctx.event_reporter.send(NodeEvent::Disconnected(node)).await?;
                return Err(e);
            }
        };

        if response.id != node.id {
            let addr = node.addr;
            ctx.event_reporter.send(NodeEvent::Disconnected(node)).await?;
            ctx.event_reporter
                .send(NodeEvent::Discovered(Node {
                    id: response.id,
                    addr,
                }))
                .await?;
            return Ok(());
        }

        // sleep until next ping time (with some jitter to avoid bursts)
        time::sleep(PING_INTERVAL + sec!(rand::random::<u64>() % 10)).await;
    }
}

async fn query_node_for_peers(
    node: Node,
    target: U160,
    local_peer_port: u16,
    ctx: Rc<Ctx>,
    node_reporter: mpsc::Sender<Node>,
    peer_reporter: mpsc::Sender<SocketAddr>,
) -> Result<()> {
    macro_rules! announce_peer {
        ($token:expr) => {
            ctx.client.announce_peer(
                node.addr,
                AnnouncePeerArgs {
                    id: ctx.local_id,
                    info_hash: target,
                    port: Some(local_peer_port),
                    token: $token,
                },
            )
        };
    }

    while !node_reporter.is_closed() && !peer_reporter.is_closed() {
        let get_peers_response = ctx
            .client
            .get_peers(
                node.addr,
                GetPeersArgs {
                    id: ctx.local_id,
                    info_hash: target,
                },
            )
            .await?;

        match get_peers_response.data {
            GetPeersResponseData::Nodes(id_addr_pairs) => {
                log::trace!("search produced {} nodes", id_addr_pairs.len());

                for (id, addr) in id_addr_pairs {
                    let discovered = Node {
                        id,
                        addr: SocketAddr::V4(addr),
                    };
                    if validate_discovered_node(&discovered, &node, &ctx) {
                        node_reporter
                            .send(Node {
                                id,
                                addr: SocketAddr::V4(addr),
                            })
                            .await?;
                    }
                }
                if let Some(token) = get_peers_response.token {
                    announce_peer!(token).await?;
                }
                break;
            }
            GetPeersResponseData::Peers(socket_addr_v4s) => {
                log::trace!("search produced {} peer(s)", socket_addr_v4s.len());
                let repeat_at = Instant::now() + GET_PEERS_INTERVAL;

                for peer_addr in socket_addr_v4s {
                    if is_valid_addr!(peer_addr) {
                        peer_reporter.send(SocketAddr::V4(peer_addr)).await?;
                    }
                }
                if let Some(token) = get_peers_response.token {
                    announce_peer!(token).await?;
                }

                let find_nodes_response = ctx
                    .client
                    .find_node(
                        node.addr,
                        FindNodeArgs {
                            id: ctx.local_id,
                            target,
                        },
                    )
                    .await?;
                for (node_id, node_addr) in find_nodes_response.nodes {
                    let discovered = Node {
                        id: node_id,
                        addr: SocketAddr::V4(node_addr),
                    };
                    if validate_discovered_node(&discovered, &node, &ctx) {
                        node_reporter.send(discovered).await?;
                    }
                }

                time::sleep_until(repeat_at).await;
            }
        }
    }
    ctx.event_reporter.send(NodeEvent::Discovered(node)).await?;
    Ok(())
}

// --------------------

pub struct SearchTaskData {
    pub target: U160,
    pub local_peer_port: u16,
    pub ctx: Rc<Ctx>,
    pub cmd_result_sender: mpsc::Sender<SocketAddr>,
    pub peer_sender: local_unbounded::Sender<(SocketAddr, U160)>,
}

pub async fn run_search(
    data: SearchTaskData,
    canceller: CancellationToken, // should be child canceller
    initial_nodes: impl ExactSizeIterator<Item = Node>,
) -> Result<()> {
    assert_ne!(initial_nodes.len(), 0, "can't start DHT search without initial nodes");
    let _sw = info_stopwatch!("Search for peers with target {}", data.target);

    let (peer_sender, mut peer_receiver) = mpsc::channel(1);
    let (node_sender, mut node_receiver) = mpsc::channel(1);

    let mut discovered_peers = HashSet::new();
    let mut queried_nodes: HashSet<Node> = initial_nodes.collect();

    for node in &queried_nodes {
        task::spawn_local(canceller.clone().run_until_cancelled_owned(query_node_for_peers(
            node.clone(),
            data.target,
            data.local_peer_port,
            data.ctx.clone(),
            node_sender.clone(),
            peer_sender.clone(),
        )));
    }

    loop {
        select! {
            biased;
            _ = data.cmd_result_sender.closed() => {
                // this search has been terminated. Stop all subtasks and try insert nodes in the routing table
                canceller.cancel();
                for node in queried_nodes {
                    data.ctx.event_reporter.send(NodeEvent::Discovered(node)).await?;
                }
                break;
            }
            _ = canceller.cancelled() => {
                // DHT shutting down
                break;
            }
            Some(peer_addr) = peer_receiver.recv() => {
                if discovered_peers.insert(peer_addr) {
                    log::debug!("Discovered new peer {peer_addr} for target {}", data.target);
                    data.peer_sender.send((peer_addr, data.target))?; // fails only if DHT is shutting down
                    _ = data.cmd_result_sender.send(peer_addr).await;
                }
            }
            Some(node) = node_receiver.recv() => {
                if queried_nodes.insert(node.clone()) {
                    task::spawn_local(canceller.clone().run_until_cancelled_owned(
                        query_node_for_peers(
                            node,
                            data.target,
                            data.local_peer_port,
                            data.ctx.clone(),
                            node_sender.clone(),
                            peer_sender.clone(),
                        ),
                    ));
                }
            }
        }
    }

    Ok(())
}
