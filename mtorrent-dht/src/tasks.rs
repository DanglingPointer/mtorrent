use super::U160;
use super::error::Error;
use super::msgs::*;
use super::queries::Client;
use crate::kademlia::Node;
use local_async_utils::prelude::*;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio::{select, task, time};
use tokio_util::sync::CancellationToken;

const PING_INTERVAL: Duration = min!(5);
const GET_PEERS_INTERVAL: Duration = sec!(30);

macro_rules! is_valid_addr {
    ($addr:expr) => {
        !$addr.ip().is_unspecified() && $addr.port() >= 1024
    };
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
    pub client: Client,
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

    for (node_id, node_addr) in response.nodes {
        if is_valid_addr!(node_addr) && node_id != ctx.local_id && node_id != response.id {
            ctx.event_reporter
                .send(NodeEvent::Discovered(Node {
                    id: node_id,
                    addr: SocketAddr::V4(node_addr),
                }))
                .await?;
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

        // sleep until next ping time
        time::sleep(PING_INTERVAL + sec!(rand::random::<u64>() % 10)).await;
    }
}

// --------------------

pub struct SearchTask {
    target: U160,
    local_peer_port: u16,
    ctx: Rc<Ctx>,

    cmd_result_sender: mpsc::Sender<SocketAddr>,
    peer_sender: local_channel::Sender<(SocketAddr, U160)>,
}

impl SearchTask {
    pub fn new(
        target: U160,
        local_peer_port: u16,
        ctx: Rc<Ctx>,
        cmd_result_sender: mpsc::Sender<SocketAddr>,
        peer_sender: local_channel::Sender<(SocketAddr, U160)>,
    ) -> Self {
        Self {
            target,
            local_peer_port,
            ctx,
            cmd_result_sender,
            peer_sender,
        }
    }

    pub async fn run(
        self,
        initial_nodes: impl ExactSizeIterator<Item = Node>,
        canceller: CancellationToken,
    ) -> Result<()> {
        if initial_nodes.len() == 0 {
            return Err(Error::ChannelClosed);
        }

        let (peer_sender, mut peer_receiver) = mpsc::channel(1024);
        let (node_sender, mut node_receiver) = mpsc::channel(1024);

        for node in initial_nodes {
            _ = node_sender.try_send(node);
        }

        let mut queried_nodes = HashSet::new();
        let mut discovered_peers = HashSet::new();

        loop {
            select! {
                biased;
                _ = self.cmd_result_sender.closed() => {
                    break;
                }
                _ = canceller.cancelled() => {
                    break;
                }
                Some(peer) = peer_receiver.recv() => {
                    if discovered_peers.insert(peer) {
                        self.peer_sender.send((peer, self.target));
                        self.cmd_result_sender.send(peer).await?;
                    }
                }
                Some(node) = node_receiver.recv() => {
                    if queried_nodes.insert(node.clone()) {
                        task::spawn_local(canceller.clone().run_until_cancelled_owned(
                            search_peers(
                                node,
                                self.target,
                                self.local_peer_port,
                                self.ctx.clone(),
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
}

pub async fn search_peers(
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
        let response = ctx
            .client
            .get_peers(
                node.addr,
                GetPeersArgs {
                    id: ctx.local_id,
                    info_hash: target,
                },
            )
            .await?;

        match response.data {
            GetPeersResponseData::Nodes(id_addr_pairs) => {
                log::trace!("search produced {} nodes", id_addr_pairs.len());

                for (id, addr) in id_addr_pairs {
                    if is_valid_addr!(addr) {
                        node_reporter
                            .send(Node {
                                id,
                                addr: SocketAddr::V4(addr),
                            })
                            .await?;
                    }
                }
                if let Some(token) = response.token {
                    announce_peer!(token).await?;
                }
                break;
            }
            GetPeersResponseData::Peers(socket_addr_v4s) => {
                log::debug!("search produced {} peer(s)", socket_addr_v4s.len());
                let repeat_at = Instant::now() + GET_PEERS_INTERVAL;

                for peer_addr in socket_addr_v4s.into_iter().map(SocketAddr::V4) {
                    if is_valid_addr!(peer_addr) {
                        peer_reporter.send(peer_addr).await?;
                    }
                }
                if let Some(token) = response.token {
                    announce_peer!(token).await?;
                }
                time::sleep_until(repeat_at).await;
            }
        }
    }
    ctx.event_reporter.send(NodeEvent::Discovered(node)).await?;
    Ok(())
}
