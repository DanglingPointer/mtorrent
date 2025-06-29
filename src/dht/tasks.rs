use super::U160;
use super::msgs::*;
use super::processor::BoxRoutingTable;
use super::queries::Client;
use crate::trace_stopwatch;
use crate::utils::connctrl::{ConnectPermit, QuickConnectControl};
use futures::io;
use local_async_utils::prelude::*;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::{Notify, mpsc};
use tokio::time::Instant;
use tokio::{select, task, time};

const PING_INTERVAL: Duration = min!(5);
const GET_PEERS_INTERVAL: Duration = sec!(30);

macro_rules! is_valid_addr {
    ($addr:expr) => {
        !$addr.ip().is_unspecified() && $addr.port() >= 1024
    };
}

pub(super) struct PingCtx {
    pub(super) nodes: LocalShared<BoxRoutingTable>,
    pub(super) client: Client,
    pub(super) shutdown_signal: Rc<Notify>,
}

pub(super) fn launch_periodic_ping(
    addr: SocketAddr,
    permit: ConnectPermit<PingCtx>,
    cnt_ctrl: QuickConnectControl<PingCtx>,
) {
    let shutdown_signal = permit.shutdown_signal.clone();
    task::spawn_local(async move {
        select! {
            biased;
            _ = periodic_ping(addr, permit, cnt_ctrl) => (),
            _ = shutdown_signal.notified() => (),
        }
    });
}

async fn periodic_ping(
    addr: SocketAddr,
    permit: ConnectPermit<PingCtx>,
    cnt_ctrl: QuickConnectControl<PingCtx>,
) -> io::Result<()> {
    let mut rt = permit.nodes.clone();
    define!(with_rt, rt);

    let local_id = with_rt!(|rt| *rt.local_id());

    // query nodes close to local ID
    let response = permit
        .client
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
    for (node_id, node_addr) in response.nodes {
        if is_valid_addr!(node_addr)
            && node_id != local_id
            && node_id != id
            && with_rt!(|rt| rt.can_insert(&node_id))
        {
            let node_addr = SocketAddr::V4(node_addr);
            if let Some(permit) = cnt_ctrl.try_acquire_permit(node_addr) {
                launch_periodic_ping(node_addr, permit, cnt_ctrl.clone());
            }
        }
    }

    // check if we should ignore this node
    if !with_rt!(|routing| routing.insert_node(&id, &addr)) {
        // no capacity in the routing table
        return Ok(());
    }

    log::debug!(
        "Periodic ping of {addr} starting; node_count = {}",
        with_rt!(|routing| routing.node_count())
    );

    let _sw = trace_stopwatch!("Periodic ping of {addr}");
    while with_rt!(|rt| rt.get_node(&id).is_some()) {
        // send periodic ping
        let response =
            permit.client.ping(addr, PingArgs { id: local_id }).await.inspect_err(|_e| {
                with_rt!(|routing| routing.remove_node(&id));
            })?;
        // handle change of id
        if response.id != id {
            let inserted_new_id = with_rt!(|routing| {
                routing.remove_node(&id);
                routing.insert_node(&response.id, &addr)
            });
            if !inserted_new_id {
                break;
            }
            id = response.id;
        }
        // sleep until next ping time
        time::sleep(PING_INTERVAL + sec!(rand::random::<u64>() % 10)).await;
    }
    Ok(())
}

pub(super) struct SearchCtx {
    pub(super) target: U160,
    pub(super) local_peer_port: u16,
    pub(super) cmd_result_sender: mpsc::Sender<SocketAddr>,
    pub(super) peer_sender: local_channel::Sender<(SocketAddr, U160)>,
    pub(super) cnt_ctrl: QuickConnectControl<PingCtx>,
    pub(super) queried_nodes: sealed::Set<SocketAddr>,
    pub(super) discovered_peers: sealed::Set<SocketAddr>,
}

pub(super) fn launch_peer_search(ctx: Rc<SearchCtx>, addr: SocketAddr) {
    if ctx.queried_nodes.insert(addr) && !ctx.cmd_result_sender.is_closed() {
        let shutdown_signal = ctx.cnt_ctrl.data().shutdown_signal.clone();
        task::spawn_local(async move {
            select! {
                biased;
                _ = peer_search(ctx, addr) => (),
                _ = shutdown_signal.notified() => (),
            }
        });
    }
}

async fn peer_search(ctx: Rc<SearchCtx>, addr: SocketAddr) -> io::Result<()> {
    let mut rt = ctx.cnt_ctrl.data().nodes.clone();
    define!(with_rt, rt);

    let local_id = with_rt!(|rt| *rt.local_id());
    let client = &ctx.cnt_ctrl.data().client;

    macro_rules! announce_peer {
        ($token:expr) => {
            if let Some(token) = $token {
                let _ = client
                    .announce_peer(
                        addr,
                        AnnouncePeerArgs {
                            id: local_id,
                            info_hash: ctx.target,
                            port: Some(ctx.local_peer_port),
                            token,
                        },
                    )
                    .await?;
            }
        };
    }

    while !ctx.cmd_result_sender.is_closed() {
        let response = client
            .get_peers(
                addr,
                GetPeersArgs {
                    id: local_id,
                    info_hash: ctx.target,
                },
            )
            .await?;

        match response.data {
            GetPeersResponseData::Nodes(id_addr_pairs) => {
                log::debug!("search produced {} nodes", id_addr_pairs.len());
                // spawn search tasks for the returned nodes
                for addr in id_addr_pairs.into_iter().map(|(_id, ipv4)| SocketAddr::V4(ipv4)) {
                    if is_valid_addr!(addr) {
                        launch_peer_search(ctx.clone(), addr);
                    }
                }
                announce_peer!(response.token);
                break;
            }
            GetPeersResponseData::Peers(socket_addr_v4s) => {
                log::debug!("search produced {} peer(s)", socket_addr_v4s.len());
                let repeat_at = Instant::now() + GET_PEERS_INTERVAL;
                // report the received peer addresses
                for peer_addr in socket_addr_v4s.into_iter().map(SocketAddr::V4) {
                    if is_valid_addr!(peer_addr) && ctx.discovered_peers.insert(peer_addr) {
                        ctx.peer_sender.send((peer_addr, ctx.target));
                        let _ = ctx.cmd_result_sender.send(peer_addr).await;
                    }
                }
                announce_peer!(response.token);
                time::sleep_until(repeat_at).await;
            }
        }
    }
    // try insert in the routing table and start periodic ping unless already in progress
    if let Some(permit) = ctx.cnt_ctrl.try_acquire_permit(addr) {
        periodic_ping(addr, permit, ctx.cnt_ctrl.clone()).await?;
    }
    Ok(())
}
