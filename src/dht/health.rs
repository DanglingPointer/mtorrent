use super::msgs::{FindNodeArgs, PingArgs};
use super::processor::RoutingTable;
use super::queries::Client;
use crate::debug_stopwatch;
use crate::utils::connctrl::{ConnectPermit, QuickConnectControl};
use futures::io;
use local_async_utils::local_sync::LocalShared;
use local_async_utils::shared::Shared;
use local_async_utils::{min, sec};
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::{select, task, time};

pub(super) struct Ctx {
    pub(super) nodes: LocalShared<RoutingTable>,
    pub(super) client: Client,
    pub(super) shutdown_signal: Rc<Notify>,
}

pub(super) fn spawn_periodic_ping(
    addr: SocketAddr,
    permit: ConnectPermit<Ctx>,
    cnt_ctrl: QuickConnectControl<Ctx>,
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
    permit: ConnectPermit<Ctx>,
    cnt_ctrl: QuickConnectControl<Ctx>,
) -> io::Result<()> {
    const PING_INTERVAL: Duration = min!(5);
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
        if node_id != local_id && node_id != id && with_rt!(|rt| rt.can_insert(&node_id)) {
            let node_addr = SocketAddr::V4(node_addr);
            if let Some(permit) = cnt_ctrl.try_acquire_permit(node_addr) {
                spawn_periodic_ping(node_addr, permit, cnt_ctrl.clone());
            }
        }
    }

    // check if we should ignore this node
    if !with_rt!(|routing| routing.insert_node(&id, &addr)) {
        // no capacity in the routing table
        return Ok(());
    }

    log::info!(
        "Periodic ping of {addr} starting; node_count = {}",
        with_rt!(|routing| routing.node_count())
    );

    let _sw = debug_stopwatch!("Periodic ping of {addr}");
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
