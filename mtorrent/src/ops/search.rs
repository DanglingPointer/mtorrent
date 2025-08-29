use super::connections::PeerReporter;
use local_async_utils::prelude::*;
use mtorrent_core::pwp::PeerOrigin;
use mtorrent_dht as dht;
use mtorrent_utils::debug_stopwatch;
use std::time::Duration;
use tokio::{select, sync::mpsc, time};

/// Start dht search of `info_hash` and relay results into `peer_sink`.
/// Restart search every minute, exit if the search terminates by itself.
/// When `peer_sink` no longer has a receiver, this will eventually stop,
/// but not immmediately after the channel has been closed.
pub async fn run_dht_search(
    info_hash: [u8; 20],
    dht_cmds: dht::CmdSender,
    peer_reporter: PeerReporter,
    pwp_listener_port: u16,
) {
    const SEARCH_RESTART_INTERVAL: Duration = sec!(120);

    let _sw = debug_stopwatch!("DHT search operation");

    async fn do_search(
        info_hash: &[u8; 20],
        dht_cmds: &dht::CmdSender,
        peer_reporter: &PeerReporter,
        pwp_listener_port: u16,
    ) {
        let (sender, mut result_receiver) = mpsc::channel(1024);
        if let Err(e) = dht_cmds
            .send(dht::Command::FindPeers {
                info_hash: dht::U160::from(*info_hash),
                callback: sender,
                local_peer_port: pwp_listener_port,
            })
            .await
        {
            log::warn!("Failed to start DHT search: {e}");
            return;
        }
        while let Some(peer_addr) = result_receiver.recv().await {
            log::debug!("DHT discovered peer: {peer_addr}");
            if !peer_reporter.report_discovered_new(peer_addr, PeerOrigin::Dht).await {
                return;
            }
        }
    }

    loop {
        select! {
            biased;
            _ = do_search(&info_hash, &dht_cmds, &peer_reporter, pwp_listener_port) => break,
            _ = time::sleep(SEARCH_RESTART_INTERVAL) => (),
        }
    }
}
