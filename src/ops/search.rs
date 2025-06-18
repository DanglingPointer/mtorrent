use crate::{dht, pwp::PeerOrigin};
use local_async_utils::prelude::*;
use std::net::SocketAddr;
use tokio::sync::mpsc;

/// Start dht search of `info_hash` and relay results into `peer_sink`.
/// Repeat search once/if it finishes.
/// When `peer_sink` no longer has a receiver, this will eventually stop,
/// but not immmediately after the channel has been closed.
pub async fn run_dht_search(
    info_hash: [u8; 20],
    dht_cmds: dht::CmdSender,
    peer_sink: local_channel::Sender<(SocketAddr, PeerOrigin)>,
    pwp_listener_port: u16,
) {
    loop {
        let (sender, mut result_receiver) = mpsc::channel(1024);
        if let Err(e) = dht_cmds
            .send(dht::Command::FindPeers {
                info_hash: info_hash.into(),
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
            if peer_sink.is_closed() {
                return;
            }
            peer_sink.send((peer_addr, PeerOrigin::Dht));
        }
        log::warn!("Restarting DHT search");
    }
}
