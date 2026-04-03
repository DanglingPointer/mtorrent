use super::connections::PeerReporter;
use local_async_utils::prelude::*;
use mtorrent_core::pwp::PeerOrigin;
use mtorrent_dht as dht;
use mtorrent_utils::info_stopwatch;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};

const MIN_INTERVAL_BETWEEN_RESTARTS: Duration = min!(1);

/// Start dht search of `info_hash` and pass results into a [`PeerReporter`].
/// Restart search if it finishes, until the `PeerReporter` is closed or the DHT subsystem has been
/// shut down. When the peer reporter has been closed, this will eventually stop, but not
/// immmediately.
pub async fn run_dht_search(
    info_hash: [u8; 20],
    dht_cmds: dht::CommandSink,
    peer_reporter: PeerReporter,
    pwp_listener_port: u16,
) {
    loop {
        let earliest_restart_time = Instant::now() + MIN_INTERVAL_BETWEEN_RESTARTS;

        let (sender, mut result_receiver) = mpsc::channel(1024);
        if let Err(e) = dht_cmds
            .send(dht::Command::FindPeers {
                info_hash,
                callback: sender,
                local_peer_port: pwp_listener_port,
            })
            .await
        {
            // DHT has shut down
            log::error!("Failed to start DHT search: {e}");
            return;
        }

        let sw = info_stopwatch!("DHT search operation");
        while let Some(peer_addr) = result_receiver.recv().await {
            if !peer_reporter.report_discovered(peer_addr, PeerOrigin::Dht).await {
                // peer reporter has been closed
                return;
            }
        }
        drop(sw);

        if Instant::now() < earliest_restart_time {
            // search finished too quickly, likely due to connectiviy issues. Wait a bit before
            // restarting to avoid busy loop
            time::sleep_until(earliest_restart_time).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use tokio::{task, time};

    #[tokio::test(start_paused = true)]
    async fn test_restart_dht_search_when_it_finishes() {
        let (cmd_sender, mut cmd_receiver) = mpsc::channel(1);

        task::spawn(run_dht_search([0; 20], cmd_sender, PeerReporter::new_mock(), 6881));
        task::yield_now().await;

        let find_peers_cmd = cmd_receiver.try_recv().expect("search should have started");
        let dht::Command::FindPeers { callback, .. } = find_peers_cmd else {
            panic!("expected FindPeers command");
        };
        assert!(matches!(cmd_receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty)));

        // finish the search by dropping the callback
        drop(callback);
        task::yield_now().await;
        assert!(matches!(cmd_receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty)));

        // search should restart after the min interval
        time::sleep(MIN_INTERVAL_BETWEEN_RESTARTS).await;
        task::yield_now().await;
        let find_peers_cmd = cmd_receiver.try_recv().expect("search should have restarted");
        let dht::Command::FindPeers { .. } = find_peers_cmd else {
            panic!("expected FindPeers command");
        };
        assert!(matches!(cmd_receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }

    #[tokio::test(start_paused = true)]
    async fn test_dont_restart_dht_search_periodically() {
        let (cmd_sender, mut cmd_receiver) = mpsc::channel(1);

        task::spawn(run_dht_search([0; 20], cmd_sender, PeerReporter::new_mock(), 6881));
        task::yield_now().await;

        let find_peers_cmd = cmd_receiver.try_recv().expect("search should have started");
        assert!(matches!(find_peers_cmd, dht::Command::FindPeers { .. }));
        assert!(matches!(cmd_receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty)));

        time::sleep(Duration::MAX).await;
        assert!(matches!(cmd_receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn test_dht_search_fails_when_dht_is_shut_down() {
        let (cmd_sender, _) = mpsc::channel(1);

        let handle =
            task::spawn(run_dht_search([0; 20], cmd_sender, PeerReporter::new_mock(), 6881));

        task::yield_now().await;
        assert!(
            handle.is_finished(),
            "search should have exited when DHT commands channel was closed"
        );
    }
}
