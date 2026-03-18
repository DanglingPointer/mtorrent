use super::ctx;
use crate::ops::PeerReporter;
use crate::utils::config;
use futures_util::future;
use local_async_utils::prelude::*;
use mtorrent_core::input::Metainfo;
use mtorrent_core::pwp::PeerOrigin;
use mtorrent_core::trackers::*;
use std::path::Path;
use std::{io, iter};
use tokio::time::{self, Instant};

pub async fn make_periodic_announces(
    mut ctx_handle: ctx::Handle<ctx::MainCtx>,
    tracker_client: TrackerClient,
    peer_reporter: PeerReporter,
    config_dir: impl AsRef<Path>,
) {
    define_with!(ctx_handle);
    let tracker_urls =
        with!(|ctx| update_tracker_urls(trackers_from_metainfo(&ctx.metainfo), &config_dir));
    launch_announces(&tracker_client, &peer_reporter, ctx_handle, tracker_urls, &config_dir).await;
}

pub async fn make_preliminary_announces(
    mut ctx_handle: ctx::Handle<ctx::PreliminaryCtx>,
    trackers_handle: TrackerClient,
    peer_reporter: PeerReporter,
    config_dir: impl AsRef<Path>,
) {
    define_with!(ctx_handle);
    let tracker_urls = with!(|ctx| update_tracker_urls(ctx.magnet.trackers(), &config_dir));
    launch_announces(&trackers_handle, &peer_reporter, ctx_handle, tracker_urls, &config_dir).await;
}

// ------------------------------------------------------------------------------------------------

fn update_tracker_urls<'a>(
    supplied_trackers: impl IntoIterator<Item = &'a str>,
    config_dir: impl AsRef<Path>,
) -> Vec<TrackerUrl> {
    let mut all_trackers: Vec<TrackerUrl> = supplied_trackers
        .into_iter()
        .filter_map(|s| s.parse::<TrackerUrl>().ok())
        .collect();

    match config::load_trackers(&config_dir) {
        Ok(loaded_trackers) => {
            for tracker in loaded_trackers {
                if !all_trackers.contains(&tracker) {
                    all_trackers.push(tracker);
                }
            }
        }
        Err(e) => {
            log::warn!("Failed to load trackers from file: {e}");
        }
    }

    // note that when saving trackers below, the '/announce' suffix disappears for udp urls
    match config::save_trackers(&config_dir, all_trackers.clone()) {
        Ok(()) => (),
        Err(e) => log::warn!("Failed to save trackers to file: {e}"),
    }
    all_trackers
}

async fn launch_announces(
    tracker_client: &TrackerClient,
    peer_reporter: &PeerReporter,
    handler: impl AnnounceHandler + Clone,
    tracker_urls: impl IntoIterator<Item = TrackerUrl>,
    config_dir: impl AsRef<Path>,
) {
    future::join_all(tracker_urls.into_iter().map(|url| {
        announce_periodically(
            tracker_client,
            peer_reporter,
            url,
            handler.clone(),
            config_dir.as_ref(),
        )
    }))
    .await;
}

async fn announce_periodically(
    tracker_client: &TrackerClient,
    peer_reporter: &PeerReporter,
    url: TrackerUrl,
    mut handler: impl AnnounceHandler,
    config_dir: impl AsRef<Path>,
) {
    loop {
        let request = handler.generate_request();
        log::debug!("Announcing to {url:?}: {request:?}");
        match tracker_client.announce(url.clone(), request).await {
            Ok(mut response) => {
                log::info!("Announce response from {url:?}: {response:?}");
                handler.preprocess_response(&mut response);
                let reannounce_at = Instant::now() + response.interval.clamp(sec!(5), sec!(300));
                for peer_addr in response.peers {
                    peer_reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await;
                }
                time::sleep_until(reannounce_at).await;
            }
            Err(e) => {
                if e.kind() != io::ErrorKind::BrokenPipe {
                    // BrokenPipe means TrackerManager is shutting down
                    log::warn!("Announce to {url:?} failed: {e}. Removing tracker from config");
                    _ = config::remove_tracker(config_dir, &url)
                        .inspect_err(|e| log::error!("Failed to remove tracker from config: {e}"));
                }
                return;
            }
        }
    }
}

fn trackers_from_metainfo(metainfo: &Metainfo) -> Box<dyn Iterator<Item = &str> + '_> {
    if let Some(announce_list) = metainfo.announce_list() {
        Box::new(announce_list.flatten())
    } else if let Some(url) = metainfo.announce() {
        Box::new(iter::once(url))
    } else {
        Box::new(iter::empty())
    }
}

// ------------------------------------------------------------------------------------------------

trait AnnounceHandler {
    fn generate_request(&mut self) -> AnnounceRequest;
    fn preprocess_response(&mut self, response: &mut AnnounceResponse);
}

impl AnnounceHandler for ctx::Handle<ctx::MainCtx> {
    fn generate_request(&mut self) -> AnnounceRequest {
        self.with(|ctx| AnnounceRequest {
            info_hash: *ctx.metainfo.info_hash(),
            downloaded: ctx.accountant.accounted_bytes(),
            left: ctx.accountant.missing_bytes(),
            uploaded: ctx.peer_states.uploaded_bytes(),
            local_peer_id: *ctx.const_data.local_peer_id(),
            listener_port: ctx.const_data.pwp_listener_public_addr().port(),
            event: if ctx.accountant.missing_bytes() == 0 {
                Some(AnnounceEvent::Completed)
            } else if ctx.accountant.accounted_bytes() == 0 || ctx.peer_states.iter().count() == 0 {
                Some(AnnounceEvent::Started)
            } else {
                None
            },
            num_want: if ctx.accountant.missing_bytes() == 0 {
                0
            } else {
                100
            },
        })
    }

    fn preprocess_response(&mut self, response: &mut AnnounceResponse) {
        self.with(|ctx| response.peers.retain(|peer_ip| ctx.peer_states.get(peer_ip).is_none()));
    }
}

impl AnnounceHandler for ctx::Handle<ctx::PreliminaryCtx> {
    fn generate_request(&mut self) -> AnnounceRequest {
        self.with(|ctx| AnnounceRequest {
            info_hash: *ctx.magnet.info_hash(),
            downloaded: 0,
            left: 0,
            uploaded: 0,
            local_peer_id: *ctx.const_data.local_peer_id(),
            listener_port: ctx.const_data.pwp_listener_public_addr().port(),
            event: Some(AnnounceEvent::Started),
            num_want: 100,
        })
    }

    fn preprocess_response(&mut self, response: &mut AnnounceResponse) {
        self.with(|ctx| {
            // filter out already discovered peers
            response.peers.retain(|peer_ip| !ctx.discovered_peers.contains(peer_ip));

            // Save all the returned peers now because when the metadata download finishes the
            // addresses in the PeerReporter queue will be lost, and it might take time to re-fetch
            // them from trackers
            // (if the preliminary stage was very short, the trackers might rate-limit us when we
            // announce again immediately after the transition to main stage)
            ctx.discovered_peers.extend(&response.peers);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mtorrent_core::input::MagnetLink;
    use mtorrent_utils::peer_id::PeerId;
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::{fs, iter};

    #[test]
    fn test_extract_trackers_from_metainfo() {
        fn get_udp_tracker_addr(tracker: &str) -> Option<String> {
            match tracker.parse::<TrackerUrl>() {
                Ok(TrackerUrl::Udp(url)) => Some((*url).to_owned()),
                _ => None,
            }
        }

        fn get_http_tracker_addr(tracker: &str) -> Option<String> {
            match tracker.parse::<TrackerUrl>() {
                Ok(TrackerUrl::Http(url)) => Some((*url).to_owned()),
                _ => None,
            }
        }

        let info = Metainfo::from_file("../mtorrent-cli/tests/assets/example.torrent").unwrap();

        let mut http_iter = trackers_from_metainfo(&info).filter_map(get_http_tracker_addr);
        assert_eq!("http://tracker.trackerfix.com:80/announce", http_iter.next().unwrap());
        assert!(http_iter.next().is_none());
        let udp_trackers = trackers_from_metainfo(&info)
            .filter_map(get_udp_tracker_addr)
            .collect::<HashSet<_>>();
        assert_eq!(4, udp_trackers.len());
        assert!(udp_trackers.contains("9.rarbg.me:2720"));
        assert!(udp_trackers.contains("9.rarbg.to:2740"));
        assert!(udp_trackers.contains("tracker.fatkhoala.org:13780"));
        assert!(udp_trackers.contains("tracker.tallpenguin.org:15760"));

        let info =
            Metainfo::from_file("../mtorrent-cli/tests/assets/torrents_with_tracker/pcap.torrent")
                .unwrap();

        let mut http_iter = trackers_from_metainfo(&info).filter_map(get_http_tracker_addr);
        assert_eq!("http://localhost:8000/announce", http_iter.next().unwrap());
        assert!(http_iter.next().is_none());
    }

    #[test]
    fn test_combine_supplied_and_saved_trackers() {
        let config_dir = "test_combine_supplied_and_saved_trackers";
        fs::create_dir_all(config_dir).unwrap();

        {
            let supplied_trackers = [
                "udp://open.stealth.si:80/announce",
                "invalid",
                "https://example.com",
            ];

            let updated_trackers = update_tracker_urls(supplied_trackers, config_dir);

            assert_eq!(
                updated_trackers,
                vec![
                    "udp://open.stealth.si:80/announce".parse().unwrap(),
                    "https://example.com".parse().unwrap(),
                ]
            );
        }

        {
            let updated_trackers = update_tracker_urls(iter::empty(), config_dir);

            assert_eq!(
                updated_trackers,
                vec![
                    "https://example.com".parse().unwrap(),
                    "udp://open.stealth.si:80".parse().unwrap(),
                ]
            );
        }

        {
            let supplied_trackers = [
                "http://tracker1.com",
                "udp://tracker.tiny-vps.com:6969",
                "udp://open.stealth.si:80/announce",
            ];

            let updated_trackers = update_tracker_urls(supplied_trackers, config_dir);

            assert_eq!(
                updated_trackers,
                vec![
                    "http://tracker1.com".parse().unwrap(),
                    "udp://tracker.tiny-vps.com:6969".parse().unwrap(),
                    "udp://open.stealth.si:80".parse().unwrap(),
                    "https://example.com".parse().unwrap(),
                ]
            );
        }

        {
            let updated_trackers = update_tracker_urls(iter::empty(), config_dir);

            assert_eq!(
                updated_trackers,
                vec![
                    "http://tracker1.com".parse().unwrap(),
                    "https://example.com".parse().unwrap(),
                    "udp://open.stealth.si:80".parse().unwrap(),
                    "udp://tracker.tiny-vps.com:6969".parse().unwrap(),
                ]
            );
        }

        fs::remove_dir_all(config_dir).unwrap();
    }

    #[test]
    fn test_preliminary_preprocess_response_filters_and_saves_peers() {
        let magnet = "magnet:?xt=urn:btih:1EBD3DBFBB25C1333F51C99C7EE670FC2A1727C9"
            .parse::<MagnetLink>()
            .unwrap();
        let peer_id = PeerId::from(&[0u8; 20]);
        let listener_addr: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let mut handle = ctx::PreliminaryCtx::new(magnet, peer_id, listener_addr, 6881);

        let peer1: SocketAddr = "1.2.3.4:1000".parse().unwrap();
        let peer2: SocketAddr = "5.6.7.8:2000".parse().unwrap();
        let peer3: SocketAddr = "9.10.11.12:3000".parse().unwrap();

        // First response: all peers are new, none should be filtered out
        {
            let mut response = AnnounceResponse {
                interval: sec!(60),
                peers: vec![peer1, peer2],
            };
            handle.preprocess_response(&mut response);
            assert_eq!(response.peers, vec![peer1, peer2]);
            // Verify peers were saved to discovered_peers
            handle.with(|ctx| {
                assert!(ctx.discovered_peers.contains(&peer1));
                assert!(ctx.discovered_peers.contains(&peer2));
                assert_eq!(ctx.discovered_peers.len(), 2);
            });
        }

        // Second response: peer1 is already known, peer3 is new
        {
            let mut response = AnnounceResponse {
                interval: sec!(60),
                peers: vec![peer1, peer3],
            };
            handle.preprocess_response(&mut response);
            // peer1 should be filtered out
            assert_eq!(response.peers, vec![peer3]);
            // peer3 should now also be saved
            handle.with(|ctx| {
                assert!(ctx.discovered_peers.contains(&peer3));
                assert_eq!(ctx.discovered_peers.len(), 3);
            });
        }

        // Third response: all peers already known
        {
            let mut response = AnnounceResponse {
                interval: sec!(60),
                peers: vec![peer1, peer2, peer3],
            };
            handle.preprocess_response(&mut response);
            assert!(response.peers.is_empty());
        }

        // verify discovered_peers
        let discovered_peers = handle.with(|ctx| ctx.discovered_peers.clone());
        assert_eq!(discovered_peers, [peer1, peer2, peer3].into_iter().collect());
    }
}
