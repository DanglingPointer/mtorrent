use super::ctx;
use crate::app::main::Mode;
use crate::core::PeerReporter;
use crate::utils::disk;
use futures_util::future;
use local_async_utils::prelude::*;
use mtorrent_base::input::Metainfo;
use mtorrent_base::pwp::PeerOrigin;
use mtorrent_base::trackers::*;
use std::path::Path;
use std::{io, iter};
use tokio::time::{self, Instant};

pub async fn make_periodic_announces(
    ctx_handle: ctx::Handle<ctx::MainCtx>,
    tracker_client: Client,
    peer_reporter: PeerReporter,
    config_dir: impl AsRef<Path>,
) {
    define_with!(ctx_handle);
    let tracker_urls =
        with!(|ctx| update_tracker_urls(trackers_from_metainfo(&ctx.metainfo), &config_dir));
    launch_announces(&tracker_client, &peer_reporter, ctx_handle, tracker_urls, &config_dir).await;
}

pub async fn make_preliminary_announces(
    ctx_handle: ctx::Handle<ctx::PreliminaryCtx>,
    trackers_handle: Client,
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

    match disk::load_trackers(&config_dir) {
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
    match disk::save_trackers(&config_dir, all_trackers.clone()) {
        Ok(()) => (),
        Err(e) => log::warn!("Failed to save trackers to file: {e}"),
    }
    all_trackers
}

async fn launch_announces(
    tracker_client: &Client,
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
    tracker_client: &Client,
    peer_reporter: &PeerReporter,
    url: TrackerUrl,
    mut handler: impl AnnounceHandler,
    config_dir: impl AsRef<Path>,
) {
    let mut sequence_num = 0;
    loop {
        let request = handler.generate_request(sequence_num);
        log::debug!("Announcing to {url:?}: {request:?}");
        match tracker_client.announce(url.clone(), request).await {
            Ok(mut response) => {
                log::info!("Announce response from {url:?}: {response:?}");
                handler.preprocess_response(&mut response);
                let reannounce_at = Instant::now() + response.interval.clamp(sec!(5), sec!(300));
                for peer_addr in response.peers {
                    peer_reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await;
                }
                sequence_num += 1;
                time::sleep_until(reannounce_at).await;
            }
            Err(e) => {
                if e.kind() != io::ErrorKind::BrokenPipe {
                    // BrokenPipe means TrackerManager is shutting down
                    log::warn!("Announce to {url:?} failed: {e}. Removing tracker from config");
                    _ = disk::remove_tracker(config_dir, &url)
                        .inspect_err(|e| log::warn!("Failed to remove tracker from config: {e}"));
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
    fn generate_request(&mut self, sequence_num: usize) -> AnnounceRequest;
    fn preprocess_response(&mut self, response: &mut AnnounceResponse);
}

impl AnnounceHandler for ctx::Handle<ctx::MainCtx> {
    fn generate_request(&mut self, sequence_num: usize) -> AnnounceRequest {
        self.with(|ctx| {
            let missing_pieces = ctx.piece_tracker.missing_pieces_bitfield();
            let missing_bytes = missing_pieces
                .iter_ones()
                .fold(0, |total, piece_index| total + ctx.pieces.piece_len(piece_index));
            AnnounceRequest {
                info_hash: *ctx.metainfo.info_hash(),
                downloaded: ctx.accountant.accounted_bytes(),
                left: missing_bytes,
                uploaded: ctx.peer_states.uploaded_bytes(),
                local_peer_id: *ctx.const_data.local_peer_id(),
                listener_port: ctx.const_data.pwp_external_port(),
                event: if sequence_num == 0 {
                    Some(AnnounceEvent::Started)
                } else if missing_pieces.not_any() {
                    Some(AnnounceEvent::Completed)
                } else {
                    None
                },
                num_want: if missing_pieces.not_any()
                    && matches!(ctx.const_data.mode(), Mode::Leech)
                {
                    0
                } else {
                    100
                },
            }
        })
    }

    fn preprocess_response(&mut self, response: &mut AnnounceResponse) {
        self.with(|ctx| response.peers.retain(|peer_ip| ctx.peer_states.get(peer_ip).is_none()));
    }
}

impl AnnounceHandler for ctx::Handle<ctx::PreliminaryCtx> {
    fn generate_request(&mut self, sequence_num: usize) -> AnnounceRequest {
        self.with(|ctx| AnnounceRequest {
            info_hash: *ctx.magnet.info_hash(),
            downloaded: 0,
            left: 0,
            uploaded: 0,
            local_peer_id: *ctx.const_data.local_peer_id(),
            listener_port: ctx.const_data.pwp_external_port(),
            event: (sequence_num == 0).then_some(AnnounceEvent::Started),
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
    use crate::app::main::DownloadStrategy;
    use mtorrent_base::input::MagnetLink;
    use mtorrent_utils::peer_id::PeerId;
    use std::collections::HashSet;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
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
        let mut handle = ctx::PreliminaryCtx::new(
            magnet,
            peer_id,
            listener_addr.port(),
            6881,
            Ipv4Addr::LOCALHOST,
            Ipv6Addr::LOCALHOST,
            None,
        );

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

    #[rstest::rstest]
    #[case::leech(Mode::Leech, 0)]
    #[case::seeder(Mode::Seeder, 100)]
    fn test_main_ctx_announce_request(#[case] mode: Mode, #[case] num_want_when_complete: usize) {
        let metainfo = Metainfo::from_file("../mtorrent-cli/tests/assets/example.torrent").unwrap();
        let mut handle = ctx::MainCtx::new(
            metainfo,
            PeerId::from(&[0u8; 20]),
            6881,
            6881,
            Ipv4Addr::LOCALHOST,
            Ipv6Addr::LOCALHOST,
            None,
            DownloadStrategy::RarestFirst,
            mode,
        )
        .unwrap();
        let (total_len, piece_count, first_len, last_len) = handle.with(|ctx| {
            let piece_count = ctx.pieces.piece_count();
            (
                ctx.pieces.total_len(),
                piece_count,
                ctx.pieces.piece_len(0),
                ctx.pieces.piece_len(piece_count - 1),
            )
        });
        assert!(last_len < first_len, "test requires a short last piece");
        assert!(piece_count > 3, "test requires more than 3 pieces");

        // piece 1 is excluded: forgotten without being downloaded
        let excluded_piece = 1;
        let excluded_len = handle.with(|ctx| {
            ctx.piece_tracker.forget_piece(excluded_piece);
            ctx.pieces.piece_len(excluded_piece)
        });

        // fresh: first announce is Started, everything except the excluded piece is left
        let request = handle.generate_request(0);
        assert!(matches!(request.event, Some(AnnounceEvent::Started)), "{request:?}");
        assert_eq!(request.downloaded, 0);
        assert_eq!(request.left, total_len - excluded_len);
        assert_eq!(request.num_want, 100);

        // first piece downloaded but not verified: counts as downloaded, but still left
        handle.with(|ctx| ctx.accountant.submit_piece(0));
        let request = handle.generate_request(1);
        assert!(request.event.is_none(), "{request:?}");
        assert_eq!(request.downloaded, first_len);
        assert_eq!(request.left, total_len - excluded_len);
        assert_eq!(request.num_want, 100);

        // first piece verified, last piece downloaded and verified: only remaining pieces are left
        handle.with(|ctx| {
            ctx.piece_tracker.forget_piece(0);
            ctx.accountant.submit_piece(piece_count - 1);
            ctx.piece_tracker.forget_piece(piece_count - 1);
        });
        let request = handle.generate_request(2);
        assert!(request.event.is_none(), "{request:?}");
        assert_eq!(request.downloaded, first_len + last_len);
        assert_eq!(request.left, total_len - excluded_len - first_len - last_len);
        assert_eq!(request.num_want, 100);

        // all non-excluded pieces downloaded and verified: Completed, nothing left
        handle.with(|ctx| {
            for piece_index in (0..piece_count).filter(|&i| i != excluded_piece) {
                ctx.accountant.submit_piece(piece_index);
                ctx.piece_tracker.forget_piece(piece_index);
            }
        });
        let request = handle.generate_request(3);
        assert!(matches!(request.event, Some(AnnounceEvent::Completed)), "{request:?}");
        assert_eq!(request.downloaded, total_len - excluded_len);
        assert_eq!(request.left, 0);
        assert_eq!(request.num_want, num_want_when_complete);
    }

    #[test]
    fn test_preliminary_ctx_announce_started_only_first() {
        let magnet = "magnet:?xt=urn:btih:1EBD3DBFBB25C1333F51C99C7EE670FC2A1727C9"
            .parse::<MagnetLink>()
            .unwrap();
        let mut handle = ctx::PreliminaryCtx::new(
            magnet,
            PeerId::from(&[0u8; 20]),
            6881,
            6881,
            Ipv4Addr::LOCALHOST,
            Ipv6Addr::LOCALHOST,
            None,
        );

        let request = handle.generate_request(0);
        assert!(matches!(request.event, Some(AnnounceEvent::Started)), "{request:?}");

        let request = handle.generate_request(1);
        assert!(request.event.is_none(), "{request:?}");
    }
}
