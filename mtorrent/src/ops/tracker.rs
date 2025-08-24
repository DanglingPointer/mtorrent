use super::ctx;
use crate::utils::config;
use futures_util::future;
use local_async_utils::prelude::*;
use mtorrent_core::pwp::PeerOrigin;
use mtorrent_core::trackers::*;
use mtorrent_utils::metainfo::Metainfo;
use std::iter;
use std::net::SocketAddr;
use std::path::Path;
use tokio::time;

pub async fn make_periodic_announces(
    mut ctx_handle: ctx::Handle<ctx::MainCtx>,
    trackers_handle: TrackerClient,
    config_dir: impl AsRef<Path>,
    cb_channel: local_channel::Sender<(SocketAddr, PeerOrigin)>,
) {
    define_with_ctx!(ctx_handle);
    let tracker_urls =
        with_ctx!(|ctx| update_tracker_urls(trackers_from_metainfo(&ctx.metainfo), config_dir));
    launch_announces(&trackers_handle, ctx_handle, tracker_urls, cb_channel).await;
}

pub async fn make_preliminary_announces(
    mut ctx_handle: ctx::Handle<ctx::PreliminaryCtx>,
    trackers_handle: TrackerClient,
    config_dir: impl AsRef<Path>,
    cb_channel: local_channel::Sender<(SocketAddr, PeerOrigin)>,
) {
    define_with_ctx!(ctx_handle);
    let tracker_urls = with_ctx!(|ctx| update_tracker_urls(ctx.magnet.trackers(), config_dir));
    launch_announces(&trackers_handle, ctx_handle, tracker_urls, cb_channel).await;
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
            all_trackers.extend(loaded_trackers.filter_map(|s| s.parse::<TrackerUrl>().ok()));
        }
        Err(e) => {
            log::warn!("Failed to load trackers from file: {e}");
        }
    }
    // note that when saving trackers below, the '/announce' suffix disappears for udp urls
    match config::save_trackers(&config_dir, all_trackers.iter().cloned()) {
        Ok(()) => (),
        Err(e) => log::warn!("Failed to save trackers to file: {e}"),
    }
    all_trackers
}

async fn launch_announces(
    trackers_handle: &TrackerClient,
    handler: impl AnnounceHandler + Clone,
    tracker_urls: impl IntoIterator<Item = TrackerUrl>,
    cb_channel: local_channel::Sender<(SocketAddr, PeerOrigin)>,
) {
    future::join_all(tracker_urls.into_iter().map(|url| {
        announce_periodically(trackers_handle, url, handler.clone(), cb_channel.clone())
    }))
    .await;
}

async fn announce_periodically(
    trackers_handle: &TrackerClient,
    url: TrackerUrl,
    mut handler: impl AnnounceHandler,
    cb_channel: local_channel::Sender<(SocketAddr, PeerOrigin)>,
) {
    loop {
        let request = handler.generate_request();
        match trackers_handle.announce(url.clone(), request).await {
            Ok(mut response) => {
                log::info!("Received response from {url:?}: {response:?}");
                handler.preprocess_response(&mut response);
                let interval = response.interval.clamp(sec!(5), sec!(300));
                for peer_addr in response.peers {
                    cb_channel.send((peer_addr, PeerOrigin::Tracker));
                }
                time::sleep(interval).await;
            }
            Err(e) => {
                log::error!("Announce to {url:?} failed: {e}");
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
        self.with(|ctx| response.peers.retain(|peer_ip| !ctx.reachable_peers.contains(peer_ip)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::{fs, iter};

    #[test]
    fn test_extract_trackers_from_metainfo() {
        fn get_udp_tracker_addr(tracker: &str) -> Option<String> {
            match tracker.parse::<TrackerUrl>() {
                Ok(TrackerUrl::Udp(url)) => Some(url),
                _ => None,
            }
        }

        fn get_http_tracker_addr(tracker: &str) -> Option<String> {
            match tracker.parse::<TrackerUrl>() {
                Ok(TrackerUrl::Http(url)) => Some(url),
                _ => None,
            }
        }

        let data = fs::read("tests/assets/example.torrent").unwrap();
        let info = Metainfo::new(&data).unwrap();

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

        let data = fs::read("tests/assets/pcap.torrent").unwrap();
        let info = Metainfo::new(&data).unwrap();

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
                    TrackerUrl::Udp("open.stealth.si:80".to_owned()),
                    TrackerUrl::Http("https://example.com".to_owned()),
                ]
            );
        }

        {
            let updated_trackers = update_tracker_urls(iter::empty(), config_dir);

            assert_eq!(
                updated_trackers,
                vec![
                    TrackerUrl::Http("https://example.com".to_owned()),
                    TrackerUrl::Udp("open.stealth.si:80".to_owned()),
                ]
            );
        }

        {
            let supplied_trackers = ["http://tracker1.com", "udp://tracker.tiny-vps.com:6969"];

            let updated_trackers = update_tracker_urls(supplied_trackers, config_dir);

            assert_eq!(
                updated_trackers,
                vec![
                    TrackerUrl::Http("http://tracker1.com".to_owned()),
                    TrackerUrl::Udp("tracker.tiny-vps.com:6969".to_owned()),
                    TrackerUrl::Http("https://example.com".to_owned()),
                    TrackerUrl::Udp("open.stealth.si:80".to_owned()),
                ]
            );
        }

        fs::remove_dir_all(config_dir).unwrap();
    }
}
