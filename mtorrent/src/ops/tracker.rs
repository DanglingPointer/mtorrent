use super::ctx;
use crate::utils::config;
use futures_util::{Future, FutureExt, TryFutureExt, future};
use local_async_utils::prelude::*;
use mtorrent_core::pwp::PeerOrigin;
use mtorrent_core::trackers::url::TrackerUrl;
use mtorrent_core::trackers::{http, udp};
use mtorrent_utils::metainfo::Metainfo;
use mtorrent_utils::peer_id::PeerId;
use std::collections::HashSet;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;
use std::{fmt, io, iter};
use tokio::net::{self, UdpSocket};
use tokio::time;

pub async fn make_periodic_announces(
    mut ctx_handle: ctx::Handle<ctx::MainCtx>,
    config_dir: impl AsRef<Path>,
    cb_channel: local_channel::Sender<(SocketAddr, PeerOrigin)>,
) {
    define_with_ctx!(ctx_handle);

    let (http_trackers, udp_trackers) = with_ctx!(|ctx| {
        let mut http_trackers = HashSet::new();
        let mut udp_trackers = HashSet::new();

        add_http_and_udp_trackers(
            trackers_from_metainfo(&ctx.metainfo),
            config_dir,
            &mut http_trackers,
            &mut udp_trackers,
        );
        (http_trackers, udp_trackers)
    });

    launch_announces(udp_trackers, http_trackers, ctx_handle, cb_channel).await;
}

pub async fn make_preliminary_announces(
    mut ctx_handle: ctx::Handle<ctx::PreliminaryCtx>,
    config_dir: impl AsRef<Path>,
    cb_channel: local_channel::Sender<(SocketAddr, PeerOrigin)>,
) {
    define_with_ctx!(ctx_handle);

    let (http_trackers, udp_trackers) = with_ctx!(|ctx| {
        let mut http_trackers = HashSet::new();
        let mut udp_trackers = HashSet::new();

        add_http_and_udp_trackers(
            ctx.magnet.trackers(),
            config_dir,
            &mut http_trackers,
            &mut udp_trackers,
        );
        (http_trackers, udp_trackers)
    });

    launch_announces(udp_trackers, http_trackers, ctx_handle, cb_channel).await;
}

// ------------------------------------------------------------------------------------------------

fn add_http_and_udp_trackers<'a>(
    supplied_trackers: impl IntoIterator<Item = &'a str>,
    config_dir: impl AsRef<Path>,
    http_trackers: &mut HashSet<String>,
    udp_trackers: &mut HashSet<String>,
) {
    let supplied_trackers: HashSet<_> = supplied_trackers
        .into_iter()
        .filter(|addr| TrackerUrl::from_str(addr).is_ok())
        .map(|addr| addr.to_string())
        .collect();

    if !supplied_trackers.is_empty() {
        match config::save_trackers(&config_dir, supplied_trackers) {
            Ok(()) => (),
            Err(e) => log::warn!("Failed to save trackers to file: {e}"),
        }
    }

    match config::load_trackers(config_dir) {
        Ok(loaded_trackers) => {
            for tracker_addr in loaded_trackers {
                match tracker_addr.parse::<TrackerUrl>() {
                    Ok(TrackerUrl::Http(addr)) => {
                        http_trackers.insert(addr);
                    }
                    Ok(TrackerUrl::Udp(addr)) => {
                        udp_trackers.insert(addr);
                    }
                    Err(e) => {
                        log::error!("Failed to parse tracker URL '{tracker_addr}': {e}");
                    }
                }
            }
        }
        Err(e) => {
            log::warn!("Failed to load trackers from file: {e}");
        }
    }

    if http_trackers.is_empty() && udp_trackers.is_empty() {
        log::warn!("No trackers available");
    }
}

async fn launch_announces(
    udp_trackers: impl IntoIterator<Item = String>,
    http_trackers: impl IntoIterator<Item = String>,
    handler: impl AnnounceHandler + Clone,
    cb_channel: local_channel::Sender<(SocketAddr, PeerOrigin)>,
) {
    let udp_futures_it = udp_trackers.into_iter().map(|tracker_addr| async {
        let tracker_addr_copy = tracker_addr.clone();
        match new_udp_client(tracker_addr).await {
            Ok(udp_client) => {
                announce_periodically(udp_client, handler.clone(), cb_channel.clone()).await;
            }
            Err(e) => {
                log::error!("Failed to create UDP tracker client for {tracker_addr_copy}: {e}");
            }
        }
    });

    match http::TrackerClient::new() {
        Ok(http_client) => {
            let http_futures_it = http_trackers.into_iter().map(|tracker_addr| {
                announce_periodically(
                    (http_client.clone(), tracker_addr),
                    handler.clone(),
                    cb_channel.clone(),
                )
                .boxed_local()
            });
            future::join_all(http_futures_it.chain(udp_futures_it.map(FutureExt::boxed_local)))
                .await;
        }
        Err(e) => {
            log::error!("Failed to create HTTP tracker client: {e}");
            future::join_all(udp_futures_it).await;
        }
    }
}

async fn announce_periodically(
    mut client: impl TrackerClient,
    mut handler: impl AnnounceHandler,
    cb_channel: local_channel::Sender<(SocketAddr, PeerOrigin)>,
) {
    loop {
        let request = handler.generate_request();
        match client.announce(&request).await {
            Ok(mut response) => {
                log::info!("Received response from {}: {:?}", client.name(), response);
                handler.process_response(&mut response);
                let interval = response.interval.clamp(sec!(5), sec!(300));
                for peer_addr in response.peers {
                    cb_channel.send((peer_addr, PeerOrigin::Tracker));
                }
                time::sleep(interval).await;
            }
            Err(e) => {
                log::error!("Announce to {} failed: {}", client.name(), e);
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

trait TrackerClient {
    fn announce(
        &mut self,
        request: &AnnounceData,
    ) -> impl Future<Output = io::Result<ResponseData>>;

    fn name(&self) -> impl fmt::Display;
}

impl TrackerClient for (http::TrackerClient, String) {
    async fn announce(&mut self, request: &AnnounceData) -> io::Result<ResponseData> {
        let request_builder = HttpAnnounceInput((self.1.as_ref(), request)).try_into()?;
        let response = self.0.announce(request_builder).await?;
        response.try_into()
    }

    fn name(&self) -> impl fmt::Display {
        format!("HTTP tracker at {}", self.1)
    }
}

impl TrackerClient for udp::TrackerConnection {
    async fn announce(&mut self, request: &AnnounceData) -> io::Result<ResponseData> {
        self.do_announce_request(request.into()).await.map(Into::into)
    }

    fn name(&self) -> impl fmt::Display {
        format!("UDP tracker at {}", self.remote_addr())
    }
}

async fn new_udp_client(tracker_addr_str: String) -> io::Result<udp::TrackerConnection> {
    async fn bind_and_connect_socket(
        bind_addr: &SocketAddr,
        remote_addr: &SocketAddr,
    ) -> io::Result<UdpSocket> {
        let socket = UdpSocket::bind(bind_addr).await?;
        socket.connect(&remote_addr).await?;
        Ok(socket)
    }

    for tracker_addr in net::lookup_host(tracker_addr_str).await? {
        let local_ip = match &tracker_addr {
            SocketAddr::V4(_) => Ipv4Addr::UNSPECIFIED.into(),
            SocketAddr::V6(_) => Ipv6Addr::UNSPECIFIED.into(),
        };
        let local_addr = SocketAddr::new(local_ip, 0);
        if let Ok(client) = bind_and_connect_socket(&local_addr, &tracker_addr)
            .and_then(udp::TrackerConnection::from_connected_socket)
            .await
        {
            return Ok(client);
        }
    }
    Err(io::Error::new(io::ErrorKind::ConnectionRefused, "failed to connect to tracker"))
}

// ------------------------------------------------------------------------------------------------

trait AnnounceHandler {
    fn generate_request(&mut self) -> AnnounceData;
    fn process_response(&mut self, response: &mut ResponseData);
}

impl AnnounceHandler for ctx::Handle<ctx::MainCtx> {
    fn generate_request(&mut self) -> AnnounceData {
        self.with(|ctx| AnnounceData {
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

    fn process_response(&mut self, response: &mut ResponseData) {
        self.with(|ctx| response.peers.retain(|peer_ip| ctx.peer_states.get(peer_ip).is_none()));
    }
}

impl AnnounceHandler for ctx::Handle<ctx::PreliminaryCtx> {
    fn generate_request(&mut self) -> AnnounceData {
        self.with(|ctx| AnnounceData {
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

    fn process_response(&mut self, response: &mut ResponseData) {
        self.with(|ctx| response.peers.retain(|peer_ip| !ctx.reachable_peers.contains(peer_ip)));
    }
}

// ------------------------------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum AnnounceEvent {
    Started,
    #[expect(dead_code)]
    Stopped,
    Completed,
}

struct AnnounceData {
    info_hash: [u8; 20],
    downloaded: usize,
    left: usize,
    uploaded: usize,
    local_peer_id: PeerId,
    listener_port: u16,
    event: Option<AnnounceEvent>,
    num_want: usize,
}

impl From<&AnnounceData> for udp::AnnounceRequest {
    fn from(data: &AnnounceData) -> Self {
        Self {
            info_hash: data.info_hash,
            peer_id: *data.local_peer_id,
            downloaded: data.downloaded as u64,
            left: data.left as u64,
            uploaded: data.uploaded as u64,
            event: data.event.map(Into::into).unwrap_or(udp::AnnounceEvent::None),
            ip: None,
            key: 0,
            num_want: Some(data.num_want as i32),
            port: data.listener_port,
        }
    }
}

struct HttpAnnounceInput<'a>((&'a str, &'a AnnounceData));

impl<'a> TryFrom<HttpAnnounceInput<'a>> for http::TrackerRequestBuilder {
    type Error = io::Error;

    fn try_from(
        HttpAnnounceInput((url, data)): HttpAnnounceInput<'_>,
    ) -> Result<Self, Self::Error> {
        let mut request = http::TrackerRequestBuilder::try_from(url)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, Box::new(e)))?;
        request
            .info_hash(&data.info_hash)
            .peer_id(data.local_peer_id.as_slice())
            .bytes_downloaded(data.downloaded)
            .bytes_left(data.left)
            .bytes_uploaded(data.uploaded)
            .numwant(data.num_want)
            .compact_support()
            .no_peer_id()
            .port(data.listener_port);
        if let Some(event) = data.event {
            request.event(event.into());
        }
        Ok(request)
    }
}

impl From<AnnounceEvent> for http::AnnounceEvent {
    fn from(event: AnnounceEvent) -> Self {
        match event {
            AnnounceEvent::Started => http::AnnounceEvent::Started,
            AnnounceEvent::Stopped => http::AnnounceEvent::Stopped,
            AnnounceEvent::Completed => http::AnnounceEvent::Completed,
        }
    }
}

impl From<AnnounceEvent> for udp::AnnounceEvent {
    fn from(event: AnnounceEvent) -> Self {
        match event {
            AnnounceEvent::Started => udp::AnnounceEvent::Started,
            AnnounceEvent::Stopped => udp::AnnounceEvent::Stopped,
            AnnounceEvent::Completed => udp::AnnounceEvent::Completed,
        }
    }
}

// ------------------------------------------------------------------------------------------------

struct ResponseData {
    interval: Duration,
    peers: Vec<SocketAddr>,
}

impl fmt::Debug for ResponseData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("")
            .field("interval", &self.interval)
            .field("peers", &self.peers)
            .finish()
    }
}

impl TryFrom<http::AnnounceResponseContent> for ResponseData {
    type Error = io::Error;

    fn try_from(response: http::AnnounceResponseContent) -> Result<Self, Self::Error> {
        fn make_error(s: &'static str) -> impl FnOnce() -> io::Error {
            move || io::Error::new(io::ErrorKind::InvalidData, s)
        }
        let interval =
            sec!(response.interval().ok_or_else(make_error("no interval in response"))? as u64);
        let mut peers = response.peers().ok_or_else(make_error("no peers in response"))?;
        // some udp trackers send 0.0.0.0:65535 and 0.0.0.0 (for obfuscation?)
        peers.retain(|peer_ip| !peer_ip.ip().is_unspecified());
        Ok(Self { interval, peers })
    }
}

impl From<udp::AnnounceResponse> for ResponseData {
    fn from(response: udp::AnnounceResponse) -> Self {
        let mut peers = response.ips;
        // some udp trackers send 0.0.0.0:65535 and 0.0.0.0 (for obfuscation?)
        peers.retain(|peer_ip| !peer_ip.ip().is_unspecified());
        Self {
            interval: sec!(response.interval as u64),
            peers,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::startup;
    use mtorrent_utils::{benc, ip};
    use std::cell::Cell;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::rc::Rc;
    use std::{fs, iter};
    use tokio::net::UdpSocket;

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

            let mut http_trackers = HashSet::new();
            let mut udp_trackers = HashSet::new();
            add_http_and_udp_trackers(
                supplied_trackers,
                config_dir,
                &mut http_trackers,
                &mut udp_trackers,
            );

            assert_eq!(
                http_trackers,
                ["https://example.com"].into_iter().map(Into::into).collect()
            );
            assert_eq!(udp_trackers, ["open.stealth.si:80"].into_iter().map(Into::into).collect());
        }

        {
            let mut http_trackers = HashSet::new();
            let mut udp_trackers = HashSet::new();
            add_http_and_udp_trackers(
                iter::empty(),
                config_dir,
                &mut http_trackers,
                &mut udp_trackers,
            );
            assert_eq!(
                http_trackers,
                ["https://example.com"].into_iter().map(Into::into).collect()
            );
            assert_eq!(udp_trackers, ["open.stealth.si:80"].into_iter().map(Into::into).collect());
        }

        {
            let supplied_trackers = ["http://tracker1.com", "udp://tracker.tiny-vps.com:6969"];

            let mut http_trackers = HashSet::new();
            let mut udp_trackers = HashSet::new();
            add_http_and_udp_trackers(
                supplied_trackers,
                config_dir,
                &mut http_trackers,
                &mut udp_trackers,
            );
            assert_eq!(
                http_trackers,
                ["https://example.com", "http://tracker1.com"]
                    .into_iter()
                    .map(Into::into)
                    .collect()
            );
            assert_eq!(
                udp_trackers,
                ["open.stealth.si:80", "tracker.tiny-vps.com:6969"]
                    .into_iter()
                    .map(Into::into)
                    .collect()
            );
        }

        fs::remove_dir_all(config_dir).unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn test_udp_announce() {
        let metainfo = startup::read_metainfo("tests/assets/example.torrent").unwrap();
        let udp_tracker_addrs =
            trackers_from_metainfo(&metainfo).filter_map(|addr| match TrackerUrl::from_str(addr) {
                Ok(TrackerUrl::Udp(addr)) => Some(addr),
                _ => None,
            });

        let local_ip = SocketAddr::V4(SocketAddrV4::new(ip::get_local_addr().unwrap(), 6666));

        let announce_request = udp::AnnounceRequest {
            info_hash: *metainfo.info_hash(),
            peer_id: [b'm'; 20],
            downloaded: 0,
            left: 200,
            uploaded: 0,
            event: udp::AnnounceEvent::None,
            ip: None,
            key: 0,
            num_want: Some(5),
            port: local_ip.port(),
        };

        for tracker_addr in udp_tracker_addrs {
            let client_socket = UdpSocket::bind(local_ip).await.unwrap();
            client_socket
                .connect(&tracker_addr)
                .await
                .unwrap_or_else(|e| panic!("Failed to connect to {}: {}", &tracker_addr, e));

            let mut client =
                udp::TrackerConnection::from_connected_socket(client_socket).await.unwrap();

            let response = client
                .do_announce_request(announce_request.clone())
                .await
                .unwrap_or_else(|e| panic!("Announce error: {e}"));

            println!("Announce response: {response:?}");
            let peer_count = response.ips.len();
            let seeders = response.seeders as usize;
            let leechers = response.leechers as usize;
            assert_eq!(peer_count, seeders + leechers);
        }
    }

    #[ignore]
    #[tokio::test]
    async fn test_udp_scrape() {
        let _ = simple_logger::SimpleLogger::new()
            .with_level(log::LevelFilter::Off)
            .with_module_level("mtorrent::tracker::udp", log::LevelFilter::Trace)
            .init();

        let udp_tracker_addrs = [
            "udp://open.stealth.si:80/announce",
            "udp://tracker.opentrackr.org:1337/announce",
            "udp://tracker.tiny-vps.com:6969/announce",
            "udp://tracker.internetwarriors.net:1337/announce",
            "udp://tracker.skyts.net:6969/announce",
        ];

        let success_count = Rc::new(Cell::new(0usize));
        let local_set = tokio::task::LocalSet::new();

        for (i, tracker_addr) in udp_tracker_addrs
            .iter()
            .filter_map(|uri| {
                let udp_addr = uri.strip_prefix("udp://")?;
                if let Some(stripped) = udp_addr.strip_suffix("/announce") {
                    Some(stripped.to_string())
                } else {
                    Some(udp_addr.to_string())
                }
            })
            .enumerate()
        {
            let success_count = success_count.clone();
            local_set.spawn_local(async move {
                let bind_addr =
                    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 6666 + i as u16));
                let socket = UdpSocket::bind(bind_addr).await.unwrap();
                socket.connect(&tracker_addr).await.unwrap();

                match tokio::time::timeout(
                    sec!(10),
                    udp::TrackerConnection::from_connected_socket(socket),
                )
                .await
                {
                    Ok(Ok(mut client)) => {
                        let response = client
                            .do_scrape_request(udp::ScrapeRequest {
                                info_hashes: Vec::new(),
                            })
                            .await
                            .unwrap();
                        println!("Response from {tracker_addr}: {response:?}");
                        success_count.set(success_count.get() + 1);
                    }
                    _ => {
                        eprintln!("Failed to connect to {tracker_addr}");
                    }
                }
            });
        }
        local_set.await;
        assert!(success_count.get() > 0);
    }

    #[ignore]
    #[tokio::test]
    async fn test_https_scrape_and_announce() {
        let _ = simple_logger::SimpleLogger::new()
            .with_level(log::LevelFilter::Off)
            .with_module_level("mtorrent::tracker::http", log::LevelFilter::Trace)
            .init();

        let tracker_url = "https://torrent.ubuntu.com/announce";
        let client = http::TrackerClient::new().unwrap();

        let request = http::TrackerRequestBuilder::try_from(tracker_url).unwrap();
        let response = client.scrape(request).await.unwrap_or_else(|e| panic!("Scrape error: {e}"));

        if let benc::Element::Dictionary(root) = response {
            for (key, files) in root {
                if let benc::Element::Dictionary(torrents) = files {
                    for (info_hash_bytes, info) in torrents {
                        if let benc::Element::ByteString(info_hash) = info_hash_bytes {
                            println!("Announce for torrent: {info}");
                            let mut request =
                                http::TrackerRequestBuilder::try_from(tracker_url).unwrap();
                            request
                                .info_hash(&info_hash)
                                .peer_id(&[b'm'; 20])
                                .bytes_left(0)
                                .bytes_uploaded(0)
                                .bytes_downloaded(0)
                                // .compact_support()
                                .port(6666);

                            let response = client
                                .announce(request)
                                .await
                                .unwrap_or_else(|e| panic!("Announce error: {e}"));

                            let peer_count = response.peers().unwrap().len();
                            let seeders = response.complete().unwrap();
                            let leechers = response.incomplete().unwrap();
                            assert!(peer_count <= seeders + leechers);
                        }
                    }
                } else {
                    panic!("'{key}' value is not a dictionary");
                }
            }
        } else {
            panic!("Scrape response is not a dictionary");
        }
    }
}
