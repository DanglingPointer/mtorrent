mod http;
mod udp;
mod utils;

#[cfg(test)]
mod tests;

use super::ctx;
use crate::sec;
use crate::utils::peer_id::PeerId;
use crate::utils::shared::Shared;
use crate::utils::{config, local_sync};
use futures::{future, Future, FutureExt, TryFutureExt};
use std::collections::HashSet;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::time::Duration;
use std::{fmt, io};
use tokio::net::{self, UdpSocket};
use tokio::time;

pub async fn make_periodic_announces(
    mut ctx_handle: ctx::Handle<ctx::MainCtx>,
    config_dir: impl AsRef<Path>,
    cb_channel: local_sync::channel::Sender<SocketAddr>,
) {
    define_with_ctx!(ctx_handle);

    let (http_trackers, udp_trackers) = with_ctx!(|ctx| {
        let mut http_trackers = HashSet::new();
        let mut udp_trackers = HashSet::new();

        add_http_and_udp_trackers(
            utils::trackers_from_metainfo(&ctx.metainfo),
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
    cb_channel: local_sync::channel::Sender<SocketAddr>,
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
        .filter(|addr| {
            utils::get_http_tracker_addr(addr).is_some()
                || utils::get_udp_tracker_addr(addr).is_some()
        })
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
                if let Some(http) = utils::get_http_tracker_addr(&tracker_addr) {
                    http_trackers.insert(http.into());
                }
                if let Some(udp) = utils::get_udp_tracker_addr(&tracker_addr) {
                    udp_trackers.insert(udp.into());
                }
            }
        }
        Err(e) => {
            log::warn!("Failed to load trackers from file: {e}");
        }
    }

    if http_trackers.is_empty() && udp_trackers.is_empty() {
        log::error!("No trackers available - download will fail");
    }
}

async fn launch_announces(
    udp_trackers: impl IntoIterator<Item = String>,
    http_trackers: impl IntoIterator<Item = String>,
    handler: impl AnnounceHandler + Clone,
    cb_channel: local_sync::channel::Sender<SocketAddr>,
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

    match http::Client::new() {
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
    cb_channel: local_sync::channel::Sender<SocketAddr>,
) {
    loop {
        let request = handler.generate_request();
        match client.announce(&request).await {
            Ok(mut response) => {
                log::info!("Received response from {}: {:?}", client.name(), response);
                handler.process_response(&mut response);
                let interval = response.interval.clamp(sec!(5), sec!(300));
                for peer_addr in response.peers {
                    cb_channel.send(peer_addr);
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

// ------------------------------------------------------------------------------------------------

trait TrackerClient {
    fn announce(
        &mut self,
        request: &AnnounceData,
    ) -> impl Future<Output = io::Result<ResponseData>>;

    fn name(&self) -> impl fmt::Display;
}

impl TrackerClient for (http::Client, String) {
    async fn announce(&mut self, request: &AnnounceData) -> io::Result<ResponseData> {
        let response = self.0.announce((self.1.as_ref(), request).try_into()?).await?;
        response.try_into()
    }

    fn name(&self) -> impl fmt::Display {
        format!("HTTP tracker at {}", self.1)
    }
}

impl TrackerClient for udp::UdpTrackerConnection {
    async fn announce(&mut self, request: &AnnounceData) -> io::Result<ResponseData> {
        self.do_announce_request(request.into()).await.map(Into::into)
    }

    fn name(&self) -> impl fmt::Display {
        format!("UDP tracker at {}", self.remote_addr())
    }
}

async fn new_udp_client(tracker_addr_str: String) -> io::Result<udp::UdpTrackerConnection> {
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
            .and_then(udp::UdpTrackerConnection::from_connected_socket)
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
        self.with(|ctx| response.peers.retain(|peer_ip| !ctx.known_peers.contains(peer_ip)));
    }
}

// ------------------------------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum AnnounceEvent {
    Started,
    #[allow(dead_code)]
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

impl TryFrom<(&str, &AnnounceData)> for http::TrackerRequestBuilder {
    type Error = io::Error;

    fn try_from((url, data): (&str, &AnnounceData)) -> Result<Self, Self::Error> {
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
