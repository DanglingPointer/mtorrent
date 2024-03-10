use super::ctx;
use crate::sec;
use crate::tracker::{http, udp, utils};
use crate::utils::peer_id::PeerId;
use crate::utils::{config, ip};
use futures::future;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;
use std::{cmp, io};
use tokio::net::UdpSocket;
use tokio::time;

const NUM_WANT: usize = 100;

#[derive(Clone, Copy)]
enum AnnounceEvent {
    Started,
    #[allow(dead_code)]
    Stopped,
    Completed,
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

struct AnnounceData {
    info_hash: [u8; 20],
    downloaded: usize,
    left: usize,
    uploaded: usize,
    local_peer_id: PeerId,
    listener_port: u16,
    event: Option<AnnounceEvent>,
}

impl AnnounceData {
    fn new_main(ctx: &ctx::MainCtx, listener_port: u16) -> Self {
        Self {
            info_hash: *ctx.metainfo.info_hash(),
            downloaded: ctx.accountant.accounted_bytes(),
            left: ctx.accountant.missing_bytes(),
            uploaded: ctx.peer_states.uploaded_bytes(),
            local_peer_id: ctx.local_peer_id,
            listener_port,
            event: if ctx.accountant.missing_bytes() == 0 {
                Some(AnnounceEvent::Completed)
            } else if ctx.accountant.accounted_bytes() == 0 || ctx.peer_states.iter().count() == 0 {
                Some(AnnounceEvent::Started)
            } else {
                None
            },
        }
    }

    fn new_preliminary(ctx: &ctx::PreliminaryCtx, listener_port: u16) -> Self {
        Self {
            info_hash: *ctx.magnet.info_hash(),
            downloaded: 0,
            left: 0,
            uploaded: 0,
            local_peer_id: ctx.local_peer_id,
            listener_port,
            event: Some(AnnounceEvent::Started),
        }
    }
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
            num_want: Some(NUM_WANT as i32),
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
            .numwant(NUM_WANT)
            .compact_support()
            .no_peer_id()
            .port(data.listener_port);
        if let Some(event) = data.event {
            request.event(event.into());
        }
        Ok(request)
    }
}

// ------------------------------------------------------------------------------------------------

pub struct ResponseData {
    interval: Duration,
    pub peers: Vec<SocketAddr>,
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

// ------------------------------------------------------------------------------------------------

async fn http_announce(tracker_url: &str, request: &AnnounceData) -> io::Result<ResponseData> {
    let response = http::do_announce_request((tracker_url, request).try_into()?).await?;
    response.try_into()
}

async fn udp_announce(tracker_addr: &str, request: &AnnounceData) -> io::Result<ResponseData> {
    // we need a unique udp port for each tracker
    let bind_addr = ip::any_socketaddr_from_hash(&tracker_addr);
    let socket = UdpSocket::bind(bind_addr).await?;
    socket.connect(&tracker_addr).await?;
    let client = udp::UdpTrackerConnection::from_connected_socket(socket).await?;
    let response = client.do_announce_request(request.into()).await?;
    Ok(response.into())
}

#[derive(Debug)]
enum TrackerType {
    Http,
    Udp,
}

async fn announce_periodically(
    tracker_type: TrackerType,
    tracker_addr: String,
    mut handle: ctx::Handle<ctx::MainCtx>,
    listener_port: u16,
    mut callback: impl FnMut(ResponseData),
) {
    define_with_ctx!(handle);
    loop {
        let request = with_ctx!(|ctx| AnnounceData::new_main(ctx, listener_port));
        let response_result = match tracker_type {
            TrackerType::Http => http_announce(&tracker_addr, &request).await,
            TrackerType::Udp => udp_announce(&tracker_addr, &request).await,
        };
        match response_result {
            Ok(mut response) => {
                with_ctx!(|ctx| {
                    response.peers.retain(|peer_ip| ctx.peer_states.get(peer_ip).is_none());
                });
                let interval = cmp::min(sec!(300), response.interval);
                log::info!(
                    "Received response from {tracker_type:?} tracker at {tracker_addr}: {:?}",
                    response.peers
                );
                callback(response);
                time::sleep(interval).await;
            }
            Err(e) => {
                log::error!("Announce to {tracker_type:?} tracker at {tracker_addr} failed: {e}");
                return;
            }
        }
    }
}

async fn announce_once(
    tracker_type: TrackerType,
    tracker_addr: String,
    mut handle: ctx::Handle<ctx::PreliminaryCtx>,
    listener_port: u16,
    mut callback: impl FnMut(ResponseData),
) {
    define_with_ctx!(handle);
    let request = with_ctx!(|ctx| AnnounceData::new_preliminary(ctx, listener_port));
    let response_result = match tracker_type {
        TrackerType::Http => http_announce(&tracker_addr, &request).await,
        TrackerType::Udp => udp_announce(&tracker_addr, &request).await,
    };
    match response_result {
        Ok(mut response) => {
            with_ctx!(|ctx| { response.peers.retain(|peer_ip| ctx.peers.get(peer_ip).is_none()) });
            log::info!(
                "Received response from {tracker_type:?} tracker at {tracker_addr}: {:?}",
                response.peers
            );
            callback(response);
        }
        Err(e) => log::error!("Announce to {tracker_type:?} tracker at {tracker_addr} failed: {e}"),
    }
}

// ------------------------------------------------------------------------------------------------

fn add_http_and_udp_trackers<'a>(
    supplied_trackers: impl IntoIterator<Item = &'a str>,
    config_dir: impl AsRef<Path>,
    http_trackers: &mut HashSet<String>,
    udp_trackers: &mut HashSet<String>,
) {
    macro_rules! add_trackers {
        ($trackers:expr) => {
            for tracker_addr in $trackers {
                if let Some(http) = utils::get_http_tracker_addr(&tracker_addr) {
                    http_trackers.insert(http.into());
                }
                if let Some(udp) = utils::get_udp_tracker_addr(&tracker_addr) {
                    udp_trackers.insert(udp.into());
                }
            }
        };
    }

    let supplied_trackers = supplied_trackers.into_iter().collect::<Vec<_>>();
    add_trackers!(&supplied_trackers);

    if http_trackers.is_empty() && udp_trackers.is_empty() {
        log::warn!("No trackers found - trying to load from config");
        if let Ok(loaded_trackers) = config::load_trackers(&config_dir) {
            add_trackers!(loaded_trackers);
        }
    } else {
        match config::save_trackers(config_dir, supplied_trackers) {
            Ok(()) => (),
            Err(e) => log::warn!("Failed to save trackers: {e}"),
        }
    }

    if http_trackers.is_empty() && udp_trackers.is_empty() {
        log::error!("No trackers found - download will fail");
    }
}

pub async fn make_periodic_announces(
    mut ctx_handle: ctx::Handle<ctx::MainCtx>,
    config_dir: impl AsRef<Path>,
    public_listener_port: u16,
    callback: impl FnMut(ResponseData) + Clone,
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

    let http_futures_it = http_trackers.into_iter().map(|tracker_addr| {
        announce_periodically(
            TrackerType::Http,
            tracker_addr,
            ctx_handle.clone(),
            public_listener_port,
            callback.clone(),
        )
    });
    let udp_futures_it = udp_trackers.into_iter().map(|tracker_addr| {
        announce_periodically(
            TrackerType::Udp,
            tracker_addr,
            ctx_handle.clone(),
            public_listener_port,
            callback.clone(),
        )
    });
    future::join_all(http_futures_it.chain(udp_futures_it)).await;
}

pub async fn make_preliminary_announces(
    mut ctx_handle: ctx::Handle<ctx::PreliminaryCtx>,
    config_dir: impl AsRef<Path>,
    public_listener_port: u16,
    callback: impl FnMut(ResponseData) + Clone,
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

    let http_futures_it = http_trackers.into_iter().map(|tracker_addr| {
        announce_once(
            TrackerType::Http,
            tracker_addr,
            ctx_handle.clone(),
            public_listener_port,
            callback.clone(),
        )
    });
    let udp_futures_it = udp_trackers.into_iter().map(|tracker_addr| {
        announce_once(
            TrackerType::Udp,
            tracker_addr,
            ctx_handle.clone(),
            public_listener_port,
            callback.clone(),
        )
    });
    future::join_all(http_futures_it.chain(udp_futures_it)).await;
}
