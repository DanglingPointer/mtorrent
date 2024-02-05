use super::ctx;
use crate::tracker::{http, udp, utils};
use crate::{define_with_ctx, sec};
use futures::future;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::ops::Deref;
use std::time::Duration;
use std::{cmp, io, ops};
use tokio::net::UdpSocket;
use tokio::time;

const NUM_WANT: usize = 50;
const DYNAMIC_PORT_RANGE: ops::Range<u32> = 49152..65536;

#[derive(Clone, Copy)]
enum AnnounceEvent {
    Started,
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
    local_peer_id: [u8; 20],
    listener_port: u16,
    event: Option<AnnounceEvent>,
}

impl AnnounceData {
    fn new(ctx: &ctx::Ctx, listener_port: u16) -> Self {
        Self {
            info_hash: *ctx.metainfo.info_hash(),
            downloaded: ctx.accountant.accounted_bytes(),
            left: ctx.accountant.missing_bytes(),
            uploaded: ctx.peer_states.uploaded_bytes(),
            local_peer_id: *ctx.local_peer_id.deref(),
            listener_port,
            event: if ctx.accountant.accounted_bytes() == 0 {
                Some(AnnounceEvent::Started)
            } else if ctx.accountant.missing_bytes() == 0 {
                Some(AnnounceEvent::Completed)
            } else if ctx.peer_states.all().count() == 0 {
                Some(AnnounceEvent::Stopped)
            } else {
                None
            },
        }
    }
}

impl From<&AnnounceData> for udp::AnnounceRequest {
    fn from(data: &AnnounceData) -> Self {
        Self {
            info_hash: data.info_hash,
            peer_id: data.local_peer_id,
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
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{e}")))?;
        request
            .info_hash(&data.info_hash)
            .peer_id(&data.local_peer_id)
            .bytes_downloaded(data.downloaded)
            .bytes_left(data.left)
            .bytes_uploaded(data.uploaded)
            .numwant(NUM_WANT)
            .port(data.listener_port);
        if let Some(event) = data.event {
            request.event(event.into());
        }
        Ok(request)
    }
}

pub struct ResponseData {
    pub interval: Duration,
    pub peers: Vec<SocketAddr>,
}

impl TryFrom<http::AnnounceResponseContent> for ResponseData {
    type Error = io::Error;

    fn try_from(response: http::AnnounceResponseContent) -> Result<Self, Self::Error> {
        fn make_error(what: &'static str) -> io::Error {
            io::Error::new(io::ErrorKind::Other, what.to_owned())
        }
        Ok(Self {
            interval: sec!(response
                .interval()
                .ok_or_else(|| make_error("No interval in response"))?
                as u64),
            peers: response.peers().ok_or_else(|| make_error("No peers in response"))?,
        })
    }
}

impl From<udp::AnnounceResponse> for ResponseData {
    fn from(response: udp::AnnounceResponse) -> Self {
        Self {
            interval: sec!(response.interval as u64),
            peers: response.ips,
        }
    }
}

async fn http_announce(tracker_url: &str, request: &AnnounceData) -> io::Result<ResponseData> {
    let response = http::do_announce_request((tracker_url, request).try_into()?).await?;
    response.try_into()
}

async fn udp_announce(tracker_addr: &str, request: &AnnounceData) -> io::Result<ResponseData> {
    // unique udp port for each tracker
    let bind_addr = {
        let mut hasher = DefaultHasher::default();
        tracker_addr.hash(&mut hasher);
        let hashed_addr = hasher.finish();
        let port = hashed_addr % DYNAMIC_PORT_RANGE.len() as u64 + DYNAMIC_PORT_RANGE.start as u64;
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port as u16))
    };
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

async fn run_tracker(
    tracker_type: TrackerType,
    tracker_addr: String,
    mut handle: ctx::Handle,
    listener_port: u16,
    mut callback: impl FnMut(ResponseData),
) {
    define_with_ctx!(handle);
    loop {
        let request = with_ctx!(|ctx| AnnounceData::new(ctx, listener_port));
        let response_result = match tracker_type {
            TrackerType::Http => http_announce(&tracker_addr, &request).await,
            TrackerType::Udp => udp_announce(&tracker_addr, &request).await,
        };
        match response_result {
            Ok(response) => {
                let interval = cmp::min(sec!(300), response.interval);
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

pub async fn run_periodic_announces(
    mut ctx_handle: ctx::Handle,
    listener_port: u16,
    callback: impl FnMut(ResponseData) + Clone,
) {
    define_with_ctx!(ctx_handle);
    let (http_trackers, udp_trackers) = with_ctx!(|ctx| {
        (utils::get_http_tracker_addrs(&ctx.metainfo), utils::get_udp_tracker_addrs(&ctx.metainfo))
    });
    let http_futures_it = http_trackers.into_iter().map(|tracker_addr| {
        run_tracker(
            TrackerType::Http,
            tracker_addr,
            ctx_handle.clone(),
            listener_port,
            callback.clone(),
        )
    });
    let udp_futures_it = udp_trackers.into_iter().map(|tracker_addr| {
        run_tracker(
            TrackerType::Udp,
            tracker_addr,
            ctx_handle.clone(),
            listener_port,
            callback.clone(),
        )
    });
    future::join_all(http_futures_it.chain(udp_futures_it)).await;
}
