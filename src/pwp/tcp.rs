use super::channels::{channels_from_incoming, channels_from_outgoing};
use crate::sec;
use crate::utils::ip;
use crate::utils::peer_id::PeerId;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio::{runtime, time};

macro_rules! marshal_stream {
    ($stream:expr, $rt_handle:expr) => {{
        let std_stream = $stream.into_std()?;
        std_stream.set_nodelay(true)?;
        // note: EnterGuard must NEVER live across a suspension point
        let _g = $rt_handle.enter();
        TcpStream::from_std(std_stream)?
    }};
}

fn can_retry(e: &io::Error, attempts_left: usize) -> bool {
    attempts_left > 0
        && matches!(
            e.kind(),
            io::ErrorKind::ConnectionRefused
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::UnexpectedEof
                | io::ErrorKind::TimedOut
        )
}

pub async fn channels_for_outgoing_connection(
    local_peer_id: &PeerId,
    info_hash: &[u8; 20],
    extension_protocol_enabled: bool,
    remote_ip: SocketAddr,
    local_port: u16,
    pwp_runtime: runtime::Handle,
) -> io::Result<(super::DownloadChannels, super::UploadChannels, Option<super::ExtendedChannels>)> {
    log::debug!("Connecting to {remote_ip}...");
    const MAX_RETRY_COUNT: usize = 3;
    let mut attempts_left = MAX_RETRY_COUNT;
    let mut timeout = sec!(15);

    let local_addr = match &remote_ip {
        SocketAddr::V4(_) => Ipv4Addr::UNSPECIFIED.into(),
        SocketAddr::V6(_) => Ipv6Addr::UNSPECIFIED.into(),
    };
    let stream = loop {
        let socket = ip::bound_tcp_socket(SocketAddr::new(local_addr, local_port))?;
        let next_attempt_time = Instant::now() + timeout;
        match time::timeout_at(next_attempt_time, socket.connect(remote_ip))
            .await
            .map_err(io::Error::from)
        {
            Ok(Ok(stream)) => {
                break stream;
            }
            Ok(Err(e)) | Err(e) if !can_retry(&e, attempts_left) => {
                return Err(e);
            }
            Ok(Err(_)) => {
                time::sleep_until(next_attempt_time).await;
            }
            _ => (),
        }
        attempts_left -= 1;
        timeout *= 2;
    };
    // re-register socket so that it will be polled on PWP runtime
    let stream = marshal_stream!(stream, pwp_runtime);
    let (download_chans, upload_chans, extended_chans, runner) = channels_from_outgoing(
        local_peer_id,
        info_hash,
        extension_protocol_enabled,
        remote_ip,
        stream,
        None,
    )
    .await?;
    log::info!("Successful outgoing connection to {remote_ip}");

    pwp_runtime.spawn(async move {
        if let Err(e) = runner.await {
            log::debug!("Peer runner exited: {}", e);
        }
    });

    Ok((download_chans, upload_chans, extended_chans))
}

pub async fn channels_for_incoming_connection(
    local_peer_id: &[u8; 20],
    info_hash: &[u8; 20],
    extension_protocol_enabled: bool,
    remote_ip: SocketAddr,
    stream: TcpStream,
    pwp_runtime: runtime::Handle,
) -> io::Result<(super::DownloadChannels, super::UploadChannels, Option<super::ExtendedChannels>)> {
    // re-register socket so that it will be polled on PWP runtime
    let stream = marshal_stream!(stream, pwp_runtime);
    let (download_chans, upload_chans, extended_chans, runner) = channels_from_incoming(
        local_peer_id,
        Some(info_hash),
        extension_protocol_enabled,
        remote_ip,
        stream,
    )
    .await?;
    log::info!("Successful incoming connection from {remote_ip}");

    pwp_runtime.spawn(async move {
        if let Err(e) = runner.await {
            log::debug!("Peer runner exited: {}", e);
        }
    });

    Ok((download_chans, upload_chans, extended_chans))
}

pub async fn run_listener(
    local_addr: SocketAddr,
    mut callback: impl FnMut(TcpStream, SocketAddr),
) -> io::Result<()> {
    let socket = ip::bound_tcp_socket(local_addr)?;
    let listener = socket.listen(1024)?;
    log::info!("TCP listener started on {}", listener.local_addr()?);
    loop {
        let (stream, addr) = listener.accept().await?;
        callback(stream, addr);
    }
}
