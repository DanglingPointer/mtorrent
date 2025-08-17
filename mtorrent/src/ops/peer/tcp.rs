use local_async_utils::prelude::*;
use mtorrent_core::pwp;
use mtorrent_utils::peer_id::PeerId;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::{cmp, io};
use tokio::net::{TcpSocket, TcpStream};
use tokio::time::Instant;
use tokio::{runtime, time};

/// Re-register socket so that it will be polled on PWP runtime
macro_rules! marshal_stream {
    ($stream:expr, $rt_handle:expr) => {{
        let std_stream = $stream.into_std()?;
        std_stream.set_nodelay(true)?;
        // note: EnterGuard must NEVER live across a suspension point
        let _g = $rt_handle.enter();
        TcpStream::from_std(std_stream)?
    }};
}

fn bound_pwp_socket(local_addr: SocketAddr) -> io::Result<TcpSocket> {
    let socket = match local_addr {
        SocketAddr::V4(_) => TcpSocket::new_v4()?,
        SocketAddr::V6(_) => TcpSocket::new_v6()?,
    };

    // To use the same local addr and port for outgoing PWP connections and for TCP listener,
    // (in order to deal with endpoint-independent NAT mappings, https://www.rfc-editor.org/rfc/rfc5128#section-2.3)
    // we need to set SO_REUSEADDR on Windows, and SO_REUSEADDR and SO_REUSEPORT on Linux.
    // See https://stackoverflow.com/a/14388707/4432988 for details.
    socket.set_reuseaddr(true)?;
    #[cfg(not(windows))]
    socket.set_reuseport(true)?;
    // To avoid putting socket into TIME_WAIT when disconnecting someone, enable SO_LINGER with 0 timeout
    // See https://stackoverflow.com/a/71975993
    socket.set_linger(Some(sec!(0)))?;
    socket.bind(local_addr)?;
    Ok(socket)
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

pub async fn new_outbound_connection(
    local_peer_id: &PeerId,
    info_hash: &[u8; 20],
    extension_protocol_enabled: bool,
    remote_ip: SocketAddr,
    local_port: u16,
    pwp_runtime: runtime::Handle,
    quick: bool,
) -> io::Result<(pwp::DownloadChannels, pwp::UploadChannels, Option<pwp::ExtendedChannels>)> {
    log::debug!("Connecting to {remote_ip}...");
    let mut attempts_left = if quick { 0 } else { 3 };
    let mut timeout = if quick { sec!(5) } else { sec!(15) };

    let local_addr = match &remote_ip {
        SocketAddr::V4(_) => Ipv4Addr::UNSPECIFIED.into(),
        SocketAddr::V6(_) => Ipv6Addr::UNSPECIFIED.into(),
    };

    let (download_chans, upload_chans, extended_chans, runner) = loop {
        let connect_and_handshake = async {
            let socket = bound_pwp_socket(SocketAddr::new(local_addr, local_port))?;
            let stream = socket.connect(remote_ip).await?;
            let stream = marshal_stream!(stream, pwp_runtime);
            pwp::channels_for_outbound_connection(
                local_peer_id,
                info_hash,
                extension_protocol_enabled,
                remote_ip,
                stream,
                None,
            )
            .await
        };
        let next_attempt_time = Instant::now() + timeout;
        match time::timeout_at(next_attempt_time, connect_and_handshake)
            .await
            .map_err(io::Error::from)
        {
            Ok(Ok(channels)) => {
                break channels;
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
        timeout = cmp::min(min!(1), timeout * 2);
    };
    log::info!("Successful outgoing connection to {remote_ip}");

    pwp_runtime.spawn(async move {
        if let Err(e) = runner.await {
            log::debug!("Peer runner for {remote_ip} exited: {e}");
        }
    });

    Ok((download_chans, upload_chans, extended_chans))
}

pub async fn new_inbound_connection(
    local_peer_id: &[u8; 20],
    info_hash: &[u8; 20],
    extension_protocol_enabled: bool,
    remote_ip: SocketAddr,
    stream: TcpStream,
    pwp_runtime: runtime::Handle,
) -> io::Result<(pwp::DownloadChannels, pwp::UploadChannels, Option<pwp::ExtendedChannels>)> {
    let stream = marshal_stream!(stream, pwp_runtime);
    let (download_chans, upload_chans, extended_chans, runner) =
        pwp::channels_for_inbound_connection(
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
            log::debug!("Peer runner for {remote_ip} exited: {e}");
        }
    });

    Ok((download_chans, upload_chans, extended_chans))
}

pub async fn run_listener(
    local_addr: SocketAddr,
    mut callback: impl FnMut(TcpStream, SocketAddr),
) -> io::Result<()> {
    let socket = bound_pwp_socket(local_addr)?;
    let listener = socket.listen(1024)?;
    log::info!("TCP listener started on {}", listener.local_addr()?);
    loop {
        let (stream, addr) = listener.accept().await?;
        callback(stream, addr);
    }
}
