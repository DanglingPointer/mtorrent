use super::super::PeerReporter;
use local_async_utils::prelude::*;
use mtorrent_core::pwp;
use mtorrent_utils::peer_id::PeerId;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use tokio::net::{TcpSocket, TcpStream};
use tokio::runtime;
use tokio_util::sync::CancellationToken;

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
    #[expect(deprecated)]
    socket.set_linger(Some(sec!(0)))?;
    socket.set_nodelay(true)?;
    socket.bind(local_addr)?;
    Ok(socket)
}

pub async fn new_outbound_connection(
    local_peer_id: &PeerId,
    info_hash: &[u8; 20],
    extension_protocol_enabled: bool,
    peer_addr: SocketAddr,
    local_port: u16,
    pwp_runtime: &runtime::Handle,
) -> io::Result<(pwp::DownloadChannels, pwp::UploadChannels, Option<pwp::ExtendedChannels>)> {
    let local_addr = match &peer_addr {
        SocketAddr::V4(_) => Ipv4Addr::UNSPECIFIED.into(),
        SocketAddr::V6(_) => Ipv6Addr::UNSPECIFIED.into(),
    };

    let local_peer_id = *local_peer_id;
    let info_hash = *info_hash;
    pwp_runtime
        .spawn(async move {
            let socket = bound_pwp_socket(SocketAddr::new(local_addr, local_port))?;
            let stream = socket.connect(peer_addr).await?;
            pwp::channels_for_outbound_connection(
                &local_peer_id,
                &info_hash,
                extension_protocol_enabled,
                peer_addr,
                stream,
                None,
            )
            .await
        })
        .await?
}

pub async fn new_inbound_connection(
    local_peer_id: &[u8; 20],
    info_hash: &[u8; 20],
    extension_protocol_enabled: bool,
    remote_ip: SocketAddr,
    stream: TcpStream,
    pwp_runtime: &runtime::Handle,
) -> io::Result<(pwp::DownloadChannels, pwp::UploadChannels, Option<pwp::ExtendedChannels>)> {
    let local_peer_id = *local_peer_id;
    let info_hash = *info_hash;
    pwp_runtime
        .spawn(async move {
            pwp::channels_for_inbound_connection(
                &local_peer_id,
                Some(&info_hash),
                extension_protocol_enabled,
                remote_ip,
                stream,
            )
            .await
        })
        .await?
}

pub async fn run_listener(
    local_addr: SocketAddr,
    peer_reporter: PeerReporter,
    canceller: CancellationToken,
) -> io::Result<()> {
    let task = async move {
        let socket = bound_pwp_socket(local_addr)?;
        let listener = socket.listen(1024)?;
        log::info!("TCP listener started on {}", listener.local_addr()?);
        loop {
            let (stream, addr) = listener.accept().await?;
            peer_reporter.report_accepted(addr, stream).await;
        }
    };
    canceller.run_until_cancelled_owned(task).await.unwrap_or(Ok(()))
}
