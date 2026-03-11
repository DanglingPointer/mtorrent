use super::super::PeerReporter;
use bytes::BytesMut;
use mtorrent_core::{pe, pwp};
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
    // To avoid putting socket into TIME_WAIT when disconnecting someone, enable SO_LINGER with 0
    // timeout See https://stackoverflow.com/a/71975993
    socket.set_zero_linger()?;
    socket.set_nodelay(true)?;
    socket.bind(local_addr)?;
    Ok(socket)
}

pub async fn new_outbound_connection(
    local_peer_id: &PeerId,
    info_hash: &[u8; 20],
    extension_protocol_enabled: bool,
    protocol_encryption_enabled: bool,
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
            let mut stream = socket.connect(peer_addr).await?;
            let crypto = if protocol_encryption_enabled {
                pe::outbound_handshake(&mut stream, &info_hash, &[0u8; 0][..]).await?
            } else {
                None
            };
            pwp::channels_for_outbound_connection(
                &local_peer_id,
                &info_hash,
                extension_protocol_enabled,
                peer_addr,
                stream,
                None,
                crypto,
            )
            .await
        })
        .await?
}

pub async fn new_inbound_connection(
    local_peer_id: &PeerId,
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
            match pe::is_stream_unencrypted(stream).await? {
                pe::MaybeEncrypted::Plain(stream) => {
                    pwp::channels_for_inbound_connection(
                        &local_peer_id,
                        &info_hash,
                        extension_protocol_enabled,
                        remote_ip,
                        stream,
                        None,
                    )
                    .await
                }
                pe::MaybeEncrypted::Encrypted(mut stream) => {
                    let mut ia_buffer = BytesMut::new();
                    let crypto =
                        pe::inbound_handshake(&mut stream, &info_hash, &mut ia_buffer).await?;
                    stream.replace_prefix(ia_buffer.freeze());
                    pwp::channels_for_inbound_connection(
                        &local_peer_id,
                        &info_hash,
                        extension_protocol_enabled,
                        remote_ip,
                        stream,
                        crypto,
                    )
                    .await
                }
            }
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
