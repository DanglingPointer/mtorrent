use super::super::PeerReporter;
use super::ctx;
use bytes::BytesMut;
use mtorrent_core::{pe, pwp};
use mtorrent_utils::peer_id::PeerId;
use mtorrent_utils::{info_stopwatch, net};
use std::io;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::runtime;

pub async fn new_outbound_connection(
    data: &ctx::ConstData,
    info_hash: &[u8; 20],
    extension_protocol_enabled: bool,
    protocol_encryption_enabled: bool,
    peer_addr: SocketAddr,
    pwp_runtime: &runtime::Handle,
) -> io::Result<(pwp::DownloadChannels, pwp::UploadChannels, Option<pwp::ExtendedChannels>)> {
    let local_addr = match &peer_addr {
        SocketAddr::V4(_) => data.local_ip_v4().into(),
        SocketAddr::V6(_) => data.local_ip_v6().into(),
    };

    let local_peer_id = *data.local_peer_id();
    let info_hash = *info_hash;
    let interface = data.bind_interface().map(ToOwned::to_owned);
    let local_port = data.pwp_internal_port();
    pwp_runtime
        .spawn(async move {
            let socket = net::bound_tcp_socket(
                SocketAddr::new(local_addr, local_port),
                interface.as_deref(),
            )?;
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
            match pe::detect_encryption(stream).await? {
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
                    let (_, stream) = stream.into_parts();
                    let stream = pe::PrefixedStream::new(ia_buffer, stream);
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

pub async fn run_pwp_listener(
    local_addr: SocketAddr,
    interface: Option<String>,
    peer_reporter: PeerReporter,
) {
    let _sw = info_stopwatch!("TCP listener on {local_addr}");

    let result: io::Result<()> = async {
        let socket = net::bound_tcp_socket(local_addr, interface.as_deref())?;
        let listener = socket.listen(1024)?;
        log::info!("TCP listener started on {}", listener.local_addr()?);
        loop {
            let (stream, addr) = listener.accept().await?;
            peer_reporter.report_accepted(addr, stream).await;
        }
    }
    .await;

    if let Err(e) = result {
        log::error!("TCP listener on {local_addr} exited: {e}");
    }
}
