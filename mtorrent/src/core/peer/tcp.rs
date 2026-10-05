use super::super::PeerReporter;
use super::ctx;
use bytes::BytesMut;
use mtorrent_base::{pe, pwp};
use mtorrent_utils::peer_id::PeerId;
use mtorrent_utils::task_scope::TaskScope;
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

    let mut scope = TaskScope::new();
    scope
        .spawn_on(
            async move {
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
            },
            pwp_runtime,
        )
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

    let mut scope = TaskScope::new();
    scope
        .spawn_on(
            async move {
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
            },
            pwp_runtime,
        )
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
            net::set_tcp_options(&stream)?;
            peer_reporter.report_accepted_tcp(addr, stream).await;
        }
    }
    .await;

    if let Err(e) = result {
        log::error!("TCP listener on {local_addr} exited: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ctx::ConstData;
    use local_async_utils::prelude::*;
    use std::net::Ipv4Addr;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    use tokio::task;
    use tokio::time::timeout;

    async fn drain_socket(socket: &mut TcpStream) -> io::Result<()> {
        timeout(sec!(1), socket.readable()).await??;
        let mut buf = Vec::new();
        loop {
            match socket.try_read_buf(&mut buf) {
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(_) => (),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }

    #[tokio::test(flavor = "local")]
    async fn test_abort_outbound_connection_during_handshake() {
        let peer_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let peer_addr = peer_listener.local_addr().unwrap();

        let connect_task_handle = task::spawn_local(async move {
            new_outbound_connection(
                &ConstData::new_stub(),
                &[0u8; 20],
                false,
                false,
                peer_addr,
                &runtime::Handle::current(),
            )
            .await
        });

        let (mut peer_socket, _addr) =
            timeout(sec!(1), peer_listener.accept()).await.unwrap().unwrap();
        drain_socket(&mut peer_socket).await.unwrap();

        connect_task_handle.abort();
        let read_result = drain_socket(&mut peer_socket).await;
        let read_error = read_result.unwrap_err();
        // RST rather than EOF because net::bound_tcp_socket() sets SO_LINGER to 0
        assert_eq!(read_error.kind(), io::ErrorKind::ConnectionReset);
    }

    #[tokio::test(flavor = "local")]
    async fn test_abort_inbound_connection_during_handshake() {
        let listener = net::bound_tcp_socket((Ipv4Addr::LOCALHOST, 0).into(), None)
            .unwrap()
            .listen(1024)
            .unwrap();
        let our_addr = listener.local_addr().unwrap();

        let connect_task_handle = task::spawn_local(async move {
            let (socket, peer_addr) = listener.accept().await.unwrap();
            net::set_tcp_options(&socket).unwrap();
            new_inbound_connection(
                &PeerId::generate_new(),
                &[0u8; 20],
                false,
                peer_addr,
                socket,
                &runtime::Handle::current(),
            )
            .await
        });

        let mut peer_socket =
            timeout(sec!(1), TcpStream::connect(our_addr)).await.unwrap().unwrap();
        let mut handshake_to_send = Vec::new();
        handshake_to_send.extend_from_slice(b"\x13BitTorrent protocol");
        handshake_to_send.extend_from_slice(&[0u8; 8]); // reserved
        handshake_to_send.extend_from_slice(&[0u8; 20]); // info_hash
        // omit peer_id so that the handshake stalls
        timeout(sec!(1), peer_socket.write_all(&handshake_to_send))
            .await
            .unwrap()
            .unwrap();
        drain_socket(&mut peer_socket).await.unwrap();

        connect_task_handle.abort();
        let read_result = drain_socket(&mut peer_socket).await;
        let read_error = read_result.unwrap_err();
        // RST rather than EOF because net::set_tcp_options() sets SO_LINGER to 0
        assert_eq!(read_error.kind(), io::ErrorKind::ConnectionReset);
    }

    #[tokio::test(flavor = "local")]
    async fn test_listener_sets_tcp_options_on_accepted_streams() {
        // Note: on Linux accepted sockets inherit SO_LINGER and TCP_NODELAY from the listening
        // socket so this test only makes sense on other platforms

        // reserve a free port; SO_REUSEADDR/SO_REUSEPORT allow the listener to bind it too
        let listener_addr = net::bound_tcp_socket((Ipv4Addr::LOCALHOST, 0).into(), None)
            .unwrap()
            .local_addr()
            .unwrap();

        let (reporter, mut accepted_peers) = PeerReporter::new_mock();
        let listener_handle = task::spawn_local(run_pwp_listener(listener_addr, None, reporter));
        task::yield_now().await;

        let mut client_socket =
            timeout(sec!(1), TcpStream::connect(listener_addr)).await.unwrap().unwrap();

        let accepted = timeout(sec!(1), accepted_peers.recv_tcp()).await.unwrap();
        let (_addr, accepted_stream) = accepted.expect("listener should have accepted a stream");
        listener_handle.abort();

        assert!(accepted_stream.nodelay().unwrap());
        assert_eq!(accepted_stream.linger().unwrap(), Some(sec!(0)));

        // RST rather than EOF on close proves SO_LINGER=0 was set on the accepted stream
        drop(accepted_stream);
        let read_error = drain_socket(&mut client_socket).await.unwrap_err();
        assert_eq!(read_error.kind(), io::ErrorKind::ConnectionReset);
    }
}
