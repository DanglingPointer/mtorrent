use crate::data::{PieceInfo, StorageProxy};
use crate::pwp::*;
use crate::tracker::{http, udp};
use crate::utils::meta::Metainfo;
use futures::future::LocalBoxFuture;
use log::{debug, error, info, warn};
use std::net::{SocketAddr, SocketAddrV4};
use std::rc::Rc;
use std::time::Duration;
use std::{io, mem};
use tokio::net::{TcpStream, UdpSocket};
use tokio::time::sleep;

pub enum TimerType {
    HttpReannounce {
        tracker_url: String,
    },
    UdpReannounce {
        tracker_addr: String,
    },
    DebugShutdown,
}

pub enum Action {
    DownloadMsgSent(Result<DownloadTxChannel, SocketAddr>),
    DownloadMsgReceived(Result<Box<(DownloadRxChannel, UploaderMessage)>, SocketAddr>),
    UploadMsgSent(Result<UploadTxChannel, SocketAddr>),
    UploadMsgReceived(Result<Box<(UploadRxChannel, DownloaderMessage)>, SocketAddr>),
    PieceVerification(Result<usize, usize>),
    PeerConnectivity(Result<Box<(DownloadChannels, UploadChannels, ConnectionRunner)>, SocketAddr>),
    PeerListen(Box<ListenMonitor>),
    UdpAnnounce(Box<io::Result<(udp::AnnounceResponse, String)>>),
    HttpAnnounce(Box<Result<(http::AnnounceResponseContent, String), http::Error>>),
    Timeout(TimerType),
    Void,
}

pub type Operation<'o> = LocalBoxFuture<'o, Action>;

impl Action {
    pub async fn new_timer(delay: Duration, timer: TimerType) -> Self {
        sleep(delay).await;
        Action::Timeout(timer)
    }

    pub async fn from_listen_monitor(mut monitor: Box<ListenMonitor>) -> Self {
        monitor.handle_incoming().await;
        Action::PeerListen(monitor)
    }

    pub async fn new_incoming_connect(
        local_peer_id: [u8; 20],
        info_hash: [u8; 20],
        stream: TcpStream,
        remote_ip: SocketAddr,
    ) -> Self {
        match channels_from_incoming(&local_peer_id, Some(&info_hash), stream).await {
            Ok(channels) => {
                info!("Successfully established an incoming connection to {remote_ip}");
                Action::PeerConnectivity(Ok(Box::new(channels)))
            }
            Err(e) => {
                error!("Failed to establish an incoming connection to {remote_ip}: {e}");
                Action::PeerConnectivity(Err(remote_ip))
            }
        }
    }

    pub async fn new_outgoing_connect(
        local_peer_id: [u8; 20],
        info_hash: [u8; 20],
        remote_ip: SocketAddr,
    ) -> Self {
        let mut attempts_left = 3;
        let mut reconnect_interval = Duration::from_secs(2);
        loop {
            debug!("Connecting to {remote_ip}, attempts left: {attempts_left}");
            match channels_from_outgoing(&local_peer_id, &info_hash, remote_ip, None).await {
                Ok(channels) => {
                    info!("Successfully established an outgoing connection to {remote_ip} (attempts_left={attempts_left})");
                    return Action::PeerConnectivity(Ok(Box::new(channels)));
                }
                Err(e) => match e.kind() {
                    io::ErrorKind::ConnectionRefused | io::ErrorKind::ConnectionReset
                        if attempts_left > 0 =>
                    {
                        sleep(reconnect_interval).await;
                        attempts_left -= 1;
                        reconnect_interval *= 2;
                    }
                    _ => {
                        error!("Failed to establish an outgoing connection to {remote_ip}: {e}");
                        return Action::PeerConnectivity(Err(remote_ip));
                    }
                },
            }
        }
    }

    pub async fn from_connection_runner(runner: ConnectionRunner) -> Self {
        if let Err(e) = runner.run().await {
            warn!("Peer runner exited: {}", e);
        }
        Action::Void
    }

    pub async fn from_listener_runner(receiver: ListenerRunner, local_ip: SocketAddrV4) -> Self {
        info!("TCP Listener started on {}", local_ip);
        if let Err(e) = receiver.run().await {
            warn!("TCP Listener exited: {}", e);
        }
        Action::Void
    }

    pub async fn from_download_rx_channel(mut channel: DownloadRxChannel) -> Self {
        let remote_ip = *channel.remote_ip();
        match channel.receive_message().await {
            Err(_) => Action::DownloadMsgReceived(Err(remote_ip)),
            Ok(msg) => Action::DownloadMsgReceived(Ok(Box::new((channel, msg)))),
        }
    }

    pub async fn from_download_tx_channel(
        mut channel: DownloadTxChannel,
        msg: DownloaderMessage,
    ) -> Self {
        let remote_ip = *channel.remote_ip();
        match channel.send_message(msg).await {
            Err(_) => Action::DownloadMsgSent(Err(remote_ip)),
            Ok(()) => Action::DownloadMsgSent(Ok(channel)),
        }
    }

    pub async fn from_upload_rx_channel(mut channel: UploadRxChannel) -> Self {
        let remote_ip = *channel.remote_ip();
        match channel.receive_message().await {
            Err(_) => Action::UploadMsgReceived(Err(remote_ip)),
            Ok(msg) => Action::UploadMsgReceived(Ok(Box::new((channel, msg)))),
        }
    }

    pub async fn from_upload_tx_channel(
        mut channel: UploadTxChannel,
        mut msg: UploaderMessage,
        pieces: Rc<PieceInfo>,
        files: Rc<StorageProxy>,
    ) -> Self {
        if let UploaderMessage::Block(info, data) = &mut msg {
            let global_offset = pieces
                .global_offset(info.piece_index, info.in_piece_offset, info.block_length)
                .expect("Logical error, this should never happen");
            debug_assert!(data.is_empty());

            let _ = mem::replace(
                data,
                files
                    .read_block(global_offset, info.block_length)
                    .await
                    .unwrap_or_else(|_| panic!("Failed to read from file: {}", info)),
            );
        }
        let remote_ip = *channel.remote_ip();
        match channel.send_message(msg).await {
            Err(_) => Action::UploadMsgSent(Err(remote_ip)),
            Ok(()) => Action::UploadMsgSent(Ok(channel)),
        }
    }

    pub async fn new_udp_announce(
        local_addr: SocketAddrV4,
        tracker_addr: String,
        request: udp::AnnounceRequest,
    ) -> Self {
        let inner_fut = async move {
            let socket = UdpSocket::bind(local_addr).await?;
            socket.connect(&tracker_addr).await?;
            let client = udp::UdpTrackerConnection::from_connected_socket(socket).await?;
            info!("Connected to tracker at {}", tracker_addr);
            Ok((client.do_announce_request(request).await?, tracker_addr))
        };
        Action::UdpAnnounce(Box::new(inner_fut.await))
    }

    pub async fn new_http_announce(
        request: http::TrackerRequestBuilder,
        tracker_url: String,
    ) -> Self {
        let inner_fut = async move { Ok((http::do_announce_request(request).await?, tracker_url)) };
        Action::HttpAnnounce(Box::new(inner_fut.await))
    }

    pub async fn from_piece_verification(
        piece_index: usize,
        pieces: Rc<PieceInfo>,
        files: Rc<StorageProxy>,
        metainfo: Rc<Metainfo>,
    ) -> Self {
        let inner_fut = async {
            let piece_length = pieces.piece_len();
            let global_offset = pieces.global_offset(piece_index, 0, piece_length).ok()?;
            let expected_sha1 = {
                let mut buf = [0u8; 20];
                buf.copy_from_slice(metainfo.pieces()?.nth(piece_index)?);
                buf
            };
            let result =
                files.verify_block(global_offset, piece_length, &expected_sha1).await.ok()?;
            Some(result)
        };
        match inner_fut.await {
            Some(true) => {
                info!("Piece verified successfully, piece_index={piece_index}");
                Action::PieceVerification(Ok(piece_index))
            }
            Some(false) => {
                error!("Piece verification failed, piece_index={piece_index}");
                Action::PieceVerification(Err(piece_index))
            }
            None => {
                error!("Piece verification could not be done, piece_index={piece_index}");
                debug_assert!(
                    false,
                    "Piece verification could not be done, piece_index={piece_index}"
                );
                Action::PieceVerification(Err(piece_index))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sizeof_action() {
        let size = std::mem::size_of::<Action>();
        assert!(size <= 64, "{size} > 64");
    }
}
