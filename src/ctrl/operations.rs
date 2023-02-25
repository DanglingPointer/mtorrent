use crate::data::PieceInfo;
use crate::data::Storage;
use crate::pwp::*;
use crate::tracker::http::TrackerResponseContent;
use crate::tracker::udp::AnnounceRequest;
use crate::tracker::udp::AnnounceResponse;
use crate::tracker::udp::UdpTrackerConnection;
use async_io::{Async, Timer};
use futures::future::LocalBoxFuture;
use log::{debug, error, info, warn};
use std::net::{SocketAddr, SocketAddrV4, TcpStream, UdpSocket};
use std::rc::Rc;
use std::time::Duration;
use std::{io, mem};

pub enum TimerType {
    Reannounce,
    DebugShutdown,
}

pub enum Outcome {
    DownloadMsgSent(Result<DownloadTxChannel, SocketAddr>),
    DownloadMsgReceived(Result<(DownloadRxChannel, UploaderMessage), SocketAddr>),
    UploadMsgSent(Result<UploadTxChannel, SocketAddr>),
    UploadMsgReceived(Result<(UploadRxChannel, DownloaderMessage), SocketAddr>),
    PeerConnectivity(Result<Box<(DownloadChannels, UploadChannels, ConnectionRunner)>, SocketAddr>),
    PeerListen(Box<ListenMonitor>),
    UdpAnnounce(Box<io::Result<AnnounceResponse>>),
    HttpAnnounce(Box<io::Result<TrackerResponseContent>>),
    Timeout(TimerType),
    Void,
}

pub type Operation<'o> = LocalBoxFuture<'o, Outcome>;

impl Outcome {
    pub async fn new_timer(delay: Duration, timer: TimerType) -> Self {
        Timer::after(delay).await;
        Outcome::Timeout(timer)
    }

    pub async fn from_listen_monitor(mut monitor: Box<ListenMonitor>) -> Self {
        monitor.handle_incoming().await;
        Outcome::PeerListen(monitor)
    }

    pub async fn new_incoming_connect(
        local_peer_id: [u8; 20],
        info_hash: [u8; 20],
        stream: Async<TcpStream>,
        remote_ip: SocketAddr,
    ) -> Self {
        match channels_from_incoming(&local_peer_id, Some(&info_hash), stream).await {
            Ok(channels) => {
                info!("Successfully established an incoming connection to {remote_ip}");
                Outcome::PeerConnectivity(Ok(Box::new(channels)))
            }
            Err(e) => {
                error!("Failed to establish an incoming connection to {remote_ip}: {e}");
                Outcome::PeerConnectivity(Err(remote_ip))
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
                    return Outcome::PeerConnectivity(Ok(Box::new(channels)));
                }
                Err(e) => match e.kind() {
                    io::ErrorKind::ConnectionRefused | io::ErrorKind::ConnectionReset
                        if attempts_left > 0 =>
                    {
                        Timer::after(reconnect_interval).await;
                        attempts_left -= 1;
                        reconnect_interval *= 2;
                    }
                    _ => {
                        error!("Failed to establish an outgoing connection to {remote_ip}: {e}");
                        return Outcome::PeerConnectivity(Err(remote_ip));
                    }
                },
            }
        }
    }

    pub async fn from_connection_runner(runner: ConnectionRunner) -> Self {
        if let Err(e) = runner.run().await {
            warn!("Peer runner exited: {}", e);
        }
        Outcome::Void
    }

    pub async fn from_listener_runner(receiver: ListenerRunner, local_ip: SocketAddrV4) -> Self {
        info!("TCP Listener started on {}", local_ip);
        if let Err(e) = receiver.run().await {
            warn!("TCP Listener exited: {}", e);
        }
        Outcome::Void
    }

    pub async fn from_download_rx_channel(mut channel: DownloadRxChannel) -> Self {
        let remote_ip = *channel.remote_ip();
        match channel.receive_message().await {
            Err(_) => Outcome::DownloadMsgReceived(Err(remote_ip)),
            Ok(msg) => Outcome::DownloadMsgReceived(Ok((channel, msg))),
        }
    }

    pub async fn from_download_tx_channel(
        mut channel: DownloadTxChannel,
        msg: DownloaderMessage,
    ) -> Self {
        let remote_ip = *channel.remote_ip();
        match channel.send_message(msg).await {
            Err(_) => Outcome::DownloadMsgSent(Err(remote_ip)),
            Ok(()) => Outcome::DownloadMsgSent(Ok(channel)),
        }
    }

    pub async fn from_upload_rx_channel(mut channel: UploadRxChannel) -> Self {
        let remote_ip = *channel.remote_ip();
        match channel.receive_message().await {
            Err(_) => Outcome::UploadMsgReceived(Err(remote_ip)),
            Ok(msg) => Outcome::UploadMsgReceived(Ok((channel, msg))),
        }
    }

    pub async fn from_upload_tx_channel(
        mut channel: UploadTxChannel,
        mut msg: UploaderMessage,
        pieces: Rc<PieceInfo>,
        files: Rc<Storage>,
    ) -> Self {
        fn fill_block_with_data(msg: &mut UploaderMessage, pieces: &PieceInfo, files: &Storage) {
            if let UploaderMessage::Block(info, data) = msg {
                let global_offset = pieces
                    .global_offset(info.piece_index, info.in_piece_offset, info.block_length)
                    .expect("Logical error, this should never happen");
                debug_assert!(data.is_empty());
                let _ = mem::replace(
                    data,
                    files
                        .read_block(global_offset, info.block_length)
                        .expect("Failed to read from file"),
                );
            }
        }
        fill_block_with_data(&mut msg, &pieces, &files);
        let remote_ip = *channel.remote_ip();
        match channel.send_message(msg).await {
            Err(_) => Outcome::UploadMsgSent(Err(remote_ip)),
            Ok(()) => Outcome::UploadMsgSent(Ok(channel)),
        }
    }

    pub async fn new_udp_announce(
        local_addr: SocketAddrV4,
        tracker_addr: String,
        request: AnnounceRequest,
    ) -> Self {
        let inner_fut = async move {
            let socket = Async::<UdpSocket>::bind(local_addr)?;
            socket.get_ref().connect(&tracker_addr)?;
            let client = UdpTrackerConnection::from_connected_socket(socket).await?;
            info!("Connected to tracker at {}", tracker_addr);
            client.do_announce_request(request).await
        };
        Outcome::UdpAnnounce(Box::new(inner_fut.await))
    }
}
