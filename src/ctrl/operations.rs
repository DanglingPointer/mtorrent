use crate::ctrl::peers::*;
use crate::data::{BlockAccountant, PieceInfo};
use crate::data::{PieceTracker, Storage};
use crate::pwp::*;
use crate::tracker::http::TrackerResponseContent;
use crate::tracker::udp::{AnnounceEvent, AnnounceRequest, AnnounceResponse, UdpTrackerConnection};
use crate::tracker::utils::get_udp_tracker_addrs;
use crate::utils::dispatch::Handler;
use crate::utils::meta::Metainfo;
use async_io::Async;
use futures::future::LocalBoxFuture;
use futures::FutureExt;
use log::{error, info, warn};
use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, SocketAddrV4, TcpStream, UdpSocket};
use std::rc::Rc;
use std::{io, mem};

pub struct OperationController {
    metainfo: Metainfo,
    internal_local_ip: SocketAddrV4,
    external_local_ip: SocketAddrV4,
    local_peer_id: [u8; 20],
    pieces: Rc<PieceInfo>,
    filekeeper: Rc<Storage>,
    local_records: BlockAccountant,
    piece_availability: PieceTracker,
    known_peers: HashSet<SocketAddr>,
    peermgr: PeerManager,
    stored_channels: HashMap<SocketAddr, (Option<DownloadTxChannel>, Option<UploadTxChannel>)>,
}

impl OperationController {
    pub fn new(
        metainfo: Metainfo,
        filekeeper: Storage,
        internal_local_ip: SocketAddrV4,
        external_local_ip: SocketAddrV4,
        local_peer_id: [u8; 20],
    ) -> Option<Self> {
        let pieces = Rc::new(PieceInfo::new(metainfo.pieces()?, metainfo.piece_length()?));
        let local_records = BlockAccountant::new(pieces.clone());
        let piece_tracker = PieceTracker::new(pieces.piece_count());
        let peermgr = PeerManager::new(pieces.clone());
        Some(Self {
            metainfo,
            internal_local_ip,
            external_local_ip,
            local_peer_id,
            pieces,
            filekeeper: Rc::new(filekeeper),
            local_records,
            piece_availability: piece_tracker,
            known_peers: HashSet::new(),
            peermgr,
            stored_channels: HashMap::new(),
        })
    }
}

pub enum OperationOutput {
    DownloadMsgSent(Result<DownloadTxChannel, SocketAddr>),
    DownloadMsgReceived(Result<(DownloadRxChannel, UploaderMessage), SocketAddr>),
    UploadMsgSent(Result<UploadTxChannel, SocketAddr>),
    UploadMsgReceived(Result<(UploadRxChannel, DownloaderMessage), SocketAddr>),
    PeerConnectivity(Result<Box<(DownloadChannels, UploadChannels, ConnectionRunner)>, SocketAddr>),
    PeerListen(Box<ListenMonitor>),
    UdpAnnounce(Box<io::Result<AnnounceResponse>>),
    HttpAnnounce(Box<io::Result<TrackerResponseContent>>),
    Void,
}

type Operation<'o> = LocalBoxFuture<'o, OperationOutput>;

impl<'h> Handler<'h> for OperationController {
    type OperationResult = OperationOutput;

    fn first_operations(&mut self) -> Vec<Operation<'h>> {
        [
            self.create_udp_announce_ops(AnnounceEvent::Started),
            self.create_listener_ops(),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    fn next_operations(
        &mut self,
        last_operation_result: OperationOutput,
    ) -> Option<Vec<Operation<'h>>> {
        match last_operation_result {
            OperationOutput::DownloadMsgSent(result) => self.process_download_msg_sent(result),
            OperationOutput::DownloadMsgReceived(result) => {
                self.process_download_msg_received(result)
            }
            OperationOutput::UploadMsgSent(result) => self.process_upload_msg_sent(result),
            OperationOutput::UploadMsgReceived(result) => self.process_upload_msg_received(result),
            OperationOutput::UdpAnnounce(response) => self.process_udp_announce_result(response),
            OperationOutput::PeerConnectivity(result) => self.process_connect_result(result),
            OperationOutput::PeerListen(monitor) => Some(self.process_listener_result(monitor)),
            OperationOutput::HttpAnnounce(_) => {
                todo!()
            }
            OperationOutput::Void => {
                // TODO: timer, periodic announce?
                None
            }
        }
    }
}

impl<'h> OperationController {
    fn create_udp_announce_ops(&mut self, event: AnnounceEvent) -> Vec<Operation<'h>> {
        fn create_op(
            local_addr: SocketAddrV4,
            tracker_addr: String,
            request: AnnounceRequest,
        ) -> Operation<'static> {
            let inner_fut = async move {
                let socket = Async::<UdpSocket>::bind(local_addr)?;
                info!("Connecting to tracker at {}", tracker_addr);
                socket.get_ref().connect(tracker_addr)?;
                let client = UdpTrackerConnection::from_connected_socket(socket).await?;
                client.do_announce_request(request).await
            };
            async move { OperationOutput::UdpAnnounce(Box::new(inner_fut.await)) }.boxed_local()
        }

        let downloaded = self.local_records.accounted_bytes();
        let left = self.local_records.missing_bytes();

        let announce_request = AnnounceRequest {
            info_hash: *self.metainfo.info_hash(),
            peer_id: self.local_peer_id,
            downloaded: downloaded as u64,
            left: left as u64,
            uploaded: 0, // TODO
            event,
            ip: None,
            key: 0,
            num_want: Some(50),
            port: self.external_local_ip.port(),
        };

        get_udp_tracker_addrs(&self.metainfo)
            .into_iter()
            .enumerate()
            .map(|(index, tracker_addr)| {
                let mut addr = self.internal_local_ip;
                addr.set_port(addr.port() + index as u16);
                create_op(addr, tracker_addr, announce_request.clone())
            })
            .collect()
    }

    #[allow(clippy::boxed_local)]
    fn process_udp_announce_result(
        &mut self,
        outcome: Box<io::Result<AnnounceResponse>>,
    ) -> Option<Vec<Operation<'h>>> {
        let response = outcome
            .map_err(|e| {
                info!("Announce error: {}", e);
                e
            })
            .ok()?;
        info!("Received announce response: {:?}", response);
        Some(
            response
                .ips
                .into_iter()
                .filter_map(|ip| {
                    self.known_peers.insert(ip).then_some(Self::outgoing_connect_fut(
                        self.local_peer_id,
                        *self.metainfo.info_hash(),
                        ip,
                    ))
                })
                .collect(),
        )
    }

    fn create_listener_ops(&mut self) -> Vec<Operation<'h>> {
        match listener_on_addr(self.internal_local_ip) {
            Ok((monitor, receiver)) => {
                let local_ip = self.internal_local_ip;
                let runner_fut = async move {
                    info!("TCP Listener started on {}", local_ip);
                    if let Err(e) = receiver.run().await {
                        warn!("TCP Listener exited: {}", e);
                    }
                    OperationOutput::Void
                };
                vec![
                    runner_fut.boxed_local(),
                    Self::listen_monitor_fut(Box::new(monitor)),
                ]
            }
            Err(e) => {
                error!("Failed to create TCP listener: {}", e);
                Vec::new()
            }
        }
    }

    fn process_listener_result(&mut self, mut monitor: Box<ListenMonitor>) -> Vec<Operation<'h>> {
        if let Some(stream) = monitor.take_pending_stream() {
            let mut ops = vec![Self::listen_monitor_fut(monitor)];
            if let Ok(remote_ip) = stream.get_ref().peer_addr() {
                self.known_peers.insert(remote_ip);
                ops.push(Self::incoming_connect_fut(
                    self.local_peer_id,
                    *self.metainfo.info_hash(),
                    stream,
                    remote_ip,
                ));
            }
            ops
        } else {
            // error
            self.create_listener_ops()
        }
    }

    fn process_connect_result(
        &mut self,
        outcome: Result<Box<(DownloadChannels, UploadChannels, ConnectionRunner)>, SocketAddr>,
    ) -> Option<Vec<Operation<'h>>> {
        let (download_chs, upload_chs, runner) = *outcome.ok()?;
        let DownloadChannels(download_tx, download_rx) = download_chs;
        let UploadChannels(upload_tx, upload_rx) = upload_chs;

        let remote_ip = *upload_rx.remote_ip();

        self.peermgr.add_peer(&remote_ip);
        self.stored_channels.insert(remote_ip, (None, None));

        if self.local_records.accounted_bytes() > 0 {
            let bitfield = self.local_records.generate_bitfield();
            self.peermgr
                .upload_monitor(&remote_ip)
                .unwrap()
                .submit_outbound(UploaderMessage::Bitfield(bitfield));
        }

        // TODO: run engine

        let mut ops = vec![
            Self::connection_runner_fut(runner),
            Self::download_rx_channel_fut(download_rx),
            Self::upload_rx_channel_fut(upload_rx),
        ];
        if let Some(mut tx_ops) = self.process_download_msg_sent(Ok(download_tx)) {
            ops.append(&mut tx_ops);
        }
        if let Some(mut tx_ops) = self.process_upload_msg_sent(Ok(upload_tx)) {
            ops.append(&mut tx_ops);
        }
        Some(ops)
    }

    fn process_download_msg_sent(
        &mut self,
        outcome: Result<DownloadTxChannel, SocketAddr>,
    ) -> Option<Vec<Operation<'h>>> {
        let tx_channel = outcome
            .map_err(|remote_ip| {
                error!("DownloadTxChannel error, disconnected {remote_ip}");
                self.erase_peer(&remote_ip);
            })
            .ok()?;

        let download_handler = self.peermgr.download_handler(tx_channel.remote_ip())?;
        if let Some(msg) = download_handler.next_outbound() {
            Some(vec![Self::download_tx_channel_fut(tx_channel, msg)])
        } else {
            let (download_channel_slot, _) =
                self.stored_channels.get_mut(tx_channel.remote_ip()).unwrap();
            download_channel_slot.replace(tx_channel);
            None
        }
    }
    fn process_download_msg_received(
        &mut self,
        outcome: Result<(DownloadRxChannel, UploaderMessage), SocketAddr>,
    ) -> Option<Vec<Operation<'h>>> {
        let (rx_channel, msg) = outcome
            .map_err(|remote_ip| {
                error!("DownloadRxChannel error, disconnected {remote_ip}");
                self.erase_peer(&remote_ip);
            })
            .ok()?;

        let remote_ip = *rx_channel.remote_ip();
        self.peermgr.download_handler(&remote_ip)?.update_state(&msg);

        match msg {
            UploaderMessage::Block(info, data) => {
                if let Ok(global_offset) = self.local_records.submit_block(&info) {
                    // TODO: check hash
                    self.filekeeper
                        .write_block(global_offset, data)
                        .expect("Failed to write to file");

                    if self.local_records.has_piece(info.piece_index) {
                        for upload_monitor in self.peermgr.all_upload_monitors() {
                            upload_monitor.submit_outbound(UploaderMessage::Have {
                                piece_index: info.piece_index,
                            });
                        }
                    }
                } else {
                    error!("Received block ignored: {info}");
                }
            }
            UploaderMessage::Bitfield(bitfield) => {
                self.piece_availability.add_bitfield_record(remote_ip, &bitfield);
            }
            UploaderMessage::Have { piece_index } => {
                self.piece_availability.add_single_record(remote_ip, piece_index);
            }
            _ => (),
        }

        // TODO: run engine

        let mut ops = vec![Self::download_rx_channel_fut(rx_channel)];
        let (download_channel_slot, _) = self.stored_channels.get_mut(&remote_ip).unwrap();
        if let Some(tx_channel) = download_channel_slot.take() {
            if let Some(msg) = self.peermgr.download_handler(&remote_ip).unwrap().next_outbound() {
                ops.push(Self::download_tx_channel_fut(tx_channel, msg));
            } else {
                download_channel_slot.replace(tx_channel);
            }
        }
        Some(ops)
    }

    fn process_upload_msg_sent(
        &mut self,
        outcome: Result<UploadTxChannel, SocketAddr>,
    ) -> Option<Vec<Operation<'h>>> {
        let tx_channel = outcome
            .map_err(|remote_ip| {
                error!("UploadTxChannel error, disconnected {remote_ip}");
                self.erase_peer(&remote_ip);
            })
            .ok()?;

        let upload_handler = self.peermgr.upload_handler(tx_channel.remote_ip())?;
        if let Some(msg) = upload_handler.next_outbound() {
            Some(vec![Self::upload_tx_channel_fut(
                tx_channel,
                msg,
                self.pieces.clone(),
                self.filekeeper.clone(),
            )])
        } else {
            let (_, upload_channel_slot) =
                self.stored_channels.get_mut(tx_channel.remote_ip()).unwrap();
            upload_channel_slot.replace(tx_channel);
            None
        }
    }

    fn process_upload_msg_received(
        &mut self,
        outcome: Result<(UploadRxChannel, DownloaderMessage), SocketAddr>,
    ) -> Option<Vec<Operation<'h>>> {
        let (rx_channel, msg) = outcome
            .map_err(|remote_ip| {
                error!("UploadRxChannel error, disconnected {remote_ip}");
                self.erase_peer(&remote_ip);
            })
            .ok()?;

        let remote_ip = *rx_channel.remote_ip();
        self.peermgr.upload_handler(&remote_ip)?.update_state(&msg);

        match msg {
            DownloaderMessage::Request(block) => {
                info!("{} Received request: {}", &remote_ip, block);
                // TODO
            }
            DownloaderMessage::Cancel(block) => {
                info!("{} Received cancel: {}", &remote_ip, block);
                // TODO
            }
            _ => (),
        }

        // TODO: run engine

        let mut ops = vec![Self::upload_rx_channel_fut(rx_channel)];
        let (_, upload_channel_slot) = self.stored_channels.get_mut(&remote_ip).unwrap();
        if let Some(tx_channel) = upload_channel_slot.take() {
            if let Some(msg) = self.peermgr.upload_handler(&remote_ip).unwrap().next_outbound() {
                ops.push(Self::upload_tx_channel_fut(
                    tx_channel,
                    msg,
                    self.pieces.clone(),
                    self.filekeeper.clone(),
                ));
            } else {
                upload_channel_slot.replace(tx_channel);
            }
        }
        Some(ops)
    }
}

impl OperationController {
    fn erase_peer(&mut self, remote_ip: &SocketAddr) {
        self.peermgr.remove_peer(remote_ip);
        self.stored_channels.remove(remote_ip);
        self.piece_availability.forget_peer(*remote_ip);
    }

    fn listen_monitor_fut(mut monitor: Box<ListenMonitor>) -> Operation<'static> {
        async move {
            monitor.handle_incoming().await;
            OperationOutput::PeerListen(monitor)
        }
        .boxed_local()
    }

    fn incoming_connect_fut(
        local_peer_id: [u8; 20],
        info_hash: [u8; 20],
        stream: Async<TcpStream>,
        remote_ip: SocketAddr,
    ) -> Operation<'static> {
        async move {
            match channels_from_incoming(&local_peer_id, Some(&info_hash), stream).await {
                Ok(channels) => {
                    info!("Successfully established an incoming connection to {remote_ip}");
                    OperationOutput::PeerConnectivity(Ok(Box::new(channels)))
                }
                Err(e) => {
                    error!("Failed to establish an incoming connection to {remote_ip}: {e}");
                    OperationOutput::PeerConnectivity(Err(remote_ip))
                }
            }
        }
        .boxed_local()
    }

    fn outgoing_connect_fut(
        local_peer_id: [u8; 20],
        info_hash: [u8; 20],
        remote_ip: SocketAddr,
    ) -> Operation<'static> {
        async move {
            info!("Trying to establish an outgoing connection to {remote_ip}");
            match channels_from_outgoing(&local_peer_id, &info_hash, remote_ip, None).await {
                Ok(channels) => {
                    info!("Successfully established an outgoing connection to {remote_ip}");
                    OperationOutput::PeerConnectivity(Ok(Box::new(channels)))
                }
                Err(e) => {
                    error!("Failed to establish an outgoing connection to {remote_ip}: {e}");
                    OperationOutput::PeerConnectivity(Err(remote_ip))
                }
            }
        }
        .boxed_local()
    }

    fn connection_runner_fut(runner: ConnectionRunner) -> Operation<'static> {
        async move {
            if let Err(e) = runner.run().await {
                warn!("Peer runner exited: {}", e);
            }
            OperationOutput::Void
        }
        .boxed_local()
    }

    fn download_rx_channel_fut(mut channel: DownloadRxChannel) -> Operation<'static> {
        async move {
            let remote_ip = *channel.remote_ip();
            match channel.receive_message().await {
                Err(_) => OperationOutput::DownloadMsgReceived(Err(remote_ip)),
                Ok(msg) => OperationOutput::DownloadMsgReceived(Ok((channel, msg))),
            }
        }
        .boxed_local()
    }

    fn download_tx_channel_fut(
        mut channel: DownloadTxChannel,
        msg: DownloaderMessage,
    ) -> Operation<'static> {
        async move {
            let remote_ip = *channel.remote_ip();
            match channel.send_message(msg).await {
                Err(_) => OperationOutput::DownloadMsgSent(Err(remote_ip)),
                Ok(()) => OperationOutput::DownloadMsgSent(Ok(channel)),
            }
        }
        .boxed_local()
    }

    fn upload_rx_channel_fut(mut channel: UploadRxChannel) -> Operation<'static> {
        async move {
            let remote_ip = *channel.remote_ip();
            match channel.receive_message().await {
                Err(_) => OperationOutput::UploadMsgReceived(Err(remote_ip)),
                Ok(msg) => OperationOutput::UploadMsgReceived(Ok((channel, msg))),
            }
        }
        .boxed_local()
    }

    fn upload_tx_channel_fut(
        mut channel: UploadTxChannel,
        mut msg: UploaderMessage,
        pieces: Rc<PieceInfo>,
        files: Rc<Storage>,
    ) -> Operation<'static> {
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
        async move {
            fill_block_with_data(&mut msg, &pieces, &files);
            let remote_ip = *channel.remote_ip();
            match channel.send_message(msg).await {
                Err(_) => OperationOutput::UploadMsgSent(Err(remote_ip)),
                Ok(()) => OperationOutput::UploadMsgSent(Ok(channel)),
            }
        }
        .boxed_local()
    }
}
