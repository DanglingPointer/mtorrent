use crate::ctrl::channel_monitors::{DownloadChannelMonitor, UploadChannelMonitor};
use crate::ctrl::peer_status::PeerStatus;
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
use std::io;
use std::net::{SocketAddr, SocketAddrV4, TcpStream, UdpSocket};
use std::rc::Rc;
use std::time::Duration;

pub struct OperationController {
    metainfo: Metainfo,
    internal_local_ip: SocketAddrV4,
    external_local_ip: SocketAddrV4,
    local_peer_id: [u8; 20],
    pieces: Rc<PieceInfo>,
    filekeeper: Storage,
    local_records: BlockAccountant,
    piece_availability: PieceTracker,
    known_peers: HashSet<SocketAddr>,
    peer_statuses: HashMap<SocketAddr, PeerStatus>,
}

impl OperationController {
    pub fn new(
        metainfo: Metainfo,
        filekeeper: Storage,
        internal_local_ip: SocketAddrV4,
        external_local_ip: SocketAddrV4,
        local_peer_id: [u8; 20],
    ) -> Option<Self> {
        let piecekeeper = Rc::new(PieceInfo::new(metainfo.pieces()?, metainfo.piece_length()?));
        let local_records = BlockAccountant::new(piecekeeper.clone());
        let piece_tracker = PieceTracker::new(piecekeeper.piece_count());
        Some(Self {
            metainfo,
            internal_local_ip,
            external_local_ip,
            local_peer_id,
            pieces: piecekeeper,
            filekeeper,
            local_records,
            piece_availability: piece_tracker,
            known_peers: HashSet::new(),
            peer_statuses: HashMap::new(),
        })
    }
}

pub enum OperationOutput {
    DownloadFromPeer(Result<Box<DownloadChannelMonitor>, SocketAddr>),
    UploadToPeer(Result<Box<UploadChannelMonitor>, SocketAddr>),
    PeerConnectivity(Result<Box<(DownloadChannel, UploadChannel, ConnectionRunner)>, SocketAddr>),
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
            OperationOutput::UdpAnnounce(response) => self.process_udp_announce_result(response),
            OperationOutput::DownloadFromPeer(monitor) => self.process_download_monitor(monitor),
            OperationOutput::UploadToPeer(monitor) => self.process_upload_monitor(monitor),
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
                    self.known_peers.insert(ip).then_some(outgoing_connect_fut(
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
                    listen_monitor_fut(Box::new(monitor)),
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
            let mut ops = vec![listen_monitor_fut(monitor)];
            if let Ok(remote_ip) = stream.get_ref().peer_addr() {
                self.known_peers.insert(remote_ip);
                ops.push(incoming_connect_fut(
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
        outcome: Result<Box<(DownloadChannel, UploadChannel, ConnectionRunner)>, SocketAddr>,
    ) -> Option<Vec<Operation<'h>>> {
        let (download_ch, upload_ch, runner) = *outcome.ok()?;
        let remote_ip = *download_ch.remote_ip();

        let download_mon = Box::new(DownloadChannelMonitor::new(download_ch));
        let upload_mon = Box::new(UploadChannelMonitor::new(upload_ch));

        let mut peer_status = PeerStatus::new(self.pieces.clone());
        if self.local_records.accounted_bytes() > 0 {
            let bitfield = self.local_records.generate_bitfield();
            peer_status.enqueue_uploader_msg(UploaderMessage::Bitfield(bitfield));
        }
        self.peer_statuses.insert(remote_ip, peer_status);

        let connection_ops = vec![connection_runner_fut(runner)];
        let download_ops = self.process_download_monitor(Ok(download_mon)).unwrap_or_default();
        let upload_ops = self.process_upload_monitor(Ok(upload_mon)).unwrap_or_default();
        Some([connection_ops, download_ops, upload_ops].into_iter().flatten().collect())
    }

    fn process_download_monitor(
        &mut self,
        outcome: Result<Box<DownloadChannelMonitor>, SocketAddr>,
    ) -> Option<Vec<Operation<'h>>> {
        let monitor = outcome
            .map_err(|remote_ip| {
                error!("DownloadMonitor error, disconnected {}", &remote_ip);
                self.known_peers.remove(&remote_ip);
                self.peer_statuses.remove(&remote_ip);
                remote_ip
            })
            .ok()?;

        let peer_status = self.peer_statuses.get_mut(monitor.remote_ip())?;
        let state_update = monitor.last_update();

        // update peer status
        peer_status.update_download_state(
            monitor.am_interested(),
            monitor.peer_choking(),
            &state_update,
        );

        // write and announce received blocks
        for (info, data) in state_update.received_blocks {
            if let Ok(global_offset) = self.local_records.submit_block(&info) {
                self.filekeeper
                    .write_block(global_offset, data)
                    .expect("Failed to write to file");

                if self.local_records.has_piece(info.piece_index) {
                    for peer_status in self.peer_statuses.values_mut() {
                        peer_status.enqueue_uploader_msg(UploaderMessage::Have {
                            piece_index: info.piece_index,
                        });
                    }
                }
            } else {
                error!("Received block ignored: {info}");
            }
        }

        // update piece tracker
        for bitfield in state_update.received_bitfields {
            self.piece_availability.add_bitfield_record(*monitor.remote_ip(), &bitfield);
        }
        for piece_index in state_update.received_haves {
            self.piece_availability.add_single_record(*monitor.remote_ip(), piece_index);
        }

        // TODO: run engine

        let peer_status = self.peer_statuses.get_mut(monitor.remote_ip())?;
        let pending_msgs = peer_status.take_pending_downloader_msgs();
        Some(vec![download_monitor_fut(monitor, pending_msgs)])
    }

    fn process_upload_monitor(
        &mut self,
        outcome: Result<Box<UploadChannelMonitor>, SocketAddr>,
    ) -> Option<Vec<Operation<'h>>> {
        let monitor = outcome
            .map_err(|remote_ip| {
                error!("UploadMonitor error, disconnected {}", &remote_ip);
                self.known_peers.remove(&remote_ip);
                self.peer_statuses.remove(&remote_ip);
                remote_ip
            })
            .ok()?;

        let peer_status = self.peer_statuses.get_mut(monitor.remote_ip())?;
        let state_update = monitor.last_update();

        // update peer status
        peer_status.update_upload_state(
            monitor.peer_interested(),
            monitor.am_choking(),
            &state_update,
        );

        // temp: print received requests
        let received_requests = state_update
            .received_requests
            .iter()
            .map(|block_info| format!("[{}]", block_info))
            .collect::<Vec<String>>()
            .join(", ");
        if !received_requests.is_empty() {
            info!("{} Received requests: {}", &monitor.remote_ip(), received_requests);
        }

        // temp: print received cancellations
        let received_cancels = state_update
            .received_cancels
            .iter()
            .map(|block_info| format!("[{}]", block_info))
            .collect::<Vec<String>>()
            .join(", ");
        if !received_cancels.is_empty() {
            info!("{} Received cancellations: {}", &monitor.remote_ip(), received_cancels);
        }

        // TODO: run engine

        let pending_msgs = peer_status.take_pending_uploader_msgs();
        Some(vec![upload_monitor_fut(monitor, pending_msgs)])
    }
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

fn download_monitor_fut<I>(
    mut monitor: Box<DownloadChannelMonitor>,
    tx_msgs: I,
) -> Operation<'static>
where
    I: Iterator<Item = DownloaderMessage> + 'static,
{
    async move {
        let remote_ip = *monitor.remote_ip();
        if let Err(_e) = monitor.send_outgoing(tx_msgs).await {
            return OperationOutput::DownloadFromPeer(Err(remote_ip));
        }
        match monitor.receive_incoming(Duration::from_secs(1)).await {
            Err(ChannelError::ConnectionClosed) => {
                OperationOutput::DownloadFromPeer(Err(remote_ip))
            }
            _ => OperationOutput::DownloadFromPeer(Ok(monitor)),
        }
    }
    .boxed_local()
}

fn upload_monitor_fut<I>(mut monitor: Box<UploadChannelMonitor>, tx_msgs: I) -> Operation<'static>
where
    I: Iterator<Item = UploaderMessage> + 'static,
{
    async move {
        let remote_ip = *monitor.remote_ip();
        if let Err(_e) = monitor.send_outgoing(tx_msgs).await {
            return OperationOutput::UploadToPeer(Err(remote_ip));
        }
        match monitor.receive_incoming(Duration::from_secs(1)).await {
            Err(ChannelError::ConnectionClosed) => OperationOutput::UploadToPeer(Err(remote_ip)),
            _ => OperationOutput::UploadToPeer(Ok(monitor)),
        }
    }
    .boxed_local()
}
