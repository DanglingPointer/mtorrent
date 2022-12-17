use crate::ctrl::monitors::{DownloadChannelMonitor, UploadChannelMonitor};
use crate::dispatch::Handler;
use crate::peers::*;
use crate::storage::files::FileKeeper;
use crate::storage::meta::MetaInfo;
use crate::storage::pieces::{Accountant, PieceKeeper};
use crate::tracker::http::TrackerResponseContent;
use crate::tracker::udp::{AnnounceEvent, AnnounceRequest, AnnounceResponse, UdpTrackerConnection};
use crate::tracker::utils::get_udp_tracker_addrs;
use async_io::Async;
use futures::future::LocalBoxFuture;
use futures::FutureExt;
use log::{error, info, warn};
use std::collections::HashSet;
use std::net::{SocketAddr, SocketAddrV4, UdpSocket};
use std::rc::Rc;
use std::time::Duration;
use std::{io, iter};

pub struct OperationController {
    metainfo: MetaInfo,
    internal_local_ip: SocketAddrV4,
    external_local_ip: SocketAddrV4,
    local_peer_id: [u8; 20],
    piecekeeper: Rc<PieceKeeper>,
    filekeeper: FileKeeper,
    local_records: Accountant,
    known_peers: HashSet<SocketAddr>,
}

impl OperationController {
    pub fn new(
        metainfo: MetaInfo,
        filekeeper: FileKeeper,
        internal_local_ip: SocketAddrV4,
        external_local_ip: SocketAddrV4,
        local_peer_id: [u8; 20],
    ) -> Option<Self> {
        let piecekeeper = Rc::new(PieceKeeper::new(metainfo.pieces()?, metainfo.piece_length()?));
        let local_records = Accountant::new(piecekeeper.clone());
        Some(Self {
            metainfo,
            internal_local_ip,
            external_local_ip,
            local_peer_id,
            piecekeeper,
            filekeeper,
            local_records,
            known_peers: HashSet::new(),
        })
    }
}

pub enum OperationOutput {
    DownloadFromPeer(Result<Box<DownloadChannelMonitor>, SocketAddr>),
    UploadToPeer(Result<Box<UploadChannelMonitor>, SocketAddr>),
    PeerConnectivity(Box<io::Result<(DownloadChannel, UploadChannel, ConnectionRunner)>>),
    PeerListen(Box<ListenMonitor>),
    UdpAnnounce(Box<io::Result<AnnounceResponse>>),
    HttpAnnounce(Box<io::Result<TrackerResponseContent>>),
    Void,
}

type Operation<'o> = LocalBoxFuture<'o, OperationOutput>;

impl<'h> Handler<'h> for OperationController {
    type OperationResult = OperationOutput;

    fn first_operations(&mut self) -> Vec<Operation<'h>> {
        vec![
            self.create_udp_announce_ops(AnnounceEvent::Started),
            self.create_listener_ops(),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    fn next_operations(&mut self, last_operation_result: OperationOutput) -> Vec<Operation<'h>> {
        match last_operation_result {
            OperationOutput::UdpAnnounce(response) => self.process_udp_announce_result(response),
            OperationOutput::DownloadFromPeer(monitor) => self.process_download_monitor(monitor),
            OperationOutput::UploadToPeer(monitor) => self.process_upload_monitor(monitor),
            OperationOutput::PeerConnectivity(result) => self.process_connect_result(result),
            OperationOutput::PeerListen(monitor) => self.process_listener_result(monitor),
            OperationOutput::HttpAnnounce(_) => {
                todo!()
            }
            OperationOutput::Void => {
                // TODO: timer, periodic announce?
                Vec::new()
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
    fn process_udp_announce_result(
        &mut self,
        result: Box<io::Result<AnnounceResponse>>,
    ) -> Vec<Operation<'h>> {
        match result.as_ref() {
            Ok(response) => {
                info!("Received announce response: {:?}", response);
                response
                    .ips
                    .iter()
                    .flat_map(|ip| {
                        let new_peer = self.known_peers.insert(*ip);
                        if new_peer {
                            self.create_connect_ops(*ip)
                        } else {
                            Vec::new()
                        }
                    })
                    .collect()
            }
            Err(e) => {
                info!("Announce error: {}", e);
                Vec::new()
            }
        }
    }
}

impl<'h> OperationController {
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
                let mut monitor = Box::new(monitor);
                let monitor_fut = async move {
                    monitor.handle_incoming().await;
                    OperationOutput::PeerListen(monitor)
                };
                vec![runner_fut.boxed_local(), monitor_fut.boxed_local()]
            }
            Err(e) => {
                error!("Failed to create TCP listener: {}", e);
                Vec::new()
            }
        }
    }
    fn process_listener_result(&mut self, mut monitor: Box<ListenMonitor>) -> Vec<Operation<'h>> {
        if let Some(stream) = monitor.take_pending_stream() {
            let local_peer_id = self.local_peer_id;
            let info_hash = *self.metainfo.info_hash();
            let connect_fut = async move {
                let result = channels_from_incoming(&local_peer_id, Some(&info_hash), stream).await;
                OperationOutput::PeerConnectivity(Box::new(result))
            };
            let listen_fut = async move {
                monitor.handle_incoming().await;
                OperationOutput::PeerListen(monitor)
            };
            vec![connect_fut.boxed_local(), listen_fut.boxed_local()]
        } else {
            // error
            self.create_listener_ops()
        }
    }
}

impl<'h> OperationController {
    fn create_connect_ops(&mut self, remote_ip: SocketAddr) -> Vec<Operation<'h>> {
        let local_peer_id = self.local_peer_id;
        let info_hash = *self.metainfo.info_hash();
        let connect_fut = async move {
            let result = channels_from_outgoing(&local_peer_id, &info_hash, remote_ip, None).await;
            OperationOutput::PeerConnectivity(Box::new(result))
        };
        vec![connect_fut.boxed_local()]
    }
    fn process_connect_result(
        &mut self,
        result: Box<io::Result<(DownloadChannel, UploadChannel, ConnectionRunner)>>,
    ) -> Vec<Operation<'h>> {
        match *result {
            Ok((download_ch, upload_ch, runner)) => {
                let runner_fut = async move {
                    if let Err(e) = runner.run().await {
                        warn!("Peer runner exited: {}", e);
                    }
                    OperationOutput::Void
                };
                let download_mon =
                    Box::new(DownloadChannelMonitor::new(download_ch, self.piecekeeper.clone()));
                let mut upload_mon = Box::new(UploadChannelMonitor::new(upload_ch));
                let upload_ops = {
                    if self.local_records.accounted_bytes() > 0 {
                        let bitfield = self.local_records.generate_bitfield();
                        let send_bitfield_fut = async move {
                            let remote_ip = *upload_mon.remote_ip();
                            match upload_mon
                                .send_outgoing(iter::once(UploaderMessage::Bitfield(bitfield)))
                                .await
                            {
                                Ok(()) => OperationOutput::UploadToPeer(Ok(upload_mon)),
                                Err(_) => OperationOutput::UploadToPeer(Err(remote_ip)),
                            }
                        };
                        vec![send_bitfield_fut.boxed_local()]
                    } else {
                        self.process_upload_monitor(Ok(upload_mon))
                    }
                };
                vec![
                    vec![runner_fut.boxed_local()],
                    self.process_download_monitor(Ok(download_mon)),
                    upload_ops,
                ]
                .into_iter()
                .flatten()
                .collect()
            }
            Err(e) => {
                error!("Failed to connect to peer: {}", e);
                Vec::new()
            }
        }
    }
}

impl<'h> OperationController {
    fn process_download_monitor(
        &mut self,
        outcome: Result<Box<DownloadChannelMonitor>, SocketAddr>,
    ) -> Vec<Operation<'h>> {
        match outcome {
            Err(remote_ip) => {
                error!("DownloadMonitor error, disconnected {}", &remote_ip);
                self.known_peers.remove(&remote_ip);
                Vec::new()
            }
            Ok(mut monitor) => {
                // TODO: update state
                for (info, data) in monitor.received_blocks() {
                    if let Ok(global_offset) = self.piecekeeper.global_offset(
                        info.piece_index,
                        info.in_piece_offset,
                        info.block_length,
                    ) {
                        self.local_records.submit_block(global_offset, info.block_length);
                        self.filekeeper
                            .write_block(global_offset, data)
                            .expect("Failed to write to file");
                    }
                }
                // TODO: schedule write? Or:
                let read_fut = async move {
                    let remote_ip = *monitor.remote_ip();
                    match monitor.receive_incoming(Duration::from_secs(1)).await {
                        Err(ChannelError::ConnectionClosed) => {
                            OperationOutput::DownloadFromPeer(Err(remote_ip))
                        }
                        _ => OperationOutput::DownloadFromPeer(Ok(monitor)),
                    }
                };
                vec![read_fut.boxed_local()]
            }
        }
    }
    fn process_upload_monitor(
        &mut self,
        outcome: Result<Box<UploadChannelMonitor>, SocketAddr>,
    ) -> Vec<Operation<'h>> {
        match outcome {
            Err(remote_ip) => {
                error!("UploadMonitor error, disconnected {}", &remote_ip);
                self.known_peers.remove(&remote_ip);
                Vec::new()
            }
            Ok(mut monitor) => {
                // TODO: update state
                let received_requests = monitor
                    .received_requests()
                    .map(|block_info| format!("{}", block_info))
                    .collect::<Vec<String>>()
                    .join(", ");
                if !received_requests.is_empty() {
                    info!("{} Received requests: {}", &monitor.remote_ip(), received_requests);
                }
                let received_cancellations = monitor
                    .received_cancellations()
                    .map(|block_info| format!("{}", block_info))
                    .collect::<Vec<String>>()
                    .join(", ");
                if !received_cancellations.is_empty() {
                    info!(
                        "{} Received cancellations: {}",
                        &monitor.remote_ip(),
                        received_cancellations
                    );
                }
                // TODO: schedule write? Or:
                let read_fut = async move {
                    let remote_ip = *monitor.remote_ip();
                    match monitor.receive_incoming(Duration::from_secs(1)).await {
                        Err(ChannelError::ConnectionClosed) => {
                            OperationOutput::UploadToPeer(Err(remote_ip))
                        }
                        _ => OperationOutput::UploadToPeer(Ok(monitor)),
                    }
                };
                vec![read_fut.boxed_local()]
            }
        }
    }
}
