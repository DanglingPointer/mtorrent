use crate::dispatch::Handler;
use crate::peers::*;
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
use std::io;
use std::net::{SocketAddr, SocketAddrV4, UdpSocket};
use std::rc::Rc;
use std::time::Duration;

pub struct OperationController {
    metainfo: MetaInfo,
    internal_local_ip: SocketAddrV4,
    external_local_ip: SocketAddrV4,
    local_peer_id: [u8; 20],
    piecekeeper: Rc<PieceKeeper>,
    local_records: Accountant,
    known_peers: HashSet<SocketAddr>,
}

impl OperationController {
    pub fn new(
        metainfo: MetaInfo,
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
            local_records,
            known_peers: HashSet::new(),
        })
    }
}

pub enum OperationOutput {
    DownloadFromPeer(Box<DownloadMonitor>),
    UploadToPeer(Box<UploadMonitor>),
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
                let mut addr = self.internal_local_ip.clone();
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
                let download_mon = Box::new(DownloadMonitor::from(download_ch));
                let mut upload_mon = Box::new(UploadMonitor::from(upload_ch));
                let upload_ops = {
                    if self.local_records.accounted_bytes() > 0 {
                        let bitfield = self.local_records.generate_bitfield();
                        let send_bitfield_fut = async move {
                            upload_mon.handle_outgoing(UploaderMessage::Bitfield(bitfield)).await;
                            OperationOutput::UploadToPeer(upload_mon)
                        };
                        vec![send_bitfield_fut.boxed_local()]
                    } else {
                        self.process_upload_monitor(upload_mon)
                    }
                };
                vec![
                    vec![runner_fut.boxed_local()],
                    self.process_download_monitor(download_mon),
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
        mut monitor: Box<DownloadMonitor>,
    ) -> Vec<Operation<'h>> {
        if let Some(ChannelError::ConnectionClosed) = monitor.take_pending_error() {
            error!("{} DownloadMonitor error", monitor.channel().remote_ip());
            self.known_peers.remove(monitor.channel().remote_ip());
            return Vec::new();
        }
        if let Some(msg) = monitor.take_pending_message() {
            info!("{} Message received: {}", monitor.channel().remote_ip(), msg);
            // TODO: handle message
        }
        // TODO: schedule write? Or:
        let read_fut = async move {
            monitor.handle_incoming(Duration::from_secs(5)).await;
            OperationOutput::DownloadFromPeer(monitor)
        };
        vec![read_fut.boxed_local()]
    }
    fn process_upload_monitor(&mut self, mut monitor: Box<UploadMonitor>) -> Vec<Operation<'h>> {
        if let Some(ChannelError::ConnectionClosed) = monitor.take_pending_error() {
            error!("{} UploadMonitor error", monitor.channel().remote_ip());
            self.known_peers.remove(monitor.channel().remote_ip());
            return Vec::new();
        }
        if let Some(msg) = monitor.take_pending_message() {
            info!("{} Message received: {}", monitor.channel().remote_ip(), msg);
            // TODO: handle message
        }
        // TODO: schedule write? Or:
        let read_fut = async move {
            monitor.handle_incoming(Duration::from_secs(5)).await;
            OperationOutput::UploadToPeer(monitor)
        };
        vec![read_fut.boxed_local()]
    }
}
