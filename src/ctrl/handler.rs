use crate::ctrl::operations::*;
use crate::ctrl::peers::*;
use crate::data::{BlockAccountant, PieceInfo};
use crate::data::{PieceTracker, Storage};
use crate::pwp::*;
use crate::tracker::{http, udp, utils};
use crate::utils::dispatch::Handler;
use crate::utils::meta::Metainfo;
use futures::prelude::*;
use log::{error, info};
use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, SocketAddrV4};
use std::rc::Rc;
use std::time::Duration;
use std::{io, iter};

pub struct OperationHandler {
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
    debug_finished: bool,
}

impl OperationHandler {
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
            known_peers: HashSet::from([SocketAddr::V4(external_local_ip)]),
            peermgr,
            stored_channels: HashMap::new(),
            debug_finished: false,
        })
    }
}

impl<'h> Handler<'h> for OperationHandler {
    type OperationResult = Action;

    fn first_operations(&mut self) -> Vec<Operation<'h>> {
        let http_trackers = utils::get_http_tracker_addrs(&self.metainfo).into_iter();
        let udp_trackers = utils::get_udp_tracker_addrs(&self.metainfo).into_iter();
        [
            self.create_http_announce_ops(Some(http::AnnounceEvent::Started), http_trackers),
            self.create_udp_announce_ops(udp::AnnounceEvent::Started, udp_trackers),
            self.create_listener_ops(),
            vec![
                Action::new_timer(Duration::from_secs(60), TimerType::DebugShutdown).boxed_local(),
            ],
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    fn next_operations(&mut self, last_operation_result: Action) -> Option<Vec<Operation<'h>>> {
        match last_operation_result {
            Action::DownloadMsgSent(result) => self.process_download_msg_sent(result),
            Action::DownloadMsgReceived(result) => self.process_download_msg_received(result),
            Action::UploadMsgSent(result) => self.process_upload_msg_sent(result),
            Action::UploadMsgReceived(result) => self.process_upload_msg_received(result),
            Action::UdpAnnounce(response) => self.process_udp_announce_result(*response),
            Action::HttpAnnounce(response) => self.process_http_announce_result(*response),
            Action::PeerConnectivity(result) => self.process_connect_result(result),
            Action::PeerListen(monitor) => Some(self.process_listener_result(monitor)),
            Action::Timeout(timer) => self.process_timeout(timer),
            Action::Void => None,
        }
    }

    fn finished(&self) -> bool {
        self.local_records.missing_bytes() == 0 || self.debug_finished
    }
}

impl<'h> OperationHandler {
    fn create_udp_announce_ops(
        &mut self,
        event: udp::AnnounceEvent,
        tracker_addrs: impl Iterator<Item = String>,
    ) -> Vec<Operation<'h>> {
        let downloaded = self.local_records.accounted_bytes();
        let left = self.local_records.missing_bytes();
        let uploaded = self.peermgr.all_upload_monitors().map(|um| um.bytes_sent()).sum::<usize>();

        let announce_request = udp::AnnounceRequest {
            info_hash: *self.metainfo.info_hash(),
            peer_id: self.local_peer_id,
            downloaded: downloaded as u64,
            left: left as u64,
            uploaded: uploaded as u64,
            event,
            ip: None,
            key: 0,
            num_want: Some(50),
            port: self.external_local_ip.port(),
        };

        tracker_addrs
            .enumerate()
            .map(|(index, tracker_addr)| {
                let mut addr = self.internal_local_ip;
                addr.set_port(addr.port() + index as u16);
                Action::new_udp_announce(addr, tracker_addr, announce_request.clone()).boxed_local()
            })
            .collect()
    }

    fn create_http_announce_ops(
        &mut self,
        event: Option<http::AnnounceEvent>,
        tracker_urls: impl Iterator<Item = String>,
    ) -> Vec<Operation<'h>> {
        let downloaded = self.local_records.accounted_bytes();
        let left = self.local_records.missing_bytes();
        let uploaded = self.peermgr.all_upload_monitors().map(|um| um.bytes_sent()).sum::<usize>();

        tracker_urls
            .filter_map(|url| {
                let mut request = http::TrackerRequestBuilder::try_from(url.as_str())
                    .map_err(|e| error!("Invalid tracker url ({url}): {e}"))
                    .ok()?;
                request
                    .info_hash(self.metainfo.info_hash())
                    .peer_id(&self.local_peer_id)
                    .bytes_downloaded(downloaded)
                    .bytes_left(left)
                    .bytes_uploaded(uploaded)
                    .numwant(50)
                    .port(self.external_local_ip.port());
                if let Some(event) = &event {
                    request.event(*event);
                }
                Some(Action::new_http_announce(request, url).boxed_local())
            })
            .collect()
    }

    fn process_udp_announce_result(
        &mut self,
        outcome: io::Result<(udp::AnnounceResponse, String)>,
    ) -> Option<Vec<Operation<'h>>> {
        let (response, tracker_addr) =
            outcome.map_err(|e| error!("Udp announce error: {}", e)).ok()?;
        info!(
            "Received UDP announce response with {} ips, {} seeders, {} leechers",
            response.ips.len(),
            response.seeders,
            response.leechers,
        );
        let mut ops = response
            .ips
            .into_iter()
            .filter_map(|ip| {
                self.known_peers.insert(ip).then_some(
                    Action::new_outgoing_connect(
                        self.local_peer_id,
                        *self.metainfo.info_hash(),
                        ip,
                    )
                    .boxed_local(),
                )
            })
            .collect::<Vec<_>>();
        ops.push(
            Action::new_timer(
                Duration::from_secs(response.interval as u64),
                TimerType::UdpReannounce { tracker_addr },
            )
            .boxed_local(),
        );
        Some(ops)
    }

    fn process_http_announce_result(
        &mut self,
        outcome: Result<(http::AnnounceResponseContent, String), http::Error>,
    ) -> Option<Vec<Operation<'h>>> {
        let (response, tracker_url) =
            outcome.map_err(|e| error!("Http announce error: {e}")).ok()?;
        info!("Received HTTP announce response: {response}");
        let mut ops = response
            .peers()?
            .into_iter()
            .filter_map(|ip| {
                self.known_peers.insert(ip).then_some(
                    Action::new_outgoing_connect(
                        self.local_peer_id,
                        *self.metainfo.info_hash(),
                        ip,
                    )
                    .boxed_local(),
                )
            })
            .collect::<Vec<_>>();
        ops.push(
            Action::new_timer(
                Duration::from_secs(response.interval().unwrap_or(900) as u64),
                TimerType::HttpReannounce { tracker_url },
            )
            .boxed_local(),
        );
        Some(ops)
    }

    fn create_listener_ops(&mut self) -> Vec<Operation<'h>> {
        match listener_on_addr(self.internal_local_ip) {
            Ok((monitor, receiver)) => {
                vec![
                    Action::from_listener_runner(receiver, self.internal_local_ip).boxed_local(),
                    Action::from_listen_monitor(Box::new(monitor)).boxed_local(),
                ]
            }
            Err(e) => {
                error!("Failed to create TCP listener: {}", e);
                Vec::new()
            }
        }
    }

    fn process_timeout(&mut self, what: TimerType) -> Option<Vec<Operation<'h>>> {
        match what {
            TimerType::DebugShutdown => {
                self.debug_finished = true;
                None
            }
            TimerType::HttpReannounce { tracker_url } => {
                Some(self.create_http_announce_ops(None, iter::once(tracker_url)))
            }
            TimerType::UdpReannounce { tracker_addr } => Some(
                self.create_udp_announce_ops(udp::AnnounceEvent::None, iter::once(tracker_addr)),
            ),
        }
    }

    fn process_listener_result(&mut self, mut monitor: Box<ListenMonitor>) -> Vec<Operation<'h>> {
        if let Some(stream) = monitor.take_pending_stream() {
            let mut ops = vec![Action::from_listen_monitor(monitor).boxed_local()];
            if let Ok(remote_ip) = stream.peer_addr() {
                self.known_peers.insert(remote_ip);
                ops.push(
                    Action::new_incoming_connect(
                        self.local_peer_id,
                        *self.metainfo.info_hash(),
                        stream,
                        remote_ip,
                    )
                    .boxed_local(),
                );
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
            Action::from_connection_runner(runner).boxed_local(),
            Action::from_download_rx_channel(download_rx).boxed_local(),
            Action::from_upload_rx_channel(upload_rx).boxed_local(),
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
            Some(vec![Action::from_download_tx_channel(tx_channel, msg).boxed_local()])
        } else {
            let (download_channel_slot, _) =
                self.stored_channels.get_mut(tx_channel.remote_ip()).unwrap();
            download_channel_slot.replace(tx_channel);
            None
        }
    }
    fn process_download_msg_received(
        &mut self,
        outcome: Result<Box<(DownloadRxChannel, UploaderMessage)>, SocketAddr>,
    ) -> Option<Vec<Operation<'h>>> {
        let (rx_channel, msg) = *outcome
            .map_err(|remote_ip| {
                error!("DownloadRxChannel error, disconnected {remote_ip}");
                self.erase_peer(&remote_ip);
            })
            .ok()?;

        let remote_ip = *rx_channel.remote_ip();
        self.peermgr.download_handler(&remote_ip)?.update_state(&msg);

        match msg {
            UploaderMessage::Block(info, data) => {
                // TODO: ignore unless interested and peer not choking
                if let Ok(global_offset) = self.local_records.submit_block(&info) {
                    self.filekeeper
                        .write_block(global_offset, data)
                        .expect("Failed to write to file");

                    if self.local_records.has_piece(info.piece_index) {
                        // TODO: check hash
                        self.piece_availability.forget_piece(info.piece_index);
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

        let mut ops = vec![Action::from_download_rx_channel(rx_channel).boxed_local()];
        let (download_channel_slot, _) = self.stored_channels.get_mut(&remote_ip).unwrap();
        if let Some(tx_channel) = download_channel_slot.take() {
            if let Some(msg) = self.peermgr.download_handler(&remote_ip).unwrap().next_outbound() {
                ops.push(Action::from_download_tx_channel(tx_channel, msg).boxed_local());
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
            Some(vec![Action::from_upload_tx_channel(
                tx_channel,
                msg,
                self.pieces.clone(),
                self.filekeeper.clone(),
            )
            .boxed_local()])
        } else {
            let (_, upload_channel_slot) =
                self.stored_channels.get_mut(tx_channel.remote_ip()).unwrap();
            upload_channel_slot.replace(tx_channel);
            None
        }
    }

    fn process_upload_msg_received(
        &mut self,
        outcome: Result<Box<(UploadRxChannel, DownloaderMessage)>, SocketAddr>,
    ) -> Option<Vec<Operation<'h>>> {
        let (rx_channel, msg) = *outcome
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

        let mut ops = vec![Action::from_upload_rx_channel(rx_channel).boxed_local()];
        let (_, upload_channel_slot) = self.stored_channels.get_mut(&remote_ip).unwrap();
        if let Some(tx_channel) = upload_channel_slot.take() {
            if let Some(msg) = self.peermgr.upload_handler(&remote_ip).unwrap().next_outbound() {
                ops.push(
                    Action::from_upload_tx_channel(
                        tx_channel,
                        msg,
                        self.pieces.clone(),
                        self.filekeeper.clone(),
                    )
                    .boxed_local(),
                );
            } else {
                upload_channel_slot.replace(tx_channel);
            }
        }
        Some(ops)
    }
}

impl OperationHandler {
    fn erase_peer(&mut self, remote_ip: &SocketAddr) {
        self.peermgr.remove_peer(remote_ip);
        self.stored_channels.remove(remote_ip);
        self.piece_availability.forget_peer(*remote_ip);
    }
}
