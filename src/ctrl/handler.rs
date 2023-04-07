use crate::ctrl::operations::*;
use crate::ctrl::peers::*;
use crate::data;
use crate::engine::{self, MonitorOwner};
use crate::pwp::*;
use crate::tracker::{http, udp, utils};
use crate::utils::dispatch::Handler;
use crate::utils::meta::Metainfo;
use futures::prelude::*;
use log::{error, info};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, SocketAddrV4};
use std::rc::Rc;
use std::time::Duration;
use std::{io, iter};
use tokio::runtime;

pub trait DownloadChannelHandler {
    fn update_state(&mut self, inbound: &UploaderMessage);
    fn next_outbound(&mut self) -> Option<DownloaderMessage>;
}

pub trait UploadChannelHandler {
    fn update_state(&mut self, inbound: &DownloaderMessage);
    fn next_outbound(&mut self) -> Option<UploaderMessage>;
}

pub trait HandlerOwner {
    fn download_handler(
        &mut self,
        remote_ip: &SocketAddr,
    ) -> Option<&mut dyn DownloadChannelHandler>;

    fn upload_handler(&mut self, remote_ip: &SocketAddr) -> Option<&mut dyn UploadChannelHandler>;
}

struct Timer<'a, 'o> {
    ops: &'a mut Vec<Operation<'o>>,
}

impl<'a, 'o> Timer<'a, 'o> {
    fn new(ops: &'a mut Vec<Operation<'o>>) -> Self {
        Self { ops }
    }
}

impl<'a, 'o> engine::Timer for Timer<'a, 'o> {
    fn schedule(&mut self, delay: Duration, f: Box<dyn FnOnce(&mut engine::Context) + 'static>) {
        self.ops.push(
            Action::new_timer(delay, move |handler: &mut OperationHandler| {
                let mut ops = Vec::new();
                let mut timer = Timer::new(&mut ops);
                let mut ctx = engine_context(&mut handler.ctx, &mut timer);
                f(&mut ctx);
                handler.fill_tx_channels(&mut ops);
                Some(ops)
            })
            .boxed_local(),
        );
    }
}

struct Context {
    local_availability: data::BlockAccountant,
    piece_tracker: data::PieceTracker,
    peermgr: PeerManager,
    state: engine::State,
}

fn engine_context<'a>(ctx: &'a mut Context, timer: &'a mut Timer<'a, '_>) -> engine::Context<'a> {
    engine::Context {
        local_availability: &ctx.local_availability,
        piece_tracker: &ctx.piece_tracker,
        monitor_owner: &ctx.peermgr,
        state: &mut ctx.state,
        timer,
    }
}

type Slot<C> = std::cell::Cell<Option<C>>;

pub struct OperationHandler {
    metainfo: Rc<Metainfo>,
    internal_local_ip: SocketAddrV4,
    external_local_ip: SocketAddrV4,
    local_peer_id: [u8; 20],
    filekeeper: Rc<data::StorageClient>,
    known_peers: HashSet<SocketAddr>,
    stored_channels: HashMap<SocketAddr, (Slot<DownloadTxChannel>, Slot<UploadTxChannel>)>,
    pieces: Rc<data::PieceInfo>,
    ctx: Context,
    debug_finished: bool,
    pwp_worker_handle: runtime::Handle,
}

impl OperationHandler {
    pub fn new(
        metainfo: Rc<Metainfo>,
        filekeeper: data::StorageClient,
        internal_local_ip: SocketAddrV4,
        external_local_ip: SocketAddrV4,
        local_peer_id: [u8; 20],
        pwp_worker_handle: runtime::Handle,
    ) -> Option<Self> {
        let pieces = Rc::new(data::PieceInfo::new(metainfo.pieces()?, metainfo.piece_length()?));
        let ctx = Context {
            local_availability: data::BlockAccountant::new(pieces.clone()),
            piece_tracker: data::PieceTracker::new(pieces.piece_count()),
            peermgr: PeerManager::new(pieces.clone()),
            state: Default::default(),
        };
        Some(Self {
            metainfo,
            internal_local_ip,
            external_local_ip,
            local_peer_id,
            filekeeper: Rc::new(filekeeper),
            known_peers: HashSet::from([SocketAddr::V4(external_local_ip)]),
            stored_channels: HashMap::new(),
            pieces,
            ctx,
            debug_finished: false,
            pwp_worker_handle,
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
                Action::new_timer(Duration::from_secs(60), |ctx: &mut Self| {
                    ctx.debug_finished = true;
                    None
                })
                .boxed_local(),
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
            Action::PieceVerification(result) => self.process_piece_verification(result),
            Action::UdpAnnounce(response) => self.process_udp_announce_result(*response),
            Action::HttpAnnounce(response) => self.process_http_announce_result(*response),
            Action::PeerConnectivity(result) => self.process_connect_result(result),
            Action::PeerListen(monitor) => Some(self.process_listener_result(monitor)),
            Action::Timeout(func) => self.process_timeout(func),
            Action::Void => None,
        }
    }

    fn finished(&self) -> bool {
        self.ctx.local_availability.missing_bytes() == 0 || self.debug_finished
    }
}

impl<'h> OperationHandler {
    fn create_udp_announce_ops(
        &mut self,
        event: udp::AnnounceEvent,
        tracker_addrs: impl Iterator<Item = String>,
    ) -> Vec<Operation<'h>> {
        let downloaded = self.ctx.local_availability.accounted_bytes();
        let left = self.ctx.local_availability.missing_bytes();
        let uploaded = self
            .ctx
            .peermgr
            .all_upload_monitors()
            .map(|(_, um)| um.bytes_sent())
            .sum::<usize>();

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
        let downloaded = self.ctx.local_availability.accounted_bytes();
        let left = self.ctx.local_availability.missing_bytes();
        let uploaded = self
            .ctx
            .peermgr
            .all_upload_monitors()
            .map(|(_, um)| um.bytes_sent())
            .sum::<usize>();

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
                move |ctx: &mut Self| {
                    Some(ctx.create_udp_announce_ops(
                        udp::AnnounceEvent::None,
                        iter::once(tracker_addr),
                    ))
                },
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
                move |ctx: &mut Self| {
                    Some(ctx.create_http_announce_ops(None, iter::once(tracker_url)))
                },
            )
            .boxed_local(),
        );
        Some(ops)
    }

    fn create_listener_ops(&mut self) -> Vec<Operation<'h>> {
        match listener_on_addr(self.internal_local_ip) {
            Ok((monitor, receiver)) => {
                let local_ip = self.internal_local_ip;
                self.pwp_worker_handle.spawn(async move {
                    Action::from_listener_runner(receiver, local_ip).await;
                });
                vec![Action::from_listen_monitor(Box::new(monitor)).boxed_local()]
            }
            Err(e) => {
                error!("Failed to create TCP listener: {}", e);
                Vec::new()
            }
        }
    }

    fn process_timeout(&mut self, f: DelayedFn) -> Option<Vec<Operation<'h>>> {
        let ctx = self as &mut dyn Any;
        f(ctx)
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

        self.ctx.peermgr.add_peer(&remote_ip);
        self.stored_channels
            .insert(remote_ip, (Slot::new(Some(download_tx)), Slot::new(Some(upload_tx))));

        if self.ctx.local_availability.accounted_bytes() > 0 {
            // TODO: handle this somewhere else
            let bitfield = self.ctx.local_availability.generate_bitfield();
            self.ctx
                .peermgr
                .upload_monitor(&remote_ip)
                .unwrap()
                .submit_outbound(UploaderMessage::Bitfield(bitfield));
        }

        self.pwp_worker_handle.spawn(async move {
            Action::from_connection_runner(runner).await;
        });
        let mut ops = vec![
            Action::from_download_rx_channel(download_rx).boxed_local(),
            Action::from_upload_rx_channel(upload_rx).boxed_local(),
        ];

        self.run_engine(&mut ops);
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

        let handlers = &mut self.ctx.peermgr as &mut dyn HandlerOwner;
        let download_handler = handlers.download_handler(tx_channel.remote_ip())?;
        if let Some(msg) = download_handler.next_outbound() {
            Some(vec![Action::from_download_tx_channel(tx_channel, msg).boxed_local()])
        } else {
            let (download_channel_slot, _) =
                self.stored_channels.get(tx_channel.remote_ip()).unwrap();
            download_channel_slot.replace(Some(tx_channel));
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
        self.ctx.peermgr.download_handler(&remote_ip)?.update_state(&msg);

        let should_run_engine = !matches!(&msg, UploaderMessage::Block(_, _));

        let mut ops = vec![Action::from_download_rx_channel(rx_channel).boxed_local()];
        match msg {
            UploaderMessage::Block(info, data) => {
                // TODO: ignore unless interested and peer not choking
                if let Ok(global_offset) = self.ctx.local_availability.submit_block(&info) {
                    self.filekeeper.start_write_block(global_offset, data).unwrap_or_else(|e| {
                        panic!("Failed to start write ({info}) to storage: {e}")
                    });

                    if self.ctx.local_availability.has_piece(info.piece_index) {
                        ops.push(
                            Action::from_piece_verification(
                                info.piece_index,
                                self.pieces.clone(),
                                self.filekeeper.clone(),
                                self.metainfo.clone(),
                            )
                            .boxed_local(),
                        );
                    }
                } else {
                    error!("Received block ignored: {info}");
                }
            }
            UploaderMessage::Bitfield(bitfield) => {
                // temp
                self.ctx.piece_tracker.add_bitfield_record(&remote_ip, &bitfield);
            }
            UploaderMessage::Have { piece_index } => {
                // temp
                self.ctx.piece_tracker.add_single_record(&remote_ip, piece_index);
            }
            _ => (),
        }

        if should_run_engine {
            self.run_engine(&mut ops);
        } else {
            self.fill_tx_channels(&mut ops);
        }
        Some(ops)
    }

    fn process_piece_verification(
        &mut self,
        outcome: Result<usize, usize>,
    ) -> Option<Vec<Operation<'h>>> {
        match outcome {
            Ok(piece_index) => {
                self.ctx.piece_tracker.forget_piece(piece_index);
                // temp:
                for (_ip, upload_monitor) in self.ctx.peermgr.all_upload_monitors() {
                    upload_monitor.submit_outbound(UploaderMessage::Have { piece_index });
                }

                let mut ops = Vec::new();
                self.run_engine(&mut ops);
                Some(ops)
            }
            Err(piece_index) => {
                self.ctx.local_availability.remove_piece(piece_index);
                None
            }
        }
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

        let upload_handler = self.ctx.peermgr.upload_handler(tx_channel.remote_ip())?;
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
                self.stored_channels.get(tx_channel.remote_ip()).unwrap();
            upload_channel_slot.replace(Some(tx_channel));
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
        self.ctx.peermgr.upload_handler(&remote_ip)?.update_state(&msg);

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

        let mut ops = vec![Action::from_upload_rx_channel(rx_channel).boxed_local()];
        self.run_engine(&mut ops);
        Some(ops)
    }

    fn fill_tx_channels(&mut self, ops: &mut Vec<Operation<'h>>) {
        let handlers = &mut self.ctx.peermgr;
        for (ip, (download_channel_slot, upload_channel_slot)) in &self.stored_channels {
            if let Some(channel) = download_channel_slot.take() {
                if let Some(msg) = handlers.download_handler(ip).and_then(|h| h.next_outbound()) {
                    ops.push(Action::from_download_tx_channel(channel, msg).boxed_local());
                } else {
                    download_channel_slot.replace(Some(channel));
                }
            }
            if let Some(channel) = upload_channel_slot.take() {
                if let Some(msg) = handlers.upload_handler(ip).and_then(|h| h.next_outbound()) {
                    ops.push(
                        Action::from_upload_tx_channel(
                            channel,
                            msg,
                            self.pieces.clone(),
                            self.filekeeper.clone(),
                        )
                        .boxed_local(),
                    );
                } else {
                    upload_channel_slot.replace(Some(channel));
                }
            }
        }
    }
}

impl OperationHandler {
    fn erase_peer(&mut self, remote_ip: &SocketAddr) {
        self.ctx.peermgr.remove_peer(remote_ip);
        self.stored_channels.remove(remote_ip);
        self.ctx.piece_tracker.forget_peer(remote_ip);
    }

    fn run_engine(&mut self, ops: &mut Vec<Operation<'_>>) {
        let mut timer = Timer::new(ops);
        let mut ctx = engine_context(&mut self.ctx, &mut timer);
        engine::update_interest(&mut ctx);

        self.fill_tx_channels(ops);
    }
}
