use crate::ops::{ctrl, ctx};
use crate::utils::{bandwidth, local_mpsc};
use crate::{data, debug_stopwatch, min, pwp, sec};
use futures::prelude::*;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use std::{cmp, io};
use tokio::sync::broadcast;
use tokio::time::{self, Instant};
use tokio::try_join;

type CtxHandle = ctx::Handle<ctx::MainCtx>;

struct Data {
    handle: CtxHandle,
    rx: pwp::DownloadRxChannel,
    tx: pwp::DownloadTxChannel,
    storage: data::StorageClient,
    piece_downloaded_channel: Rc<broadcast::Sender<usize>>,
    state: pwp::DownloadState,
    verified_pieces: usize,
}

impl Drop for Data {
    fn drop(&mut self) {
        self.handle.with_ctx(|ctx| {
            ctx.piece_tracker.forget_peer(self.rx.remote_ip());
            ctx.peer_states.remove_peer(self.rx.remote_ip());
            ctx.pending_requests.clear_requests_to(self.rx.remote_ip());
        });
    }
}

pub struct IdlePeer(Box<Data>);

pub struct SeedingPeer(Box<Data>);

pub enum Peer {
    Idle(IdlePeer),
    Seeder(SeedingPeer),
}

impl From<IdlePeer> for Peer {
    fn from(value: IdlePeer) -> Self {
        Peer::Idle(value)
    }
}

impl From<SeedingPeer> for Peer {
    fn from(value: SeedingPeer) -> Self {
        Peer::Seeder(value)
    }
}

macro_rules! to_enum {
    ($inner:expr) => {
        if $inner.state.am_interested && !$inner.state.peer_choking {
            SeedingPeer($inner).into()
        } else {
            IdlePeer($inner).into()
        }
    };
}

macro_rules! inner {
    ($inner:expr) => {
        match $inner {
            Peer::Idle(IdlePeer(data)) => data,
            Peer::Seeder(SeedingPeer(data)) => data,
        }
    };
}

macro_rules! update_ctx {
    ($inner:expr) => {
        $inner
            .handle
            .with_ctx(|ctx| ctx.peer_states.update_download($inner.rx.remote_ip(), &$inner.state));
    };
}

pub async fn new_peer(
    handle: CtxHandle,
    rx: pwp::DownloadRxChannel,
    tx: pwp::DownloadTxChannel,
    storage: data::StorageClient,
    piece_downloaded_channel: Rc<broadcast::Sender<usize>>,
) -> io::Result<IdlePeer> {
    let mut inner = Box::new(Data {
        handle,
        rx,
        tx,
        storage,
        piece_downloaded_channel,
        state: Default::default(),
        verified_pieces: 0,
    });
    // try wait for bitfield
    match inner.rx.receive_message_timed(sec!(1)).await {
        Ok(msg) => {
            update_state_with_msg(&mut inner.handle, &mut inner.state, inner.tx.remote_ip(), &msg);
        }
        Err(pwp::ChannelError::Timeout) => (),
        Err(e) => return Err(e.into()),
    }
    // process queued msgs (e.g. Have's) if any
    loop {
        match inner.rx.receive_message_timed(sec!(0)).await {
            Ok(msg) => {
                update_state_with_msg(
                    &mut inner.handle,
                    &mut inner.state,
                    inner.tx.remote_ip(),
                    &msg,
                );
            }
            Err(pwp::ChannelError::Timeout) => break,
            Err(e) => return Err(e.into()),
        }
    }
    update_ctx!(inner);
    Ok(IdlePeer(inner))
}

pub async fn activate(peer: IdlePeer) -> io::Result<Peer> {
    let mut inner = peer.0;
    debug_assert!(!inner.state.am_interested || inner.state.peer_choking);
    if !inner.state.am_interested {
        inner.tx.send_message(pwp::DownloaderMessage::Interested).await?;
        inner.state.am_interested = true;
        update_ctx!(inner);
    }
    if !inner.state.peer_choking {
        Ok(to_enum!(inner))
    } else {
        let mut peer = to_enum!(inner);
        let deadline = Instant::now() + min!(1);
        loop {
            peer = linger(peer, deadline).await?;
            match peer {
                Peer::Seeder(_) => {
                    return Ok(peer);
                }
                Peer::Idle(ref mut idle) if Instant::now() >= deadline => {
                    idle.0.tx.send_message(pwp::DownloaderMessage::NotInterested).await?;
                    idle.0.state.am_interested = false;
                    update_ctx!(idle.0);
                    return Ok(peer);
                }
                _ => (),
            }
        }
    }
}

pub async fn deactivate(peer: SeedingPeer) -> io::Result<IdlePeer> {
    let mut inner = peer.0;
    debug_assert!(inner.state.am_interested && !inner.state.peer_choking);
    inner.tx.send_message(pwp::DownloaderMessage::NotInterested).await?;
    inner.state.am_interested = false;
    update_ctx!(inner);
    Ok(IdlePeer(inner))
}

pub async fn linger(peer: Peer, deadline: Instant) -> io::Result<Peer> {
    let mut inner = inner!(peer);
    loop {
        match inner.rx.receive_message_timed(deadline - Instant::now()).await {
            Ok(msg) => {
                if update_state_with_msg(
                    &mut inner.handle,
                    &mut inner.state,
                    inner.tx.remote_ip(),
                    &msg,
                ) {
                    break;
                }
                if matches!(msg, pwp::UploaderMessage::Block(_, _)) {
                    log::debug!("Received block from {} while idle", inner.rx.remote_ip());
                }
            }
            Err(pwp::ChannelError::Timeout) => break,
            Err(e) => return Err(e.into()),
        }
    }
    update_ctx!(inner);
    Ok(to_enum!(inner))
}

pub async fn get_pieces(
    peer: SeedingPeer,
    pieces: impl ExactSizeIterator<Item = usize> + Clone,
) -> io::Result<Peer> {
    let mut inner = peer.0;
    debug_assert!(inner.state.am_interested && !inner.state.peer_choking);
    define_with_ctx!(inner.handle);
    let _sw =
        debug_stopwatch!("Download of {} piece(s) from {}", pieces.len(), inner.rx.remote_ip());

    with_ctx!(|ctx| for piece in pieces.clone() {
        ctx.pending_requests.add(piece, inner.rx.remote_ip())
    });

    let received_ever = inner.state.bytes_received > 0;
    let peer_reqq = with_ctx!(|ctx| ctrl::get_peer_reqq(inner.rx.remote_ip(), ctx));

    let (piece_sink, piece_src) = local_mpsc::channel::<usize>();
    let (block_sink, block_src) = local_mpsc::channel::<pwp::BlockInfo>();
    let (reqq_slot_sink, reqq_slot_src) = local_mpsc::semaphore(peer_reqq);

    try_join!(
        request_pieces(
            pieces.clone(),
            inner.handle.clone(),
            &mut inner.tx,
            reqq_slot_src,
            block_src,
            received_ever
        ),
        receive_pieces(
            pieces,
            inner.handle.clone(),
            &mut inner.state,
            &mut inner.rx,
            &inner.storage,
            reqq_slot_sink,
            block_sink,
            piece_sink
        ),
        verify_pieces(
            inner.handle.clone(),
            &inner.storage,
            &inner.piece_downloaded_channel,
            piece_src,
            &mut inner.verified_pieces,
        )
    )?;

    update_ctx!(inner);
    Ok(to_enum!(inner))
}

const BLOCK_TIMEOUT: Duration = sec!(20);

fn divide_piece_into_blocks(
    piece_index: usize,
    piece_len: usize,
) -> impl Iterator<Item = pwp::BlockInfo> {
    (0..piece_len)
        .step_by(pwp::MAX_BLOCK_SIZE)
        .map(move |in_piece_offset| pwp::BlockInfo {
            piece_index,
            in_piece_offset,
            block_length: cmp::min(pwp::MAX_BLOCK_SIZE, piece_len - in_piece_offset),
        })
}

macro_rules! wait_or_resend_requests {
    ($signal:expr, $tx:expr, $received_blocks_ever:expr, $requests_to_resend:expr) => {{
        // wait for a slot in the request queue, resend all pending requests if no block has been received in 20s
        let mut retries_left = if $received_blocks_ever { 2 } else { 1 };
        loop {
            match time::timeout(BLOCK_TIMEOUT, $signal).await {
                Ok(result) => break Ok(result),
                Err(_timeout) if retries_left > 0 => {
                    assert!(!$requests_to_resend.is_empty());
                    log::warn!("Re-sending block requests to {}", $tx.remote_ip());
                    for block in $requests_to_resend {
                        $tx.send_message(pwp::DownloaderMessage::Request(block.clone())).await?;
                    }
                    retries_left -= 1;
                }
                Err(_timeout) => {
                    let error_kind = match $received_blocks_ever {
                        true => io::ErrorKind::TimedOut,
                        false => io::ErrorKind::Other, // don't try to reconnect
                    };
                    break Err(io::Error::new(
                        error_kind,
                        format!("peer failed to respond to requests within {BLOCK_TIMEOUT:?}"),
                    ));
                }
            }
        }
    }};
}

async fn request_pieces(
    pieces: impl Iterator<Item = usize>,
    mut handle: CtxHandle,
    tx: &mut pwp::DownloadTxChannel,
    mut reqq_slot_signal: local_mpsc::Waiter,
    mut block_received_signal: local_mpsc::Receiver<pwp::BlockInfo>,
    received_blocks_ever: bool,
) -> io::Result<()> {
    define_with_ctx!(handle);
    let mut sent_requests = HashSet::<pwp::BlockInfo>::new();

    // send out block requests, respecting peer's reqq
    let piece_info = with_ctx!(|ctx| ctx.pieces.clone());
    for block in pieces
        .flat_map(|piece| divide_piece_into_blocks(piece, piece_info.piece_len(piece)))
        .filter(|block| with_ctx!(|ctx| !ctx.accountant.has_exact_block(block)))
    {
        let received_ever = received_blocks_ever || block_received_signal.has_pending_data();
        let reqq_notifier_alive = wait_or_resend_requests!(
            reqq_slot_signal.acquire_one(),
            tx,
            received_ever,
            &sent_requests
        )?;
        if !reqq_notifier_alive {
            // the receive task has exited, which means either the peer chokes us, or
            // we've already received all pieces (from other peers)
            break;
        }
        sent_requests.insert(block.clone());
        tx.send_message(pwp::DownloaderMessage::Request(block)).await?;
    }
    // wait for the receive task to exit, re-send or cancel requests if necessary
    let mut received_ever = received_blocks_ever || block_received_signal.has_pending_data();
    while !sent_requests.is_empty() {
        match wait_or_resend_requests!(
            block_received_signal.next(),
            tx,
            received_ever,
            &sent_requests
        )? {
            Some(received_block) => {
                sent_requests.remove(&received_block);
                received_ever = true;
            }
            None => {
                // the receive task has exited, which means either the peer chokes us, or
                // we've already received all pieces (from other peers)

                // // TODO: cancel outstanding requests unless peer chokes
                // for block in sent_requests {
                //     tx.send_message(pwp::DownloaderMessage::Cancel(block)).await?;
                // }
                break;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn receive_pieces(
    pieces: impl Iterator<Item = usize>,
    mut handle: CtxHandle,
    state: &mut pwp::DownloadState,
    rx: &mut pwp::DownloadRxChannel,
    storage: &data::StorageClient,
    reqq_slot_reporter: local_mpsc::Notifier,
    block_reporter: local_mpsc::Sender<pwp::BlockInfo>,
    piece_reporter: local_mpsc::Sender<usize>,
) -> io::Result<()> {
    define_with_ctx!(handle);
    let mut expected_pieces = pieces.collect::<HashSet<_>>();
    let mut speed_measurer = bandwidth::BitrateGauge::new();
    while !expected_pieces.is_empty() {
        match rx.receive_message().await? {
            pwp::UploaderMessage::Block(info, data) => {
                if info.block_length > pwp::MAX_BLOCK_SIZE
                    || info.in_piece_offset % pwp::MAX_BLOCK_SIZE != 0
                {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("received invalid block {info}"),
                    ));
                }
                if expected_pieces.contains(&info.piece_index) {
                    reqq_slot_reporter.signal_one();
                    let global_offset = with_ctx!(|ctx| ctx.accountant.submit_block(&info))
                        .map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!("Received invalid block {info}"),
                            )
                        })?;
                    block_reporter.send(info.clone());
                    let bytes_received = data.len();
                    storage.start_write_block(global_offset, data).unwrap_or_else(|e| {
                        panic!("Failed to start write ({info}) to storage: {e}")
                    });
                    state.bytes_received += bytes_received;
                    state.last_bitrate_bps = speed_measurer.update(bytes_received).get_bps();
                    with_ctx!(|ctx| ctx.peer_states.update_download(rx.remote_ip(), state));
                    if with_ctx!(|ctx| ctx.accountant.has_piece(info.piece_index)) {
                        piece_reporter.send(info.piece_index);
                        expected_pieces.remove(&info.piece_index);
                    }
                } else {
                    // This can be a canceled block, so don't disconnect peer
                    log::debug!("Received unexpected block ({info}) from {}", rx.remote_ip());
                }
            }
            msg => {
                if update_state_with_msg(&mut handle, state, rx.remote_ip(), &msg)
                    && state.peer_choking
                {
                    with_ctx!(|ctx| ctx.pending_requests.clear_requests_to(rx.remote_ip()));
                    break;
                }
            }
        }
    }
    state.last_bitrate_bps = speed_measurer.get_bps();
    with_ctx!(|ctx| ctx.peer_states.update_download(rx.remote_ip(), state));
    Ok(())
}

async fn verify_pieces(
    mut handle: CtxHandle,
    storage: &data::StorageClient,
    progress_reporter: &broadcast::Sender<usize>,
    mut downloaded_pieces: local_mpsc::Receiver<usize>,
    verified_pieces: &mut usize,
) -> io::Result<()> {
    define_with_ctx!(handle);
    while let Some(piece_index) = downloaded_pieces.next().await {
        let piece_len = with_ctx!(|ctx| ctx.pieces.piece_len(piece_index));
        let global_offset = with_ctx!(|ctx| ctx.pieces.global_offset(piece_index, 0, piece_len))
            .expect("Requested (and received!) invalid piece index");
        let expected_sha1 = with_ctx!(|ctx| -> Option<[u8; 20]> {
            let mut buf = [0u8; 20];
            buf.copy_from_slice(ctx.metainfo.pieces()?.nth(piece_index)?);
            Some(buf)
        })
        .expect("Requested (and received!) invalid piece index");
        let verification_success =
            storage.verify_block(global_offset, piece_len, &expected_sha1).await?;
        with_ctx!(|ctx| ctx.pending_requests.clear_requests_of(piece_index));
        if verification_success {
            *verified_pieces += 1;
            with_ctx!(|ctx| ctx.piece_tracker.forget_piece(piece_index));
            let _ = progress_reporter.send(piece_index).inspect_err(|e| {
                log::warn!("Failed to broadcast downloaded piece {piece_index}: {e}")
            });
        } else {
            log::error!("Piece verification failed, piece_index={piece_index}");
            with_ctx!(|ctx| ctx.accountant.remove_piece(piece_index));
            if *verified_pieces == 0 {
                return Err(io::Error::new(io::ErrorKind::Other, "piece verification failed"));
            }
        }
    }
    io::Result::Ok(())
}

fn update_state_with_msg(
    handle: &mut CtxHandle,
    state: &mut pwp::DownloadState,
    ip: &SocketAddr,
    msg: &pwp::UploaderMessage,
) -> bool {
    match msg {
        pwp::UploaderMessage::Unchoke => {
            if state.peer_choking {
                state.peer_choking = false;
                true
            } else {
                false
            }
        }
        pwp::UploaderMessage::Have { piece_index } => {
            log::trace!("Received Have({piece_index}) from {ip}");
            handle.with_ctx(|ctx| ctx.piece_tracker.add_single_record(ip, *piece_index));
            true
        }
        pwp::UploaderMessage::Bitfield(bitfield) => {
            if log::log_enabled!(log::Level::Trace) {
                let remote_piece_count = bitfield.count_ones();
                log::trace!("Received bitfield from {ip}: peer has {remote_piece_count} pieces");
            }
            handle.with_ctx(|ctx| ctx.piece_tracker.add_bitfield_record(ip, bitfield));
            true
        }
        pwp::UploaderMessage::Choke => {
            if !state.peer_choking {
                state.peer_choking = true;
                true
            } else {
                false
            }
        }
        pwp::UploaderMessage::Block(_, _) => false,
    }
}
