use crate::ops::{ctrl, ctx};
use crate::utils::{bandwidth, local_mpsc, sealed};
use crate::{data, debug_stopwatch, min, pwp, sec, trace_stopwatch};
use futures::prelude::*;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use std::{cmp, io, iter};
use tokio::sync::broadcast;
use tokio::time::{self, Instant};
use tokio::{select, try_join};

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

pub async fn get_pieces(peer: SeedingPeer) -> io::Result<Peer> {
    let mut inner = peer.0;
    debug_assert!(inner.state.am_interested && !inner.state.peer_choking);
    define_with_ctx!(inner.handle);
    let _sw = debug_stopwatch!("Download from {}", inner.rx.remote_ip());

    let received_ever = inner.state.bytes_received > 0;
    let peer_reqq = with_ctx!(|ctx| ctrl::get_peer_reqq(inner.rx.remote_ip(), ctx));

    let requests_in_flight = sealed::Set::with_capacity(peer_reqq);
    let (piece_sink, piece_src) = local_mpsc::channel::<usize>();
    let (reqq_slot_sink, reqq_slot_src) = local_mpsc::semaphore(peer_reqq);

    try_join!(
        async {
            select! {
                biased;
                request_result = request_pieces(
                    inner.handle.clone(),
                    &mut inner.tx,
                    received_ever,
                    reqq_slot_src,
                    &requests_in_flight,
                ) => request_result,
                receive_result = receive_pieces(
                    inner.handle.clone(),
                    &mut inner.rx,
                    &mut inner.state,
                    &inner.storage,
                    reqq_slot_sink,
                    piece_sink,
                    &requests_in_flight,
                ) => receive_result,
            }
        },
        verify_pieces(
            inner.handle.clone(),
            &inner.storage,
            &inner.piece_downloaded_channel,
            piece_src,
            &mut inner.verified_pieces,
        )
    )?;
    debug_assert!(requests_in_flight.is_empty());
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

async fn wait_with_retries(
    signal: &mut local_mpsc::Waiter,
    tx: &mut pwp::DownloadTxChannel,
    received_data_before: bool,
    requests_to_resend: &(impl IntoIterator<Item = pwp::BlockInfo> + Clone),
) -> io::Result<bool> {
    // wait for signal, resend all pending requests if the signal times out
    let mut retries_left = if received_data_before { 2 } else { 1 };
    loop {
        match time::timeout(BLOCK_TIMEOUT, signal.acquire_one()).await {
            Ok(result) => break Ok(result),
            Err(_timeout) if retries_left > 0 => {
                let mut resent_requests = 0usize;
                for block in requests_to_resend.clone() {
                    tx.send_message(pwp::DownloaderMessage::Request(block)).await?;
                    resent_requests += 1;
                }
                assert!(resent_requests > 0);
                log::warn!("Re-sent {} block requests to {}", resent_requests, tx.remote_ip());
                retries_left -= 1;
            }
            Err(_timeout) => {
                let error_kind = match received_data_before {
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
}

async fn request_pieces(
    mut handle: CtxHandle,
    tx: &mut pwp::DownloadTxChannel,
    received_blocks_ever: bool,
    mut reqq_slot_signal: local_mpsc::Waiter,
    requests_in_flight: &sealed::Set<pwp::BlockInfo>,
) -> io::Result<()> {
    define_with_ctx!(handle);
    let _sw = trace_stopwatch!("Requesting pieces from {}", tx.remote_ip());
    let mut request_count = 0usize;

    debug_assert!(with_ctx!(|ctx| ctrl::next_piece_to_request(tx.remote_ip(), ctx).is_some()));

    let piece_info = with_ctx!(|ctx| ctx.pieces.clone());
    let peer_ip = *tx.remote_ip();

    let request_generator = || {
        with_ctx!(|ctx| ctrl::next_piece_to_request(&peer_ip, ctx)
            .inspect(|&piece| ctx.pending_requests.add(piece, &peer_ip)))
    };
    for block in iter::from_fn(request_generator)
        .flat_map(|piece| divide_piece_into_blocks(piece, piece_info.piece_len(piece)))
    {
        let reqq_notifier_alive = wait_with_retries(
            &mut reqq_slot_signal,
            tx,
            received_blocks_ever || requests_in_flight.len() < request_count,
            requests_in_flight,
        )
        .await?;
        if !reqq_notifier_alive {
            // the receive task has exited, which means the peer started choking
            break;
        }
        requests_in_flight.insert(block.clone());
        tx.send_message(pwp::DownloaderMessage::Request(block)).await?;
        request_count += 1;
    }
    // wait until all requested pieces have been received, retry if necessary
    reqq_slot_signal.drain();
    while !requests_in_flight.is_empty() {
        let reqq_notifier_alive = wait_with_retries(
            &mut reqq_slot_signal,
            tx,
            received_blocks_ever || requests_in_flight.len() < request_count,
            requests_in_flight,
        )
        .await?;
        if !reqq_notifier_alive {
            // the receive task has exited, which means the peer started choking
            break;
        }
    }
    Ok(())
}

async fn receive_pieces(
    mut handle: CtxHandle,
    rx: &mut pwp::DownloadRxChannel,
    state: &mut pwp::DownloadState,
    storage: &data::StorageClient,
    reqq_slot_reporter: local_mpsc::Notifier,
    verification_channel: local_mpsc::Sender<usize>,
    requests_in_flight: &sealed::Set<pwp::BlockInfo>,
) -> io::Result<()> {
    define_with_ctx!(handle);
    let _sw = trace_stopwatch!("Receiving pieces from {}", rx.remote_ip());
    let mut speed_measurer = bandwidth::BitrateGauge::new();

    loop {
        match rx.receive_message().await? {
            pwp::UploaderMessage::Block(info, data) if requests_in_flight.remove(&info) => {
                // update state
                state.bytes_received += data.len();
                state.last_bitrate_bps = speed_measurer.update(data.len()).get_bps();
                with_ctx!(|ctx| ctx.peer_states.update_download(rx.remote_ip(), state));
                // submit the block if needed
                if with_ctx!(|ctx| !ctx.accountant.has_exact_block(&info)) {
                    let global_offset = with_ctx!(|ctx| ctx.accountant.submit_block(&info))
                        .unwrap_or_else(|e| panic!("Requested invalid block {info}: {e}"));
                    storage.start_write_block(global_offset, data).unwrap_or_else(|e| {
                        panic!("Failed to start write ({info}) to storage: {e}")
                    });
                    if with_ctx!(|ctx| ctx.accountant.has_piece(info.piece_index)) {
                        verification_channel.send(info.piece_index);
                    }
                }
                // notify the request task
                reqq_slot_reporter.signal_one();
            }
            msg => {
                if update_state_with_msg(&mut handle, state, rx.remote_ip(), &msg)
                    && state.peer_choking
                {
                    requests_in_flight.clear();
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
    let _sw = trace_stopwatch!("Verifying pieces");

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
    Ok(())
}

fn update_state_with_msg(
    handle: &mut CtxHandle,
    state: &mut pwp::DownloadState,
    ip: &SocketAddr,
    msg: &pwp::UploaderMessage,
) -> bool {
    match msg {
        pwp::UploaderMessage::Unchoke => {
            log::trace!("Received Unchoke from {ip}");
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
            log::trace!("Received Choke from {ip}");
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
