use super::super::ctx;
use super::LOCAL_REQQ;
use futures_util::StreamExt;
use local_async_utils::prelude::*;
use mtorrent_core::{data, pwp};
use mtorrent_utils::bandwidth;
use mtorrent_utils::{debug_stopwatch, info_stopwatch};
use std::io;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::Instant;
use tokio::try_join;

type CtxHandle = ctx::Handle<ctx::MainCtx>;

struct Data {
    handle: CtxHandle,
    rx: pwp::UploadRxChannel,
    tx: pwp::UploadTxChannel,
    storage: data::StorageClient,
    state: pwp::UploadState,
}

impl Drop for Data {
    fn drop(&mut self) {
        self.handle.with(|ctx| {
            ctx.piece_tracker.forget_peer(self.tx.remote_ip());
            ctx.peer_states.remove_peer(self.tx.remote_ip());
        });
    }
}

pub struct IdlePeer(Box<Data>);

pub struct LeechingPeer(Box<Data>);

pub enum Peer {
    Idle(IdlePeer),
    Leech(LeechingPeer),
}

impl From<IdlePeer> for Peer {
    fn from(value: IdlePeer) -> Self {
        Peer::Idle(value)
    }
}

impl From<LeechingPeer> for Peer {
    fn from(value: LeechingPeer) -> Self {
        Peer::Leech(value)
    }
}

pub struct AvailabilityReporter {
    tx: pwp::UploadTxChannel,
    piece_downloaded_channel: broadcast::Receiver<usize>,
    reported_pieces: pwp::Bitfield,
}

impl AvailabilityReporter {
    pub async fn run(mut self) -> io::Result<()> {
        loop {
            match self.piece_downloaded_channel.recv().await {
                Ok(downloaded_piece) => {
                    let mut reported = self
                        .reported_pieces
                        .get_mut(downloaded_piece)
                        .expect("Piece count mismatch");
                    if reported == false {
                        reported.set(true);
                        self.tx
                            .send_message(pwp::UploaderMessage::Have {
                                piece_index: downloaded_piece,
                            })
                            .await?;
                    }
                }
                Err(RecvError::Lagged(skipped)) => {
                    log::warn!(
                        "Failed to notify {} about {} downloaded pieces",
                        self.tx.remote_ip(),
                        skipped
                    );
                    debug_assert!(false, "AvailabilityReporter overflow");
                }
                Err(RecvError::Closed) => break,
            }
        }
        Ok(())
    }
}

macro_rules! to_enum {
    ($inner:expr) => {
        if $inner.state.peer_interested && !$inner.state.am_choking {
            LeechingPeer($inner).into()
        } else {
            IdlePeer($inner).into()
        }
    };
}

macro_rules! update_ctx {
    ($inner:expr) => {
        $inner
            .handle
            .with(|ctx| ctx.peer_states.update_upload($inner.tx.remote_ip(), &$inner.state));
    };
}

macro_rules! update_state_with_msg {
    ($inner:expr, $msg:expr) => {
        match $msg {
            pwp::DownloaderMessage::Interested => {
                if !$inner.state.peer_interested {
                    $inner.state.peer_interested = true;
                    true
                } else {
                    false
                }
            }
            pwp::DownloaderMessage::NotInterested => {
                if $inner.state.peer_interested {
                    $inner.state.peer_interested = false;
                    true
                } else {
                    false
                }
            }
            pwp::DownloaderMessage::Request(_) => false,
            pwp::DownloaderMessage::Cancel(_) => false,
        }
    };
}

pub async fn new_peer(
    mut handle: CtxHandle,
    rx: pwp::UploadRxChannel,
    tx: pwp::UploadTxChannel,
    storage: data::StorageClient,
    piece_downloaded_channel: broadcast::Receiver<usize>,
) -> io::Result<(IdlePeer, AvailabilityReporter)> {
    let bitfield = handle.with(|ctx| ctx.accountant.generate_bitfield());
    let mut inner = Box::new(Data {
        handle,
        rx,
        tx,
        storage,
        state: Default::default(),
    });
    if bitfield.any() {
        inner.tx.send_message(pwp::UploaderMessage::Bitfield(bitfield.clone())).await?;
    }
    update_ctx!(inner);
    let reporter = AvailabilityReporter {
        tx: inner.tx.clone(),
        piece_downloaded_channel,
        reported_pieces: bitfield,
    };
    Ok((IdlePeer(inner), reporter))
}

pub async fn activate(peer: IdlePeer) -> io::Result<LeechingPeer> {
    let mut inner = peer.0;
    debug_assert!(inner.state.am_choking || !inner.state.peer_interested);
    if inner.state.am_choking {
        inner.tx.send_message(pwp::UploaderMessage::Unchoke).await?;
        inner.state.am_choking = false;
    }
    if inner.state.peer_interested {
        update_ctx!(inner);
        Ok(LeechingPeer(inner))
    } else {
        let mut peer = IdlePeer(inner);
        loop {
            match linger(peer, Duration::MAX).await? {
                Peer::Idle(idle) => peer = idle,
                Peer::Leech(leech) => break Ok(leech),
            }
        }
    }
}

pub async fn deactivate(peer: LeechingPeer) -> io::Result<IdlePeer> {
    let mut inner = peer.0;
    debug_assert!(inner.state.peer_interested && !inner.state.am_choking);
    inner.tx.send_message(pwp::UploaderMessage::Choke).await?;
    inner.state.am_choking = true;
    update_ctx!(inner);
    Ok(IdlePeer(inner))
}

pub async fn linger(peer: IdlePeer, timeout: Duration) -> io::Result<Peer> {
    let mut inner = peer.0;
    debug_assert!(inner.state.am_choking || !inner.state.peer_interested);
    let start_time = Instant::now();
    loop {
        match inner
            .rx
            .receive_message_timed(timeout.saturating_sub(start_time.elapsed()))
            .await
        {
            Ok(msg) => {
                if update_state_with_msg!(&mut inner, &msg) {
                    break;
                }
                match msg {
                    pwp::DownloaderMessage::Request(_) => {
                        log::debug!("Ignoring request from {} while idle", inner.rx.remote_ip())
                    }
                    pwp::DownloaderMessage::Cancel(_) => {
                        log::debug!("Ignoring cancel from {} while idle", inner.rx.remote_ip())
                    }
                    _ => (),
                }
            }
            Err(pwp::ChannelError::Timeout) => break,
            Err(e) => return Err(e.into()),
        }
    }
    update_ctx!(inner);
    Ok(to_enum!(inner))
}

pub async fn serve_pieces(peer: LeechingPeer, min_duration: Duration) -> io::Result<Peer> {
    let mut inner = peer.0;
    debug_assert!(!inner.state.am_choking && inner.state.peer_interested);
    define_with_ctx!(inner.handle);
    let _sw = info_stopwatch!("Serving pieces to {}", inner.tx.remote_ip());

    let (request_sink, request_src) = local_channel::channel::<pwp::BlockInfo>();
    let mut state_copy = inner.state.clone();

    let mut discarded_requests = 0u64;
    let collect_requests = async {
        let request_sink = request_sink; // move it, so that it's dropped at the end
        let start_time = Instant::now();
        loop {
            match inner
                .rx
                .receive_message_timed(min_duration.saturating_sub(start_time.elapsed()))
                .await
            {
                Ok(pwp::DownloaderMessage::Request(info)) => {
                    if !request_sink.try_send(LOCAL_REQQ, info) {
                        discarded_requests = discarded_requests.saturating_add(1);
                    }
                }
                Ok(pwp::DownloaderMessage::Cancel(info)) => {
                    request_sink.remove_all(&info);
                }
                Ok(msg) => {
                    update_state_with_msg!(inner, msg);
                    if !inner.state.peer_interested {
                        return Ok(());
                    }
                }
                Err(pwp::ChannelError::Timeout) => break,
                Err(e) => return Err(io::Error::from(e)),
            }
        }
        Ok(())
    };

    let mut speed_measurer = bandwidth::BitrateGauge::new();
    let process_requests = async {
        let mut request_src = request_src;
        let remote_ip = *inner.tx.remote_ip();
        while let Some(request) = request_src.next().await {
            let _sw = debug_stopwatch!("Serving request {} to {}", request, remote_ip);
            if request.block_length > pwp::MAX_BLOCK_SIZE {
                return Err(io::Error::other(format!("received too big request: {request}")));
            }
            let global_offset = with_ctx!(|ctx| {
                ctx.pieces.global_offset(
                    request.piece_index,
                    request.in_piece_offset,
                    request.block_length,
                )
            })
            .map_err(|_| io::Error::other(format!("received invalid request: {request}")))?;
            if !with_ctx!(|ctx| {
                ctx.accountant.has_exact_block_at(global_offset, request.block_length)
            }) {
                log::warn!("{} requested unavailable block {}", inner.tx.remote_ip(), request);
                continue;
            }
            let data = inner.storage.read_block(global_offset, request.block_length).await?;
            let length = data.len();
            inner.tx.send_message(pwp::UploaderMessage::Block(request, data)).await?;
            state_copy.bytes_sent += length;
            state_copy.last_bitrate_bps = speed_measurer.update(length).get_bps();
            with_ctx!(|ctx| ctx.peer_states.update_upload(&remote_ip, &state_copy));
        }
        Ok(())
    };

    let result = try_join!(collect_requests, process_requests);
    inner.state.bytes_sent = state_copy.bytes_sent;
    inner.state.last_bitrate_bps = speed_measurer.get_bps();
    if discarded_requests > 0 {
        log::warn!("Discarded {} requests from {}", discarded_requests, inner.tx.remote_ip());
    }
    result?;
    update_ctx!(inner);
    Ok(to_enum!(inner))
}
