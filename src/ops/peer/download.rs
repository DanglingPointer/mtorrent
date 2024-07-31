use crate::ops::{ctrl, ctx};
use crate::utils::{bandwidth, fifo};
use crate::{data, debug_stopwatch, min, pwp, sec};
use futures::prelude::*;
use std::collections::HashSet;
use std::rc::Rc;
use std::time::Duration;
use std::{cmp, io};
use tokio::sync::broadcast;
use tokio::time::Instant;
use tokio::try_join;

type CtxHandle = ctx::Handle<ctx::MainCtx>;

struct Data {
    handle: CtxHandle,
    rx: pwp::DownloadRxChannel,
    tx: pwp::DownloadTxChannel,
    storage: data::StorageClient,
    piece_downloaded_channel: Rc<broadcast::Sender<usize>>,
    state: pwp::DownloadState,
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
    });
    // try wait for bitfield
    match inner.rx.receive_message_timed(sec!(1)).await {
        Ok(msg) => {
            update_state_with_msg(&mut inner, &msg);
        }
        Err(pwp::ChannelError::Timeout) => (),
        Err(e) => return Err(e.into()),
    }
    // process queued msgs (e.g. Have's) if any
    loop {
        match inner.rx.receive_message_timed(sec!(0)).await {
            Ok(msg) => {
                update_state_with_msg(&mut inner, &msg);
            }
            Err(pwp::ChannelError::Timeout) => break,
            Err(e) => return Err(e.into()),
        }
    }
    update_ctx!(inner);
    Ok(IdlePeer(inner))
}

pub async fn activate(peer: IdlePeer) -> io::Result<SeedingPeer> {
    let mut inner = peer.0;
    debug_assert!(!inner.state.am_interested || inner.state.peer_choking);
    if !inner.state.am_interested {
        inner.tx.send_message(pwp::DownloaderMessage::Interested).await?;
        inner.state.am_interested = true;
    }
    if !inner.state.peer_choking {
        update_ctx!(inner);
        Ok(SeedingPeer(inner))
    } else {
        const RETRY_INTERVAL: Duration = min!(1);
        let max_retry_count = if inner.state.bytes_received == 0 {
            5
        } else {
            u32::MAX
        };

        let ip = *inner.tx.remote_ip();
        let mut peer = to_enum!(inner);

        for retry in 0..max_retry_count {
            let next_retry_at = Instant::now() + RETRY_INTERVAL;

            while Instant::now() < next_retry_at {
                peer = linger(peer, next_retry_at).await?;
                if let Peer::Seeder(seeder) = peer {
                    log::debug!("Activated download from {} after {} retries", ip, retry);
                    return Ok(seeder);
                }
            }
            log::debug!("Retrying to activate download from {}, attempt {}", ip, retry + 1);
            let inner = inner!(&mut peer);
            inner.tx.send_message(pwp::DownloaderMessage::NotInterested).await?;
            inner.tx.send_message(pwp::DownloaderMessage::Interested).await?;
        }
        Err(io::Error::new(io::ErrorKind::Other, "peer is parasite"))
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
                if update_state_with_msg(&mut inner, &msg) {
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

    fn divide_piece_into_request_batches(
        piece_index: usize,
        piece_len: usize,
        reqq: usize,
    ) -> impl Iterator<Item = impl Iterator<Item = pwp::BlockInfo>> {
        let max_bytes_in_flight = reqq * pwp::MAX_BLOCK_SIZE;
        (0..piece_len).step_by(max_bytes_in_flight).map(move |batch_start| {
            let batch_end = cmp::min(batch_start + max_bytes_in_flight, piece_len);
            (batch_start..batch_end)
                .step_by(pwp::MAX_BLOCK_SIZE)
                .map(move |in_piece_offset| pwp::BlockInfo {
                    piece_index,
                    in_piece_offset,
                    block_length: cmp::min(pwp::MAX_BLOCK_SIZE, piece_len - in_piece_offset),
                })
        })
    }

    macro_rules! wait_for_block {
        () => {{
            let timeout = match inner.state.bytes_received {
                0 => sec!(11),
                _ => sec!(31),
            };
            let deadline = Instant::now() + timeout;
            loop {
                let msg = inner.rx.receive_message_timed(deadline - Instant::now()).await.map_err(
                    |e| match e {
                        pwp::ChannelError::Timeout => io::Error::new(
                            io::ErrorKind::Other,
                            format!("peer failed to respond to requests within {:?}", timeout),
                        ),
                        pwp::ChannelError::ConnectionClosed => io::Error::from(e),
                    },
                )?;
                match msg {
                    pwp::UploaderMessage::Block(info, data) => break (info, data),
                    _ => {
                        if update_state_with_msg(&mut inner, &msg) && inner.state.peer_choking {
                            with_ctx!(|ctx| ctx
                                .pending_requests
                                .clear_requests_to(inner.rx.remote_ip()));
                            return io::Result::Ok(());
                        }
                    }
                }
            }
        }};
    }

    for piece in pieces.clone() {
        with_ctx!(|ctx| ctx.pending_requests.add(piece, inner.rx.remote_ip()));
    }

    let (piece_sink, piece_src) = fifo::channel::<usize>();

    let mut handle = inner.handle.clone();
    let storage = inner.storage.clone();
    let reporter = inner.piece_downloaded_channel.clone();

    let verify_pieces = async {
        let mut downloaded_pieces = piece_src;
        while let Some(piece_index) = downloaded_pieces.next().await {
            let piece_len = handle.with_ctx(|ctx| ctx.pieces.piece_len(piece_index));
            let global_offset = handle
                .with_ctx(|ctx| ctx.pieces.global_offset(piece_index, 0, piece_len))
                .expect("Requested (and received!) invalid piece index");
            let expected_sha1 = handle
                .with_ctx(|ctx| -> Option<[u8; 20]> {
                    let mut buf = [0u8; 20];
                    buf.copy_from_slice(ctx.metainfo.pieces()?.nth(piece_index)?);
                    Some(buf)
                })
                .expect("Requested (and received!) invalid piece index");
            let verification_success =
                storage.verify_block(global_offset, piece_len, &expected_sha1).await?;
            if verification_success {
                handle.with_ctx(|ctx| {
                    ctx.piece_tracker.forget_piece(piece_index);
                    ctx.pending_requests.clear_requests_of(piece_index);
                });
                let _ = reporter.send(piece_index).inspect_err(|e| {
                    log::warn!("Failed to broadcast downloaded piece {piece_index}: {e}")
                });
            } else {
                log::error!("Piece verification failed, piece_index={piece_index}");
                handle.with_ctx(|ctx| ctx.accountant.remove_piece(piece_index));
                return Err(io::Error::new(io::ErrorKind::Other, "piece verification failed"));
            }
        }
        io::Result::Ok(())
    };

    let download_pieces = async {
        let verification_channel = piece_sink; // move so that it's dropped at the end
        let mut speed_measurer = bandwidth::BitrateGauge::new();
        for piece_index in pieces {
            let received_and_verified =
                with_ctx!(|ctx| ctx.piece_tracker.get_piece_owners(piece_index).is_none());
            if received_and_verified {
                // piece already downloaded from another peer
                continue;
            }
            let (piece_len, peer_reqq) = with_ctx!(|ctx| (
                ctx.pieces.piece_len(piece_index),
                ctrl::get_peer_reqq(inner.rx.remote_ip(), ctx)
            ));
            for batch in divide_piece_into_request_batches(piece_index, piece_len, peer_reqq) {
                // send requests in bulks, up to `peer_reqq` blocks at a time
                let mut requests: HashSet<pwp::BlockInfo> = batch
                    .filter(|block| with_ctx!(|ctx| !ctx.accountant.has_exact_block(block)))
                    .collect();
                for block in &requests {
                    inner.tx.send_message(pwp::DownloaderMessage::Request(block.clone())).await?;
                }
                while !requests.is_empty()
                    && with_ctx!(|ctx| !ctx.accountant.has_piece(piece_index))
                {
                    let (info, data) = wait_for_block!();
                    if !requests.remove(&info) {
                        log::debug!(
                            "Received unexpected block ({info}) from {}",
                            inner.rx.remote_ip()
                        );
                        // This can be a canceled block, so don't disconnect peer
                        continue;
                    }
                    let global_offset = with_ctx!(|ctx| ctx.accountant.submit_block(&info))
                        .unwrap_or_else(|e| panic!("Requested invalid block {info} ({e})"));
                    let bytes_received = data.len();
                    storage.start_write_block(global_offset, data).unwrap_or_else(|e| {
                        panic!("Failed to start write ({info}) to storage: {e}")
                    });
                    inner.state.bytes_received += bytes_received;
                    inner.state.last_bitrate_bps = speed_measurer.update(bytes_received).get_bps();
                    update_ctx!(inner);
                }
                // piece (or parts of it) received from another peer
                for pending_request in requests {
                    inner.tx.send_message(pwp::DownloaderMessage::Cancel(pending_request)).await?;
                }
            }
            debug_assert!(with_ctx!(|ctx| ctx.accountant.has_piece(piece_index)));
            verification_channel.send(piece_index);
        }
        io::Result::Ok(())
    };

    try_join!(verify_pieces, download_pieces)?;
    update_ctx!(inner);
    Ok(to_enum!(inner))
}

fn update_state_with_msg(inner: &mut Data, msg: &pwp::UploaderMessage) -> bool {
    let ip = inner.rx.remote_ip();
    match msg {
        pwp::UploaderMessage::Unchoke => {
            if inner.state.peer_choking {
                inner.state.peer_choking = false;
                true
            } else {
                false
            }
        }
        pwp::UploaderMessage::Have { piece_index } => {
            log::trace!("Received Have({piece_index}) from {ip}");
            inner
                .handle
                .with_ctx(|ctx| ctx.piece_tracker.add_single_record(ip, *piece_index));
            true
        }
        pwp::UploaderMessage::Bitfield(bitfield) => {
            if log::log_enabled!(log::Level::Trace) {
                let remote_piece_count = bitfield.count_ones();
                log::trace!("Received bitfield from {ip}: peer has {remote_piece_count} pieces");
            }
            inner.handle.with_ctx(|ctx| ctx.piece_tracker.add_bitfield_record(ip, bitfield));
            true
        }
        pwp::UploaderMessage::Choke => {
            if !inner.state.peer_choking {
                inner.state.peer_choking = true;
                true
            } else {
                false
            }
        }
        pwp::UploaderMessage::Block(_, _) => false,
    }
}
