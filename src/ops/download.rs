use super::{ctx, MAX_BLOCK_SIZE};
use crate::utils::fifo;
use crate::{data, debug_stopwatch, info_stopwatch, pwp, sec};
use futures::prelude::*;
use std::collections::HashSet;
use std::time::Duration;
use std::{cmp, io};
use tokio::time::Instant;
use tokio::try_join;

struct Data {
    handle: ctx::Handle,
    rx: pwp::DownloadRxChannel,
    tx: pwp::DownloadTxChannel,
    storage: data::StorageClient,
    state: pwp::DownloadState,
}

impl Drop for Data {
    fn drop(&mut self) {
        self.handle.with_ctx(|ctx| {
            ctx.piece_tracker.forget_peer(self.rx.remote_ip());
            ctx.peer_states.remove_peer(self.rx.remote_ip());
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

macro_rules! update_state {
    ($inner:expr) => {
        $inner
            .handle
            .with_ctx(|ctx| ctx.peer_states.update_download($inner.rx.remote_ip(), &$inner.state));
    };
}

pub(super) async fn new_peer(
    handle: ctx::Handle,
    rx: pwp::DownloadRxChannel,
    tx: pwp::DownloadTxChannel,
    storage: data::StorageClient,
) -> io::Result<IdlePeer> {
    let mut inner = Box::new(Data {
        handle,
        rx,
        tx,
        storage,
        state: Default::default(),
    });
    // try wait for bitfield and some have's
    const SETTLING_PERIOD: Duration = sec!(5);
    let mut now = Instant::now();
    let end_time = now + SETTLING_PERIOD;
    while now < end_time {
        match inner.rx.receive_message_timed(end_time - now).await {
            Ok(msg) => {
                update_state_with_msg(&mut inner, &msg);
            }
            Err(pwp::ChannelError::Timeout) => (),
            Err(e) => return Err(e.into()),
        }
        now = Instant::now();
    }
    update_state!(inner);
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
        update_state!(inner);
        Ok(SeedingPeer(inner))
    } else {
        let mut peer = to_enum!(inner);
        loop {
            peer = linger(peer, Duration::MAX).await?;
            if let Peer::Seeder(seeder) = peer {
                break Ok(seeder);
            }
        }
    }
}

pub async fn deactivate(peer: SeedingPeer) -> io::Result<IdlePeer> {
    let mut inner = peer.0;
    debug_assert!(inner.state.am_interested && !inner.state.peer_choking);
    inner.tx.send_message(pwp::DownloaderMessage::NotInterested).await?;
    inner.state.am_interested = false;
    update_state!(inner);
    Ok(IdlePeer(inner))
}

pub async fn remove_interest(peer: IdlePeer) -> io::Result<IdlePeer> {
    let mut inner = peer.0;
    debug_assert!(inner.state.am_interested);
    inner.tx.send_message(pwp::DownloaderMessage::NotInterested).await?;
    inner.state.am_interested = false;
    update_state!(inner);
    Ok(IdlePeer(inner))
}

pub async fn linger(peer: Peer, timeout: Duration) -> io::Result<Peer> {
    let mut inner = inner!(peer);
    let start_time = Instant::now();
    let mut state_changed = false;
    while !state_changed && start_time.elapsed() < timeout {
        match inner.rx.receive_message_timed(timeout - start_time.elapsed()).await {
            Ok(msg) => {
                state_changed = update_state_with_msg(&mut inner, &msg);
                if matches!(msg, pwp::UploaderMessage::Block(_, _)) {
                    log::warn!("{} Received not requested block", inner.rx.remote_ip());
                }
            }
            Err(pwp::ChannelError::Timeout) => (),
            Err(e) => return Err(e.into()),
        }
    }
    update_state!(inner);
    Ok(to_enum!(inner))
}

pub async fn get_pieces(
    peer: SeedingPeer,
    pieces: impl Iterator<Item = usize>,
) -> io::Result<Peer> {
    let mut inner = peer.0;
    debug_assert!(inner.state.am_interested && !inner.state.peer_choking);

    fn divide_piece_into_blocks(
        piece_index: usize,
        piece_len: usize,
    ) -> impl Iterator<Item = pwp::BlockInfo> {
        (0..piece_len)
            .step_by(MAX_BLOCK_SIZE)
            .map(move |in_piece_offset| pwp::BlockInfo {
                piece_index,
                in_piece_offset,
                block_length: cmp::min(MAX_BLOCK_SIZE, piece_len - in_piece_offset),
            })
    }

    let (piece_sink, piece_src) = fifo::channel::<usize>();

    let mut handle = inner.handle.clone();
    let storage = inner.storage.clone();

    let verify_pieces = async {
        let mut downloaded_pieces = piece_src;
        while let Some(piece_index) = downloaded_pieces.next().await {
            let _sw = debug_stopwatch!("Verification of piece {}", piece_index);
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
                log::debug!("Piece verified successfully, piece_index={piece_index}");
                handle.with_ctx(|ctx| ctx.piece_tracker.forget_piece(piece_index));
                // TODO: send Have to everyone else
            } else {
                log::error!("Piece verification failed, piece_index={piece_index}");
                handle.with_ctx(|ctx| ctx.accountant.remove_piece(piece_index));
                return Err(io::Error::new(io::ErrorKind::Other, "Piece verification failed"));
            }
        }
        io::Result::Ok(())
    };

    let download_pieces = async {
        let mut handle = inner.handle.clone();
        let verification_channel = piece_sink; // move so that it's dropped at the end
        for piece_index in pieces {
            let received_and_verified =
                handle.with_ctx(|ctx| ctx.piece_tracker.get_piece_owners(piece_index).is_none());
            if received_and_verified {
                // piece already downloaded from another peer
                continue;
            }
            let _sw =
                info_stopwatch!("Download of piece {} from {}", piece_index, inner.tx.remote_ip());
            let piece_len = inner.handle.with_ctx(|ctx| ctx.pieces.piece_len(piece_index));
            let mut requests: HashSet<pwp::BlockInfo> =
                divide_piece_into_blocks(piece_index, piece_len).collect();
            for block in &requests {
                inner.tx.send_message(pwp::DownloaderMessage::Request(block.clone())).await?;
            }
            while !inner.handle.with_ctx(|ctx| ctx.accountant.has_piece(piece_index)) {
                let msg = inner.rx.receive_message().await?;
                update_state_with_msg(&mut inner, &msg);
                if inner.state.peer_choking {
                    return io::Result::Ok(());
                }
                if let pwp::UploaderMessage::Block(info, data) = msg {
                    if let Ok(global_offset) =
                        inner.handle.with_ctx(|ctx| ctx.accountant.submit_block(&info))
                    {
                        inner.state.bytes_received += data.len();
                        inner.storage.start_write_block(global_offset, data).unwrap_or_else(|e| {
                            panic!("Failed to start write ({info}) to storage: {e}")
                        });
                        requests.remove(&info);
                        update_state!(inner);
                    } else {
                        log::error!(
                            "Received invalid block ({info}) from {}",
                            inner.rx.remote_ip()
                        );
                        // TODO: disconnect peer?
                    }
                }
            }
            verification_channel.send(piece_index);
            if !requests.is_empty() {
                // piece (or parts of it) received from another peer
                for pending_request in requests {
                    inner.tx.send_message(pwp::DownloaderMessage::Cancel(pending_request)).await?;
                }
            }
        }
        io::Result::Ok(())
    };

    try_join!(verify_pieces, download_pieces)?;
    update_state!(inner);
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
            inner
                .handle
                .with_ctx(|ctx| ctx.piece_tracker.add_single_record(ip, *piece_index));
            true
        }
        pwp::UploaderMessage::Bitfield(bitfield) => {
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
