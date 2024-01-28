use super::{ctx, MAX_BLOCK_SIZE};
use crate::{data, pwp, sec};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use std::{cmp, io};
use tokio::time::Instant;

struct Data {
    handle: ctx::Handle,
    rx: pwp::DownloadRxChannel,
    tx: pwp::DownloadTxChannel,
    storage: Rc<data::StorageClient>,
    am_interested: bool,
    peer_choking: bool,
    bytes_received: usize,
}

impl Drop for Data {
    fn drop(&mut self) {
        self.handle.with_ctx(|ctx| ctx.piece_tracker.forget_peer(self.rx.remote_ip()));
    }
}

pub trait State {
    fn am_interested(&self) -> bool;
    fn peer_choking(&self) -> bool;
    fn bytes_received(&self) -> usize;
    fn ip(&self) -> &SocketAddr;
}

impl State for Data {
    fn am_interested(&self) -> bool {
        self.am_interested
    }

    fn peer_choking(&self) -> bool {
        self.peer_choking
    }

    fn bytes_received(&self) -> usize {
        self.bytes_received
    }

    fn ip(&self) -> &SocketAddr {
        self.rx.remote_ip()
    }
}

pub struct IdlePeer(Box<Data>);

pub struct SeedingPeer(Box<Data>);

impl IdlePeer {
    pub fn state(&self) -> &impl State {
        self.0.as_ref()
    }
}

impl SeedingPeer {
    pub fn state(&self) -> &impl State {
        self.0.as_ref()
    }
}

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
        if $inner.am_interested && !$inner.peer_choking {
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

pub async fn create_peer(
    handle: ctx::Handle,
    rx: pwp::DownloadRxChannel,
    tx: pwp::DownloadTxChannel,
    storage: Rc<data::StorageClient>,
) -> io::Result<IdlePeer> {
    let mut inner = Box::new(Data {
        handle,
        rx,
        tx,
        storage,
        am_interested: false,
        peer_choking: true,
        bytes_received: 0,
    });
    // try wait for bitfield and some have's
    const SETTLING_PERIOD: Duration = sec!(5);
    let mut now = Instant::now();
    let end_time = now + SETTLING_PERIOD;
    while now < end_time {
        match inner.rx.receive_message_timed(end_time - now).await {
            Ok(msg) => {
                update_state(&mut inner, &msg);
            }
            Err(pwp::ChannelError::Timeout) => (),
            Err(e) => return Err(e.into()),
        }
        now = Instant::now();
    }
    Ok(IdlePeer(inner))
}

pub async fn activate(peer: IdlePeer) -> io::Result<SeedingPeer> {
    let mut inner = peer.0;
    debug_assert!(!inner.am_interested || inner.peer_choking);
    if !inner.am_interested {
        inner.tx.send_message(pwp::DownloaderMessage::Interested).await?;
        inner.am_interested = true;
    }
    if !inner.peer_choking {
        Ok(SeedingPeer(inner))
    } else {
        let mut peer = to_enum!(inner);
        loop {
            peer = wait(peer, Duration::MAX).await?;
            if let Peer::Seeder(seeder) = peer {
                return Ok(seeder);
            }
        }
    }
}

pub async fn deactivate(peer: SeedingPeer) -> io::Result<IdlePeer> {
    let mut inner = peer.0;
    debug_assert!(inner.am_interested && !inner.peer_choking);
    inner.tx.send_message(pwp::DownloaderMessage::NotInterested).await?;
    inner.am_interested = false;
    Ok(IdlePeer(inner))
}

pub async fn remove_interest(peer: IdlePeer) -> io::Result<IdlePeer> {
    let mut inner = peer.0;
    debug_assert!(inner.am_interested);
    inner.tx.send_message(pwp::DownloaderMessage::NotInterested).await?;
    inner.am_interested = false;
    Ok(IdlePeer(inner))
}

pub async fn wait(peer: Peer, timeout: Duration) -> io::Result<Peer> {
    let mut inner = inner!(peer);
    let mut now = Instant::now();
    let end_time = now + timeout;
    let mut state_changed = false;
    while !state_changed && now < end_time {
        match inner.rx.receive_message_timed(end_time - now).await {
            Ok(msg) => {
                state_changed = update_state(&mut inner, &msg);
                if matches!(msg, pwp::UploaderMessage::Block(_, _)) {
                    log::warn!("{} Received not requested block", inner.rx.remote_ip());
                }
            }
            Err(pwp::ChannelError::Timeout) => (),
            Err(e) => return Err(e.into()),
        }
        now = Instant::now();
    }
    Ok(to_enum!(inner))
}

pub async fn get_pieces(
    peer: SeedingPeer,
    pieces: impl Iterator<Item = usize>,
) -> io::Result<Peer> {
    let mut inner = peer.0;
    debug_assert!(inner.am_interested && !inner.peer_choking);

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

    for piece_index in pieces {
        let received_and_verified = inner
            .handle
            .with_ctx(|ctx| ctx.piece_tracker.get_piece_owners(piece_index).is_none());
        if received_and_verified {
            // piece already downloaded from another peer
            continue;
        }
        let piece_len = inner.handle.with_ctx(|ctx| ctx.pieces.piece_len(piece_index));
        let mut requests: HashSet<pwp::BlockInfo> =
            divide_piece_into_blocks(piece_index, piece_len).collect();
        for block in &requests {
            inner.tx.send_message(pwp::DownloaderMessage::Request(block.clone())).await?;
        }
        while !inner.handle.with_ctx(|ctx| ctx.accountant.has_piece(piece_index)) {
            let msg = inner.rx.receive_message().await?;
            update_state(&mut inner, &msg);
            if inner.peer_choking {
                return Ok(to_enum!(inner));
            }
            if let pwp::UploaderMessage::Block(info, data) = msg {
                if let Ok(global_offset) =
                    inner.handle.with_ctx(|ctx| ctx.accountant.submit_block(&info))
                {
                    inner.bytes_received += data.len();
                    inner.storage.start_write_block(global_offset, data).unwrap_or_else(|e| {
                        panic!("Failed to start write ({info}) to storage: {e}")
                    });
                    requests.remove(&info);
                } else {
                    log::error!("Received invalid block ({info}) from {}", inner.rx.remote_ip());
                    // TODO: disconnect peer?
                }
            }
        }
        if !requests.is_empty() {
            // piece (or parts of it) received from another peer
            for pending_request in requests {
                inner.tx.send_message(pwp::DownloaderMessage::Cancel(pending_request)).await?;
            }
        }
        let global_offset = inner
            .handle
            .with_ctx(|ctx| ctx.pieces.global_offset(piece_index, 0, piece_len))
            .expect("Requested (and received!) invalid piece index");
        let expected_sha1 = inner
            .handle
            .with_ctx(|ctx| -> Option<[u8; 20]> {
                let mut buf = [0u8; 20];
                buf.copy_from_slice(ctx.metainfo.pieces()?.nth(piece_index)?);
                Some(buf)
            })
            .expect("Requested (and received!) invalid piece index");
        let verification_success =
            inner.storage.verify_block(global_offset, piece_len, &expected_sha1).await?;
        if verification_success {
            log::debug!("Piece verified successfully, piece_index={piece_index}");
            inner.handle.with_ctx(|ctx| ctx.piece_tracker.forget_piece(piece_index));
            // TODO: send Have to everyone else
        } else {
            inner.handle.with_ctx(|ctx| ctx.accountant.remove_piece(piece_index));
        }
    }
    Ok(to_enum!(inner))
}

fn update_state(inner: &mut Data, msg: &pwp::UploaderMessage) -> bool {
    let ip = inner.rx.remote_ip();
    match msg {
        pwp::UploaderMessage::Unchoke => {
            if inner.peer_choking {
                inner.peer_choking = false;
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
            if !inner.peer_choking {
                inner.peer_choking = true;
                true
            } else {
                false
            }
        }
        pwp::UploaderMessage::Block(_, _) => false,
    }
}
