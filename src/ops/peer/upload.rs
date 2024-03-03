use crate::ops::{ctx, MAX_BLOCK_SIZE};
use crate::utils::fifo;
use crate::{data, debug_stopwatch, info_stopwatch, pwp};
use futures::prelude::*;
use std::io;
use std::ops::BitXorAssign;
use std::time::Duration;
use tokio::time::Instant;
use tokio::try_join;

struct Data {
    handle: ctx::Handle,
    rx: pwp::UploadRxChannel,
    tx: pwp::UploadTxChannel,
    storage: data::StorageClient,
    state: pwp::UploadState,
    local_pieces_snapshot: pwp::Bitfield,
}

impl Drop for Data {
    fn drop(&mut self) {
        self.handle.with_ctx(|ctx| {
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

macro_rules! to_enum {
    ($inner:expr) => {
        if $inner.state.peer_interested && !$inner.state.am_choking {
            LeechingPeer($inner).into()
        } else {
            IdlePeer($inner).into()
        }
    };
}

macro_rules! inner {
    ($inner:expr) => {
        match $inner {
            Peer::Idle(IdlePeer(data)) => data,
            Peer::Leech(LeechingPeer(data)) => data,
        }
    };
}

macro_rules! update_state {
    ($inner:expr) => {
        $inner
            .handle
            .with_ctx(|ctx| ctx.peer_states.update_upload($inner.tx.remote_ip(), &$inner.state));
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
    mut handle: ctx::Handle,
    rx: pwp::UploadRxChannel,
    tx: pwp::UploadTxChannel,
    storage: data::StorageClient,
) -> io::Result<IdlePeer> {
    let bitfield = handle.with_ctx(|ctx| ctx.accountant.generate_bitfield());
    let mut inner = Box::new(Data {
        handle,
        rx,
        tx,
        storage,
        state: Default::default(),
        local_pieces_snapshot: bitfield.clone(),
    });
    if bitfield.iter().any(|bit| bit == true) {
        inner.tx.send_message(pwp::UploaderMessage::Bitfield(bitfield)).await?;
    }
    update_state!(inner);
    Ok(IdlePeer(inner))
}

pub async fn activate(peer: IdlePeer) -> io::Result<LeechingPeer> {
    let mut inner = peer.0;
    debug_assert!(inner.state.am_choking || !inner.state.peer_interested);
    if inner.state.am_choking {
        inner.tx.send_message(pwp::UploaderMessage::Unchoke).await?;
        inner.state.am_choking = false;
    }
    if inner.state.peer_interested {
        update_state!(inner);
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
    update_state!(inner);
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
                        log::warn!("Received request from {} while idle", inner.rx.remote_ip())
                    }
                    pwp::DownloaderMessage::Cancel(_) => {
                        log::warn!("Received cancel from {} while idle", inner.rx.remote_ip())
                    }
                    _ => (),
                }
            }
            Err(pwp::ChannelError::Timeout) => break,
            Err(e) => return Err(e.into()),
        }
    }
    update_state!(inner);
    Ok(to_enum!(inner))
}

pub async fn update_peer(peer: Peer) -> io::Result<Peer> {
    let mut inner = inner!(peer);
    let current_state = inner.handle.with_ctx(|ctx| ctx.accountant.generate_bitfield());
    // local pieces don't disappear, so xor yields all newly added pieces
    inner.local_pieces_snapshot.bitxor_assign(&current_state);
    for (piece_index, status_changed) in inner.local_pieces_snapshot.iter().enumerate() {
        if *status_changed {
            inner.tx.send_message(pwp::UploaderMessage::Have { piece_index }).await?;
        }
    }
    inner.local_pieces_snapshot = current_state;
    Ok(to_enum!(inner))
}

pub async fn serve_pieces(peer: LeechingPeer, min_duration: Duration) -> io::Result<Peer> {
    let mut inner = peer.0;
    let _sw = info_stopwatch!("Serving pieces to {}", inner.tx.remote_ip());
    debug_assert!(!inner.state.am_choking && inner.state.peer_interested);

    let (request_sink, request_src) = fifo::channel::<pwp::BlockInfo>();
    let mut initial_state = inner.state.clone();
    define_with_ctx!(inner.handle);

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
                    request_sink.send(info);
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

    let process_requests = async {
        let mut request_src = request_src;
        let remote_ip = *inner.tx.remote_ip();
        while let Some(request) = request_src.next().await {
            let _sw = debug_stopwatch!("Serving requet {} to {}", request, remote_ip);
            if request.block_length > MAX_BLOCK_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("received too big request: {request}"),
                ));
            }
            let global_offset = with_ctx!(|ctx| {
                ctx.pieces.global_offset(
                    request.piece_index,
                    request.in_piece_offset,
                    request.block_length,
                )
            })
            .map_err(|_| {
                io::Error::new(io::ErrorKind::Other, format!("received invalid request: {request}"))
            })?;
            if !with_ctx!(|ctx| {
                ctx.accountant.has_exact_block_at(global_offset, request.block_length)
            }) {
                log::warn!("{} requested unavailable block {}", inner.tx.remote_ip(), request);
                continue;
            }
            let data = inner.storage.read_block(global_offset, request.block_length).await?;
            let length = data.len();
            inner.tx.send_message(pwp::UploaderMessage::Block(request, data)).await?;
            inner.state.bytes_sent += length;
            initial_state.bytes_sent += length;
            with_ctx!(|ctx| ctx.peer_states.update_upload(&remote_ip, &initial_state));
        }
        Ok(())
    };

    try_join!(collect_requests, process_requests)?;
    update_state!(inner);
    Ok(to_enum!(inner))
}
