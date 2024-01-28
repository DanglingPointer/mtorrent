use tokio::time::Instant;

use super::{ctx, MAX_BLOCK_SIZE};
use crate::{data, millisec, pwp};
use std::cell::RefCell;
use std::collections::HashSet;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

struct Data {
    handle: ctx::UnsafeHandle,
    rx: pwp::UploadRxChannel,
    tx: pwp::UploadTxChannel,
    storage: Rc<data::StorageClient>,
    am_choking: bool,
    peer_interested: bool,
    bytes_sent: usize,
}

impl Drop for Data {
    fn drop(&mut self) {
        self.handle.with_ctx(|ctx| ctx.piece_tracker.forget_peer(self.tx.remote_ip()));
    }
}

pub trait State {
    fn am_choking(&self) -> bool;
    fn peer_interested(&self) -> bool;
    fn bytes_sent(&self) -> usize;
    fn ip(&self) -> &SocketAddr;
}

impl State for Data {
    fn am_choking(&self) -> bool {
        self.am_choking
    }

    fn peer_interested(&self) -> bool {
        self.peer_interested
    }

    fn bytes_sent(&self) -> usize {
        self.bytes_sent
    }

    fn ip(&self) -> &SocketAddr {
        self.tx.remote_ip()
    }
}

pub struct IdlePeer(Box<Data>);

pub struct LeechingPeer(Box<Data>);

impl IdlePeer {
    pub fn state(&self) -> &impl State {
        self.0.as_ref()
    }
}

impl LeechingPeer {
    pub fn state(&self) -> &impl State {
        self.0.as_ref()
    }
}

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
        if $inner.peer_interested && !$inner.am_choking {
            LeechingPeer($inner).into()
        } else {
            IdlePeer($inner).into()
        }
    };
}

// macro_rules! inner {
//     ($inner:expr) => {
//         match $inner {
//             Peer::Idle(IdlePeer(data)) => data,
//             Peer::Leech(LeechingPeer(data)) => data,
//         }
//     };
// }

pub async fn create_peer(
    handle: ctx::UnsafeHandle,
    rx: pwp::UploadRxChannel,
    tx: pwp::UploadTxChannel,
    storage: Rc<data::StorageClient>,
) -> io::Result<IdlePeer> {
    let mut inner = Box::new(Data {
        handle,
        rx,
        tx,
        storage,
        am_choking: true,
        peer_interested: false,
        bytes_sent: 0,
    });
    let bitfield = inner.handle.with_ctx(|ctx| ctx.accountant.generate_bitfield());
    inner.tx.send_message(pwp::UploaderMessage::Bitfield(bitfield)).await?;
    Ok(IdlePeer(inner))
}

pub async fn activate(peer: IdlePeer) -> io::Result<LeechingPeer> {
    let mut inner = peer.0;
    debug_assert!(inner.am_choking || !inner.peer_interested);
    if inner.am_choking {
        inner.tx.send_message(pwp::UploaderMessage::Unchoke).await?;
        inner.am_choking = false;
    }
    if inner.peer_interested {
        Ok(LeechingPeer(inner))
    } else {
        let mut peer = IdlePeer(inner);
        loop {
            match wait(peer, Duration::MAX).await? {
                Peer::Idle(idle) => peer = idle,
                Peer::Leech(leech) => break Ok(leech),
            }
        }
    }
}

pub async fn deactivate(peer: LeechingPeer) -> io::Result<IdlePeer> {
    let mut inner = peer.0;
    debug_assert!(inner.peer_interested && !inner.am_choking);
    inner.tx.send_message(pwp::UploaderMessage::Choke).await?;
    inner.am_choking = true;
    Ok(IdlePeer(inner))
}

pub async fn unchoke(peer: IdlePeer) -> io::Result<IdlePeer> {
    let mut inner = peer.0;
    debug_assert!(inner.am_choking);
    inner.tx.send_message(pwp::UploaderMessage::Unchoke).await?;
    inner.am_choking = false;
    Ok(IdlePeer(inner))
}

pub async fn wait(peer: IdlePeer, timeout: Duration) -> io::Result<Peer> {
    let mut inner = peer.0;
    debug_assert!(inner.am_choking || !inner.peer_interested);
    let mut now = Instant::now();
    let end_time = now + timeout;
    let mut state_changed = false;
    while !state_changed && now < end_time {
        match inner.rx.receive_message_timed(end_time - now).await {
            Ok(msg) => {
                state_changed = update_state(&mut inner, &msg);
                match &msg {
                    pwp::DownloaderMessage::Request(info) => {
                        log::warn!(
                            "{}: Received request ({}) while choking",
                            inner.tx.remote_ip(),
                            info
                        )
                    }
                    pwp::DownloaderMessage::Cancel(info) => {
                        log::warn!(
                            "{}: Received cancel ({}) while choking",
                            inner.tx.remote_ip(),
                            info
                        )
                    }
                    _ => (),
                }
            }
            Err(pwp::ChannelError::Timeout) => (),
            Err(e) => return Err(e.into()),
        }
        now = Instant::now();
    }
    Ok(to_enum!(inner))
}

pub async fn serve_for(peer: LeechingPeer, min_duration: Duration) -> io::Result<Peer> {
    let mut inner = peer.0;
    debug_assert!(!inner.am_choking && inner.peer_interested);

    async fn collect_requests(
        inner: &mut Box<Data>,
        period: Duration,
        requested_blocks: &mut HashSet<pwp::BlockInfo>,
    ) -> io::Result<()> {
        let mut now = Instant::now();
        let end_time = now + period;
        while now < end_time {
            match inner.rx.receive_message_timed(end_time - now).await {
                Err(pwp::ChannelError::Timeout) => (),
                Err(e) => return Err(e.into()),
                Ok(pwp::DownloaderMessage::Request(info)) => {
                    requested_blocks.insert(info);
                }
                Ok(pwp::DownloaderMessage::Cancel(info)) => {
                    requested_blocks.remove(&info);
                }
                Ok(msg) => {
                    update_state(inner, &msg);
                }
            }
            now = Instant::now();
        }
        Ok(())
    }

    async fn serve_one_request(inner: &mut Box<Data>, request: pwp::BlockInfo) -> io::Result<()> {
        if request.block_length > MAX_BLOCK_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Received too big request: {request}"),
            ));
        }
        let global_offset = inner
            .handle
            .with_ctx(|ctx| {
                ctx.pieces.global_offset(
                    request.piece_index,
                    request.in_piece_offset,
                    request.block_length,
                )
            })
            .map_err(|_| {
                io::Error::new(io::ErrorKind::Other, format!("Received invalid request: {request}"))
            })?;
        let data = inner.storage.read_block(global_offset, request.block_length).await?;
        inner.tx.send_message(pwp::UploaderMessage::Block(request, data)).await?;
        Ok(())
    }

    let mut now = Instant::now();
    let end_time = now + min_duration;
    let mut requests = HashSet::<pwp::BlockInfo>::new();
    while now < end_time && inner.peer_interested {
        collect_requests(&mut inner, millisec!(50), &mut requests).await?;
        if !inner.peer_interested {
            return Ok(to_enum!(inner));
        }
        if let Some(request) = requests.iter().next().cloned() {
            requests.remove(&request);
            serve_one_request(&mut inner, request).await?;
        }
        now = Instant::now();
    }
    for request in requests.drain() {
        serve_one_request(&mut inner, request).await?;
    }
    Ok(to_enum!(inner))
}

fn update_state(inner: &mut Data, msg: &pwp::DownloaderMessage) -> bool {
    match msg {
        pwp::DownloaderMessage::Interested => {
            if !inner.peer_interested {
                inner.peer_interested = true;
                true
            } else {
                false
            }
        }
        pwp::DownloaderMessage::NotInterested => {
            if inner.peer_interested {
                inner.peer_interested = false;
                true
            } else {
                false
            }
        }
        pwp::DownloaderMessage::Request(_) => false,
        pwp::DownloaderMessage::Cancel(_) => false,
    }
}
