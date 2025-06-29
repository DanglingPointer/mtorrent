use super::super::ctx;
use super::CLIENT_NAME;
use crate::pwp;
use local_async_utils::prelude::*;
use std::{cmp, io};
use tokio::time::Instant;

type CtxHandle = ctx::Handle<ctx::PreliminaryCtx>;

struct Data {
    handle: CtxHandle,
    rx: pwp::ExtendedRxChannel,
    tx: pwp::ExtendedTxChannel,
    remote_metadata_ext_id: u8,
}

impl Drop for Data {
    fn drop(&mut self) {
        self.handle.with(|ctx| {
            ctx.connected_peers.remove(self.rx.remote_ip());
        });
    }
}

pub struct DisabledPeer(Box<Data>);
pub struct RejectingPeer(Box<Data>);
pub struct UploadingPeer(Box<Data>);

pub enum Peer {
    Disabled(DisabledPeer),
    Rejector(RejectingPeer),
    Uploader(UploadingPeer),
}

impl From<DisabledPeer> for Peer {
    fn from(peer: DisabledPeer) -> Self {
        Peer::Disabled(peer)
    }
}

impl From<RejectingPeer> for Peer {
    fn from(peer: RejectingPeer) -> Self {
        Peer::Rejector(peer)
    }
}

impl From<UploadingPeer> for Peer {
    fn from(peer: UploadingPeer) -> Self {
        Peer::Uploader(peer)
    }
}

macro_rules! to_enum {
    ($inner:expr) => {
        if $inner.remote_metadata_ext_id == 0 {
            Peer::Disabled(DisabledPeer($inner))
        } else {
            Peer::Uploader(UploadingPeer($inner))
        }
    };
}

fn get_metadata_ext_id(hs: &pwp::ExtendedHandshake) -> io::Result<u8> {
    hs.extensions
        .get(&pwp::Extension::Metadata)
        .cloned()
        .ok_or_else(|| io::Error::other("peer does not support metadata extension"))
}

fn init_metadata(ctx: &mut ctx::PreliminaryCtx, metadata_size: usize) -> io::Result<()> {
    const MIN_METADATA_SIZE: usize = 50; // apprx
    const MAX_METADATA_SIZE: usize = 100 * 1024 * 1024; // 100MB
    match metadata_size {
        MIN_METADATA_SIZE..=MAX_METADATA_SIZE => {
            if ctx.metainfo_pieces.is_empty() {
                ctx.metainfo.resize(metadata_size, 0);
                let piece_count = metadata_size.div_ceil(pwp::MAX_BLOCK_SIZE);
                ctx.metainfo_pieces = pwp::Bitfield::repeat(false, piece_count);
            } else if ctx.metainfo.len() != metadata_size {
                log::error!(
                    "metadata size mismatch: expected {}, got {}",
                    ctx.metainfo.len(),
                    metadata_size
                );
                // TODO: what to do?
            }
            Ok(())
        }
        _ => Err(io::Error::other(format!("invalid metainfo file size ({metadata_size})"))),
    }
}

fn submit_block(ctx: &mut ctx::PreliminaryCtx, piece_index: usize, data: &[u8]) -> io::Result<()> {
    let mut has_piece = ctx
        .metainfo_pieces
        .get_mut(piece_index)
        .ok_or_else(|| io::Error::other(format!("invalid metadata piece index ({piece_index})")))?;
    if *has_piece {
        return Ok(());
    }
    let offset = piece_index * pwp::MAX_BLOCK_SIZE;
    let expected_len = cmp::min(pwp::MAX_BLOCK_SIZE, ctx.metainfo.len() - offset);
    if data.len() != expected_len {
        Err(io::Error::other(format!("invalid metadata block length ({})", data.len())))
    } else {
        let chunk = ctx
            .metainfo
            .get_mut(offset..offset + data.len())
            .ok_or_else(|| io::Error::other(format!("invalid metadata block offset ({offset})")))?;
        chunk.copy_from_slice(data);
        *has_piece = true;
        Ok(())
    }
}

pub async fn new_peer(
    mut handle: CtxHandle,
    extended_chans: pwp::ExtendedChannels,
) -> io::Result<Peer> {
    define_with_ctx!(handle);
    let pwp::ExtendedChannels(mut tx, mut rx) = extended_chans;

    // send local handshake
    let local_handshake = Box::new(pwp::ExtendedHandshake {
        extensions: [(pwp::Extension::Metadata, pwp::Extension::Metadata.local_id())]
            .into_iter()
            .collect(),
        listen_port: Some(with_ctx!(|ctx| ctx.const_data.pwp_listener_public_addr().port())),
        client_type: Some(CLIENT_NAME.to_string()),
        yourip: Some(rx.remote_ip().ip()),
        ..Default::default()
    });
    tx.send_message((pwp::ExtendedMessage::Handshake(local_handshake), 0)).await?;

    // try wait for remote handshake
    let remote_metadata_ext_id = match rx.receive_message_timed(sec!(1)).await {
        Ok(pwp::ExtendedMessage::Handshake(hs)) => {
            log::debug!("Received initial extended handshake from {}: {}", rx.remote_ip(), hs);
            if let Some(metadata_size) = hs.metadata_size {
                with_ctx!(|ctx| init_metadata(ctx, metadata_size))?;
            }
            get_metadata_ext_id(&hs)?
        }
        Ok(_) | Err(pwp::ChannelError::Timeout) => 0,
        Err(e) => return Err(e.into()),
    };

    with_ctx!(|ctx| {
        // the bookkeeping below must be done _after_ the I/O operations above,
        // otherwise it will be never undone in the case of a send/recv error
        let peer_ip = rx.remote_ip();
        ctx.connected_peers.insert(*peer_ip);
    });
    let inner = Box::new(Data {
        handle,
        rx,
        tx,
        remote_metadata_ext_id,
    });
    Ok(to_enum!(inner))
}

pub async fn wait_until_enabled(peer: DisabledPeer) -> io::Result<UploadingPeer> {
    let mut inner = peer.0;
    define_with_ctx!(inner.handle);
    while inner.remote_metadata_ext_id == 0 {
        match inner.rx.receive_message().await? {
            pwp::ExtendedMessage::Handshake(hs) => {
                log::debug!("Received extended handshake from {}: {}", inner.rx.remote_ip(), hs);
                if let Some(metadata_size) = hs.metadata_size {
                    with_ctx!(|ctx| init_metadata(ctx, metadata_size))?;
                }
                if let Some(id) = hs.extensions.get(&pwp::Extension::Metadata) {
                    inner.remote_metadata_ext_id = *id;
                }
            }
            msg => log::debug!(
                "Received {} from {} while waiting for metadata to be enabled",
                msg,
                inner.rx.remote_ip()
            ),
        }
    }
    Ok(UploadingPeer(inner))
}

pub async fn cool_off_rejecting_peer(peer: RejectingPeer, until: Instant) -> io::Result<Peer> {
    let mut inner = peer.0;
    define_with_ctx!(inner.handle);
    loop {
        match inner.rx.receive_message_timed(until - Instant::now()).await {
            Ok(pwp::ExtendedMessage::Handshake(hs)) => {
                log::debug!(
                    "Received subsequent extended handshake from {}: {}",
                    inner.rx.remote_ip(),
                    hs
                );
                if let Some(metadata_size) = hs.metadata_size {
                    with_ctx!(|ctx| init_metadata(ctx, metadata_size))?;
                }
                if let Some(id) = hs.extensions.get(&pwp::Extension::Metadata) {
                    inner.remote_metadata_ext_id = *id;
                    if inner.remote_metadata_ext_id == 0 {
                        break;
                    }
                }
            }
            Ok(pwp::ExtendedMessage::MetadataRequest { piece }) => {
                log::debug!(
                    "Rejecting metadata request (piece={}) from {}",
                    piece,
                    inner.rx.remote_ip()
                );
                inner
                    .tx
                    .send_message((
                        pwp::ExtendedMessage::MetadataReject { piece },
                        inner.remote_metadata_ext_id,
                    ))
                    .await?;
            }
            Ok(msg) => log::debug!(
                "Received {} from {} while cooling off a rejecting peer",
                msg,
                inner.rx.remote_ip()
            ),
            Err(pwp::ChannelError::Timeout) => break,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(to_enum!(inner))
}

pub async fn download_metadata(peer: UploadingPeer) -> io::Result<Peer> {
    let mut inner = peer.0;
    define_with_ctx!(inner.handle);

    fn next_piece_to_request(ctx: &ctx::PreliminaryCtx) -> Option<usize> {
        if ctx.metainfo_pieces.is_empty() {
            Some(0)
        } else {
            ctx.metainfo_pieces.iter().position(|downloaded| downloaded == false)
        }
    }

    // At this point we MUST have pieces to request, otherwise we end up in an infinite loop
    // that starves all other futures in the same (single-threaded) runtime
    assert!(with_ctx!(|ctx| next_piece_to_request(ctx)).is_some(), "infinite loop");

    while let Some(piece) = with_ctx!(|ctx| next_piece_to_request(ctx)) {
        log::debug!("Requesting metadata piece {} from {}", piece, inner.tx.remote_ip());
        inner
            .tx
            .send_message((
                pwp::ExtendedMessage::MetadataRequest { piece },
                inner.remote_metadata_ext_id,
            ))
            .await?;

        loop {
            match inner.rx.receive_message().await? {
                pwp::ExtendedMessage::Handshake(hs) => {
                    log::debug!(
                        "Received subsequent extended handshake from {}: {}",
                        inner.rx.remote_ip(),
                        hs
                    );
                    if let Some(metadata_size) = hs.metadata_size {
                        with_ctx!(|ctx| init_metadata(ctx, metadata_size))?;
                    }
                    if let Some(id) = hs.extensions.get(&pwp::Extension::Metadata) {
                        inner.remote_metadata_ext_id = *id;
                        if inner.remote_metadata_ext_id == 0 {
                            return Ok(Peer::Disabled(DisabledPeer(inner)));
                        }
                    }
                }
                pwp::ExtendedMessage::MetadataRequest { piece } => {
                    log::debug!(
                        "Rejecting metadata request (piece={}) from {}",
                        piece,
                        inner.rx.remote_ip()
                    );
                    inner
                        .tx
                        .send_message((
                            pwp::ExtendedMessage::MetadataReject { piece },
                            inner.remote_metadata_ext_id,
                        ))
                        .await?;
                }
                pwp::ExtendedMessage::MetadataBlock {
                    piece: piece_index,
                    total_size,
                    data,
                } => {
                    log::debug!("Received metadata piece {} from {}", piece, inner.tx.remote_ip());
                    with_ctx!(|ctx| init_metadata(ctx, total_size))?;
                    with_ctx!(|ctx| submit_block(ctx, piece_index, &data))?;
                    if piece_index == piece {
                        break;
                    }
                }
                pwp::ExtendedMessage::MetadataReject { .. } => {
                    return Ok(Peer::Rejector(RejectingPeer(inner)));
                }
                pwp::ExtendedMessage::PeerExchange(_) => (),
            }
        }
    }
    Ok(to_enum!(inner))
}
