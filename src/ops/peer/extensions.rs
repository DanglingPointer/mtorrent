use super::super::ctx;
use super::{ALL_SUPPORTED_EXTENSIONS, CLIENT_NAME, MAX_PENDING_REQUESTS};
use crate::{data, pwp, sec};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::{cmp, io};
use tokio::time::Instant;

type CtxHandle = ctx::Handle<ctx::MainCtx>;

struct Data {
    handle: CtxHandle,
    rx: pwp::ExtendedRxChannel,
    tx: pwp::ExtendedTxChannel,
    metadata_storage: data::StorageClient,
    remote_extensions: HashMap<pwp::Extension, u8>,
    sent_metadata_pieces: pwp::Bitfield,
    last_shared_peers: HashSet<SocketAddr>,
}

pub struct Peer(Box<Data>);

pub async fn new_peer(
    mut handle: CtxHandle,
    rx: pwp::ExtendedRxChannel,
    tx: pwp::ExtendedTxChannel,
    metadata_storage: data::StorageClient,
) -> io::Result<Peer> {
    let metadata_len = handle.with_ctx(|ctx| ctx.metainfo.size());
    let mut inner = Box::new(Data {
        handle,
        rx,
        tx,
        metadata_storage,
        remote_extensions: Default::default(),
        sent_metadata_pieces: pwp::Bitfield::repeat(false, metadata_len),
        last_shared_peers: Default::default(),
    });
    // try wait for remote handshake
    match inner.rx.receive_message_timed(sec!(1)).await {
        Ok(pwp::ExtendedMessage::Handshake(hs)) => {
            log::debug!("Received extended handshake from {}: {}", inner.rx.remote_ip(), hs);
            inner.remote_extensions = hs.extensions;
            inner.remote_extensions.retain(|_ext, id| *id != 0); // id 0 means extension is disabled
        }
        Ok(msg) => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("unexpected first extended message: {msg}"),
            ))
        }
        Err(pwp::ChannelError::Timeout) => (),
        Err(e) => return Err(e.into()),
    }
    // don't send local handshake here, because 'upload' needs to send bitfield first
    Ok(Peer(inner))
}

pub async fn send_handshake(
    peer: Peer,
    enabled_extensions: impl IntoIterator<Item = &pwp::Extension>,
) -> io::Result<Peer> {
    let mut inner = peer.0;
    define_with_ctx!(inner.handle);

    // set 0 ids for disabled extensions, and local ids for those enabled
    let mut extensions: HashMap<pwp::Extension, u8> =
        ALL_SUPPORTED_EXTENSIONS.iter().map(|e| (*e, 0)).collect();
    extensions.extend(enabled_extensions.into_iter().map(|e| (*e, e.local_id())));

    let local_handshake = Box::new(pwp::HandshakeData {
        extensions,
        listen_port: Some(with_ctx!(|ctx| ctx.const_data.pwp_listener_public_addr().port())),
        client_type: Some(CLIENT_NAME.to_string()),
        yourip: Some(inner.rx.remote_ip().ip()),
        metadata_size: Some(with_ctx!(|ctx| ctx.metainfo.size())),
        request_limit: Some(MAX_PENDING_REQUESTS),
        ..Default::default()
    });
    log::debug!("Sending extended handshake to {}: {}", inner.rx.remote_ip(), local_handshake);
    inner
        .tx
        .send_message((pwp::ExtendedMessage::Handshake(local_handshake), 0))
        .await?;
    Ok(Peer(inner))
}

pub async fn share_peers(peer: Peer) -> io::Result<Peer> {
    let mut inner = peer.0;
    define_with_ctx!(inner.handle);

    if let Some(&id) = inner.remote_extensions.get(&pwp::Extension::PeerExchange) {
        let connected_peers: HashSet<SocketAddr> =
            with_ctx!(|ctx| ctx.peer_states.iter().map(|(ip, _)| *ip).collect());
        let pex = Box::new(pwp::PeerExchangeData {
            added: connected_peers.difference(&inner.last_shared_peers).cloned().collect(),
            dropped: inner.last_shared_peers.difference(&connected_peers).cloned().collect(),
        });
        if !pex.added.is_empty() || !pex.dropped.is_empty() {
            log::debug!("Sending PEX message to {}: {}", inner.tx.remote_ip(), pex);
            inner.tx.send_message((pwp::ExtendedMessage::PeerExchange(pex), id)).await?;
            inner.last_shared_peers = connected_peers;
        }
    }
    Ok(Peer(inner))
}

pub async fn handle_incoming(
    peer: Peer,
    until: Instant,
    serve_metadata: bool,
    mut peer_discovered_callback: impl FnMut(&SocketAddr),
) -> io::Result<Peer> {
    let mut inner = peer.0;
    define_with_ctx!(inner.handle);

    let metadata_len = with_ctx!(|ctx| ctx.metainfo.size());
    let remote_ip = *inner.rx.remote_ip();
    loop {
        match inner.rx.receive_message_timed(until - Instant::now()).await {
            Err(pwp::ChannelError::Timeout) => break,
            Err(e) => return Err(e.into()),
            Ok(pwp::ExtendedMessage::Handshake(hs)) => {
                log::debug!("Received extended handshake from {remote_ip}: {hs}");
                // every subsequent handshake contains diff from the previous one
                for (ext, id) in hs.extensions {
                    if id == 0 {
                        inner.remote_extensions.remove(&ext);
                    } else {
                        inner.remote_extensions.insert(ext, id);
                    }
                }
            }
            Ok(pwp::ExtendedMessage::PeerExchange(pex)) => {
                log::debug!("Received PEX message from {remote_ip}: {pex}");
                if !pex.added.is_empty() {
                    let connected_peers: HashSet<SocketAddr> =
                        with_ctx!(|ctx| ctx.peer_states.iter().map(|(ip, _)| *ip).collect());
                    for peer_addr in pex.added.difference(&connected_peers) {
                        peer_discovered_callback(peer_addr);
                    }
                };
            }
            Ok(pwp::ExtendedMessage::MetadataRequest { piece }) => {
                if let Some(&id) = inner.remote_extensions.get(&pwp::Extension::Metadata) {
                    if serve_metadata
                        && inner.sent_metadata_pieces.get(piece).is_some_and(|sent| !sent)
                    {
                        log::debug!("Serving metadata request from {remote_ip}: (piece={piece})");
                        let global_offset = piece * pwp::MAX_BLOCK_SIZE;
                        let length = cmp::min(pwp::MAX_BLOCK_SIZE, metadata_len - global_offset);
                        let data = inner.metadata_storage.read_block(global_offset, length).await?;
                        inner
                            .tx
                            .send_message((
                                pwp::ExtendedMessage::MetadataBlock {
                                    piece,
                                    total_size: metadata_len,
                                    data,
                                },
                                id,
                            ))
                            .await?;
                        inner.sent_metadata_pieces.set(piece, true);
                    } else {
                        log::debug!("Rejecting metadata request from {remote_ip}: (piece={piece})");
                        inner
                            .tx
                            .send_message((pwp::ExtendedMessage::MetadataReject { piece }, id))
                            .await?;
                    }
                }
            }
            Ok(msg) => {
                log::warn!("Unexpected extension message from {remote_ip}: {msg}");
            }
        }
    }
    Ok(Peer(inner))
}
