use super::super::{PeerReporter, ctrl, ctx};
use super::{CLIENT_NAME, LOCAL_REQQ};
use local_async_utils::prelude::*;
use mtorrent_core::{data, pwp};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::Duration;
use std::{cmp, io};
use tokio::select;
use tokio::time;

type CtxHandle = ctx::Handle<ctx::MainCtx>;

struct Data {
    handle: CtxHandle,
    rx: pwp::ExtendedRxChannel,
    tx: pwp::ExtendedTxChannel,
    metadata_storage: data::StorageClient,
    remote_extensions: HashMap<pwp::Extension, u8>,
    sent_metadata_pieces: pwp::Bitfield,
    last_shared_peers: HashSet<SocketAddr>,
    peer_reporter: PeerReporter,
}

pub struct Peer(Box<Data>);

pub async fn new_peer(
    mut handle: CtxHandle,
    rx: pwp::ExtendedRxChannel,
    tx: pwp::ExtendedTxChannel,
    metadata_storage: data::StorageClient,
    enabled_extensions: impl IntoIterator<Item = &pwp::Extension>,
    peer_reporter: PeerReporter,
) -> io::Result<Peer> {
    let metadata_len = handle.with(|ctx| ctx.metainfo.size());
    let mut inner = Box::new(Data {
        handle,
        rx,
        tx,
        metadata_storage,
        remote_extensions: Default::default(),
        sent_metadata_pieces: pwp::Bitfield::repeat(false, metadata_len),
        last_shared_peers: Default::default(),
        peer_reporter,
    });
    define_with_ctx!(inner.handle);

    // send local handshake
    let local_handshake = Box::new(pwp::ExtendedHandshake {
        extensions: enabled_extensions.into_iter().map(|e| (*e, e.local_id())).collect(),
        listen_port: Some(with_ctx!(|ctx| ctx.const_data.pwp_listener_public_addr().port())),
        client_type: Some(CLIENT_NAME.to_string()),
        yourip: Some(inner.rx.remote_ip().ip()),
        metadata_size: Some(with_ctx!(|ctx| ctx.metainfo.size())),
        request_limit: Some(LOCAL_REQQ),
        ..Default::default()
    });
    log::debug!("Sending extended handshake to {}: {}", inner.rx.remote_ip(), local_handshake);
    inner
        .tx
        .send_message((pwp::ExtendedMessage::Handshake(local_handshake), 0))
        .await?;

    // try wait for remote handshake
    match inner.rx.receive_message_timed(sec!(1)).await {
        Ok(pwp::ExtendedMessage::Handshake(mut hs)) => {
            log::debug!("Received extended handshake from {}: {}", inner.rx.remote_ip(), hs);
            hs.extensions.retain(|_ext, id| *id != 0); // id 0 means extension is disabled
            inner.remote_extensions.clone_from(&hs.extensions);
            with_ctx!(|ctx| ctx.peer_states.set_extended_handshake(inner.rx.remote_ip(), hs));
        }
        Ok(msg) => {
            return Err(io::Error::other(format!("unexpected first extended message: {msg}")));
        }
        Err(pwp::ChannelError::Timeout) => (),
        Err(e) => return Err(e.into()),
    }
    Ok(Peer(inner))
}

pub async fn run(peer: Peer, pex_interval: Duration) -> io::Result<()> {
    let Peer(mut inner) = peer;

    let mut pex_timer = time::interval(pex_interval);
    loop {
        select! {
            biased;
            received = inner.rx.receive_message() => {
                handle_incoming_message(&mut inner, received?).await?;
            }
            _ = pex_timer.tick() => {
                send_pex(&mut inner).await?;
            }
        }
    }
}

async fn send_pex(inner: &mut Data) -> io::Result<()> {
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
    Ok(())
}

async fn handle_incoming_message(inner: &mut Data, msg: pwp::ExtendedMessage) -> io::Result<()> {
    define_with_ctx!(inner.handle);

    let remote_ip = *inner.rx.remote_ip();
    let serve_metadata = with_ctx!(|ctx| ctrl::can_serve_metadata(&remote_ip, ctx));

    match msg {
        pwp::ExtendedMessage::Handshake(mut hs) => {
            log::debug!("Received extended handshake from {remote_ip}: {hs}");
            // every subsequent handshake contains diff from the previous one
            for (ext, id) in &hs.extensions {
                if id == &0 {
                    inner.remote_extensions.remove(ext);
                } else {
                    inner.remote_extensions.insert(*ext, *id);
                }
            }
            hs.extensions.clone_from(&inner.remote_extensions);
            with_ctx!(|ctx| ctx.peer_states.set_extended_handshake(&remote_ip, hs));
        }
        pwp::ExtendedMessage::PeerExchange(pex) => {
            log::debug!("Received PEX message from {remote_ip}: {pex}");
            if !pex.added.is_empty() {
                let connected_peers: HashSet<SocketAddr> =
                    with_ctx!(|ctx| ctx.peer_states.iter().map(|(ip, _)| *ip).collect());
                for peer_addr in pex.added.difference(&connected_peers) {
                    inner
                        .peer_reporter
                        .report_discovered_new(*peer_addr, pwp::PeerOrigin::Pex)
                        .await;
                }
            };
        }
        pwp::ExtendedMessage::MetadataRequest { piece } => {
            if let Some(&id) = inner.remote_extensions.get(&pwp::Extension::Metadata) {
                if serve_metadata && inner.sent_metadata_pieces.get(piece).is_some_and(|sent| !sent)
                {
                    log::debug!("Serving metadata request from {remote_ip}: (piece={piece})");
                    let metadata_len = with_ctx!(|ctx| ctx.metainfo.size());
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
        msg => {
            log::warn!("Unexpected extension message from {remote_ip}: {msg}");
        }
    }
    Ok(())
}
