mod download;
mod extensions;
mod upload;

#[cfg(test)]
mod tests;

use super::{ctrl, ctx};
use crate::{data, pwp, sec};
use std::io;
use std::{net::SocketAddr, ops::Deref, time::Duration};
use tokio::net::TcpStream;
use tokio::time::{sleep, Instant};
use tokio::{runtime, try_join};

type CtxHandle = ctx::Handle<ctx::MainCtx>;

const EXTENSION_PROTOCOL_ENABLED: bool = true;

const ALL_SUPPORTED_EXTENSIONS: &[pwp::Extension] =
    &[pwp::Extension::Metadata, pwp::Extension::PeerExchange];

async fn from_incoming_connection(
    stream: TcpStream,
    content_storage: data::StorageClient,
    metainfo_storage: data::StorageClient,
    mut ctx_handle: CtxHandle,
    pwp_worker_handle: runtime::Handle,
    extension_protocol_enabled: bool,
) -> io::Result<(download::IdlePeer, upload::IdlePeer, Option<extensions::Peer>)> {
    define_with_ctx!(ctx_handle);

    let remote_ip = stream.peer_addr()?;
    if with_ctx!(|ctx| ctx.peer_states.get(&remote_ip).is_some()) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("already connected to {remote_ip}"),
        ));
    }

    let (info_hash, local_peer_id) =
        with_ctx!(|ctx| { (*ctx.metainfo.info_hash(), *ctx.local_peer_id.deref()) });

    let (download_chans, upload_chans, extended_chans, runner) = pwp::channels_from_incoming(
        &local_peer_id,
        Some(&info_hash),
        extension_protocol_enabled,
        stream,
    )
    .await?;
    log::info!("Successful incoming connection from {remote_ip}");

    pwp_worker_handle.spawn(async move {
        if let Err(e) = runner.run().await {
            log::debug!("Peer runner exited: {}", e);
        }
    });

    let pwp::DownloadChannels(tx, rx) = download_chans;
    let download_fut = download::new_peer(ctx_handle.clone(), rx, tx, content_storage.clone());

    let pwp::UploadChannels(tx, rx) = upload_chans;
    let upload_fut = upload::new_peer(ctx_handle.clone(), rx, tx, content_storage);

    let extensions_fut = async move {
        if let Some(pwp::ExtendedChannels(tx, rx)) = extended_chans {
            Ok(Some(extensions::new_peer(ctx_handle, rx, tx, metainfo_storage).await?))
        } else {
            Ok(None)
        }
    };

    let (seeder, leech, ext) = try_join!(download_fut, upload_fut, extensions_fut)?;
    Ok((seeder, leech, ext))
}

async fn from_outgoing_connection(
    remote_ip: SocketAddr,
    content_storage: data::StorageClient,
    metainfo_storage: data::StorageClient,
    mut ctx_handle: CtxHandle,
    pwp_worker_handle: runtime::Handle,
    extension_protocol_enabled: bool,
) -> io::Result<(download::IdlePeer, upload::IdlePeer, Option<extensions::Peer>)> {
    define_with_ctx!(ctx_handle);
    fn check_already_connected(remote_ip: SocketAddr, ctx: &ctx::MainCtx) -> io::Result<()> {
        if ctx.peer_states.get(&remote_ip).is_some() {
            Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("already connected to {remote_ip}"),
            ))
        } else {
            Ok(())
        }
    }
    with_ctx!(|ctx| check_already_connected(remote_ip, ctx))?;

    let (info_hash, local_peer_id) =
        with_ctx!(|ctx| { (*ctx.metainfo.info_hash(), *ctx.local_peer_id.deref()) });

    log::debug!("Connecting to {remote_ip}...");
    const MAX_RETRY_COUNT: usize = 3;
    let mut attempts_left = MAX_RETRY_COUNT;
    let mut reconnect_interval = sec!(2);
    let (download_chans, upload_chans, extended_chans, runner) = loop {
        match pwp::channels_from_outgoing(
            &local_peer_id,
            &info_hash,
            extension_protocol_enabled,
            remote_ip,
            None,
        )
        .await
        {
            Ok(channels) => break channels,
            Err(e) => match e.kind() {
                io::ErrorKind::ConnectionRefused | io::ErrorKind::ConnectionReset
                    if attempts_left > 0 =>
                {
                    sleep(reconnect_interval).await;
                    attempts_left -= 1;
                    reconnect_interval *= 2;
                }
                _ => return Err(e),
            },
        }
    };
    with_ctx!(|ctx| check_already_connected(remote_ip, ctx))?;
    log::info!("Successful outgoing connection to {remote_ip}");

    pwp_worker_handle.spawn(async move {
        if let Err(e) = runner.run().await {
            log::debug!("Peer runner exited: {}", e);
        }
    });

    let pwp::DownloadChannels(tx, rx) = download_chans;
    let download_fut = download::new_peer(ctx_handle.clone(), rx, tx, content_storage.clone());

    let pwp::UploadChannels(tx, rx) = upload_chans;
    let upload_fut = upload::new_peer(ctx_handle.clone(), rx, tx, content_storage);

    let extensions_fut = async move {
        if let Some(pwp::ExtendedChannels(tx, rx)) = extended_chans {
            Ok(Some(extensions::new_peer(ctx_handle, rx, tx, metainfo_storage).await?))
        } else {
            Ok(None)
        }
    };

    let (seeder, leech, ext) = try_join!(download_fut, upload_fut, extensions_fut)?;
    Ok((seeder, leech, ext))
}

async fn run_download(
    mut peer: download::Peer,
    remote_ip: SocketAddr,
    mut ctx_handle: CtxHandle,
) -> io::Result<()> {
    define_with_ctx!(ctx_handle);
    loop {
        match peer {
            download::Peer::Idle(idling_peer) => {
                match with_ctx!(|ctx| ctrl::idle_download_next_action(&remote_ip, ctx)) {
                    ctrl::IdleDownloadAction::ActivateDownload => {
                        peer = download::activate(idling_peer).await?.into();
                    }
                    ctrl::IdleDownloadAction::WaitForUpdates(timeout) => {
                        peer = download::linger(idling_peer.into(), timeout).await?;
                    }
                }
            }
            download::Peer::Seeder(seeding_peer) => {
                match with_ctx!(|ctx| ctrl::active_download_next_action(&remote_ip, ctx)) {
                    ctrl::SeederDownloadAction::RequestPieces(requests) => {
                        peer = download::get_pieces(seeding_peer, requests.into_iter()).await?;
                    }
                    ctrl::SeederDownloadAction::WaitForUpdates(timeout) => {
                        peer = download::linger(seeding_peer.into(), timeout).await?;
                    }
                    ctrl::SeederDownloadAction::DeactivateDownload => {
                        peer = download::deactivate(seeding_peer).await?.into();
                    }
                }
            }
        }
    }
}

async fn run_upload(
    mut peer: upload::Peer,
    remote_ip: SocketAddr,
    mut ctx_handle: CtxHandle,
) -> io::Result<()> {
    define_with_ctx!(ctx_handle);
    loop {
        peer = upload::update_peer(peer).await?;
        match peer {
            upload::Peer::Idle(idling_peer) => {
                match with_ctx!(|ctx| ctrl::idle_upload_next_action(&remote_ip, ctx)) {
                    ctrl::IdleUploadAction::ActivateUpload => {
                        peer = upload::activate(idling_peer).await?.into();
                    }
                    ctrl::IdleUploadAction::Linger(timeout) => {
                        peer = upload::linger(idling_peer, timeout).await?;
                    }
                }
            }
            upload::Peer::Leech(leeching_peer) => {
                match with_ctx!(|ctx| ctrl::active_upload_next_action(&remote_ip, ctx)) {
                    ctrl::LeechUploadAction::DeactivateUpload => {
                        peer = upload::deactivate(leeching_peer).await?.into();
                    }
                    ctrl::LeechUploadAction::Serve(duration) => {
                        peer = upload::serve_pieces(leeching_peer, duration).await?;
                    }
                }
            }
        }
    }
}

async fn run_extensions(
    mut peer: extensions::Peer,
    remote_ip: SocketAddr,
    mut ctx_handle: CtxHandle,
    mut peer_discovered_callback: impl FnMut(&SocketAddr),
) -> io::Result<()> {
    define_with_ctx!(ctx_handle);
    peer = extensions::send_handshake(peer, ALL_SUPPORTED_EXTENSIONS.iter()).await?;

    const PEX_INTERVAL: Duration = sec!(60);
    let mut next_pex_time = Instant::now();
    loop {
        if Instant::now() >= next_pex_time {
            peer = extensions::share_peers(peer).await?;
            next_pex_time = Instant::now() + PEX_INTERVAL;
        }
        peer = extensions::handle_incoming(
            peer,
            next_pex_time,
            with_ctx!(|ctx| ctrl::can_serve_metadata(&remote_ip, ctx)),
            &mut peer_discovered_callback,
        )
        .await?;
    }
}

macro_rules! maybe_run_extensions {
    ($peer:expr, $remote_ip:expr, $ctx_handle:expr, $peer_discovered_callback:expr) => {
        async {
            if let Some(peer) = $peer {
                run_extensions(
                    peer,
                    $remote_ip,
                    $ctx_handle.clone(),
                    &mut $peer_discovered_callback,
                )
                .await
            } else {
                Ok(())
            }
        }
    };
}

pub async fn outgoing_pwp_connection(
    remote_ip: SocketAddr,
    content_storage: data::StorageClient,
    metainfo_storage: data::StorageClient,
    ctx_handle: CtxHandle,
    pwp_worker_handle: runtime::Handle,
    mut peer_discovered_callback: impl FnMut(&SocketAddr),
) -> io::Result<()> {
    loop {
        let (download, upload, extensions) = from_outgoing_connection(
            remote_ip,
            content_storage.clone(),
            metainfo_storage.clone(),
            ctx_handle.clone(),
            pwp_worker_handle.clone(),
            EXTENSION_PROTOCOL_ENABLED,
        )
        .await?;
        let run_result = try_join!(
            run_download(download.into(), remote_ip, ctx_handle.clone()),
            run_upload(upload.into(), remote_ip, ctx_handle.clone()),
            maybe_run_extensions!(extensions, remote_ip, ctx_handle, peer_discovered_callback),
        );
        match run_result {
            Err(e) if e.kind() != io::ErrorKind::Other => {
                // ErrorKind::Other means we disconnected the peer intentionally
                log::warn!("Peer {remote_ip} disconnected: {e}. Reconnecting...")
            }
            Err(e) => return Err(e),
            _ => break,
        }
    }
    Ok(())
}

pub async fn incoming_pwp_connection(
    stream: TcpStream,
    content_storage: data::StorageClient,
    metainfo_storage: data::StorageClient,
    ctx_handle: CtxHandle,
    pwp_worker_handle: runtime::Handle,
    mut peer_discovered_callback: impl FnMut(&SocketAddr),
) -> io::Result<()> {
    let remote_ip = stream.peer_addr()?;
    let (download, upload, extensions) = from_incoming_connection(
        stream,
        content_storage.clone(),
        metainfo_storage.clone(),
        ctx_handle.clone(),
        pwp_worker_handle.clone(),
        EXTENSION_PROTOCOL_ENABLED,
    )
    .await?;
    let run_result = try_join!(
        run_download(download.into(), remote_ip, ctx_handle.clone()),
        run_upload(upload.into(), remote_ip, ctx_handle.clone()),
        maybe_run_extensions!(extensions, remote_ip, ctx_handle, peer_discovered_callback),
    );
    match run_result {
        Err(e) if e.kind() != io::ErrorKind::Other => {
            // ErrorKind::Other means we disconnected the peer intentionally
            log::warn!("Peer {remote_ip} disconnected: {e}. Reconnecting...");
            outgoing_pwp_connection(
                remote_ip,
                content_storage,
                metainfo_storage,
                ctx_handle,
                pwp_worker_handle,
                peer_discovered_callback,
            )
            .await
        }
        Err(e) => Err(e),
        _ => Ok(()),
    }
}
