mod download;
mod extensions;
mod metadata;
mod upload;

#[cfg(test)]
mod tests;

use super::{ctrl, ctx};
use crate::utils::peer_id::PeerId;
use crate::{data, pwp, sec};
use std::io;
use std::{net::SocketAddr, time::Duration};
use tokio::net::TcpStream;
use tokio::time::{self, Instant};
use tokio::{runtime, try_join};

type MainHandle = ctx::Handle<ctx::MainCtx>;
type PreliminaryHandle = ctx::Handle<ctx::PreliminaryCtx>;

const EXTENSION_PROTOCOL_ENABLED: bool = true;

const ALL_SUPPORTED_EXTENSIONS: &[pwp::Extension] =
    &[pwp::Extension::Metadata, pwp::Extension::PeerExchange];

const CLIENT_NAME: &str = concat!(env!("CARGO_PKG_NAME"), " ", env!("CARGO_PKG_VERSION"));

const MAX_PENDING_REQUESTS: usize = 1024 * 3;

macro_rules! marshal_stream {
    ($stream:expr, $rt_handle:expr) => {{
        // note: EnterGuard must NEVER live across a suspension point
        let _g = $rt_handle.enter();
        let std_stream = $stream.into_std()?;
        TcpStream::from_std(std_stream)?
    }};
}

async fn channels_for_outgoing_connection(
    local_peer_id: &PeerId,
    info_hash: &[u8; 20],
    extension_protocol_enabled: bool,
    remote_ip: SocketAddr,
    pwp_worker_handle: runtime::Handle,
) -> io::Result<(pwp::DownloadChannels, pwp::UploadChannels, Option<pwp::ExtendedChannels>)> {
    log::debug!("Connecting to {remote_ip}...");
    const MAX_RETRY_COUNT: usize = 3;
    let mut attempts_left = MAX_RETRY_COUNT;
    let mut reconnect_interval = sec!(2);
    let stream = loop {
        match time::timeout(sec!(30), TcpStream::connect(remote_ip)).await? {
            Ok(stream) => break stream,
            Err(e) => match e.kind() {
                io::ErrorKind::ConnectionRefused | io::ErrorKind::ConnectionReset
                    if attempts_left > 0 =>
                {
                    time::sleep(reconnect_interval).await;
                    attempts_left -= 1;
                    reconnect_interval *= 2;
                }
                _ => return Err(e),
            },
        }
    };
    // re-register socket so that it will be polled on PWP thread
    let stream = marshal_stream!(stream, pwp_worker_handle);
    let (download_chans, upload_chans, extended_chans, runner) = pwp::channels_from_outgoing(
        local_peer_id,
        info_hash,
        extension_protocol_enabled,
        remote_ip,
        stream,
        None,
    )
    .await?;
    log::info!("Successful outgoing connection to {remote_ip}");

    pwp_worker_handle.spawn(async move {
        if let Err(e) = runner.await {
            log::debug!("Peer runner exited: {}", e);
        }
    });

    Ok((download_chans, upload_chans, extended_chans))
}

async fn channels_for_incoming_connection(
    local_peer_id: &[u8; 20],
    info_hash: &[u8; 20],
    extension_protocol_enabled: bool,
    remote_ip: SocketAddr,
    stream: TcpStream,
    pwp_worker_handle: runtime::Handle,
) -> io::Result<(pwp::DownloadChannels, pwp::UploadChannels, Option<pwp::ExtendedChannels>)> {
    // re-register socket so that it will be polled on PWP thread
    let stream = marshal_stream!(stream, pwp_worker_handle);
    let (download_chans, upload_chans, extended_chans, runner) = pwp::channels_from_incoming(
        local_peer_id,
        Some(info_hash),
        extension_protocol_enabled,
        remote_ip,
        stream,
    )
    .await?;
    log::info!("Successful incoming connection from {remote_ip}");

    pwp_worker_handle.spawn(async move {
        if let Err(e) = runner.await {
            log::debug!("Peer runner exited: {}", e);
        }
    });

    Ok((download_chans, upload_chans, extended_chans))
}

// ------------------------------------------------------------------------------------------------

async fn run_download(
    mut peer: download::Peer,
    remote_ip: SocketAddr,
    mut ctx_handle: MainHandle,
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
    mut ctx_handle: MainHandle,
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
    mut ctx_handle: MainHandle,
    mut peer_discovered_callback: impl FnMut(&SocketAddr),
) -> io::Result<()> {
    define_with_ctx!(ctx_handle);
    peer = extensions::send_handshake(peer, ALL_SUPPORTED_EXTENSIONS).await?;

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

// ------------------------------------------------------------------------------------------------

async fn run_peer_connection(
    download_chans: pwp::DownloadChannels,
    upload_chans: pwp::UploadChannels,
    extended_chans: Option<pwp::ExtendedChannels>,
    content_storage: data::StorageClient,
    metainfo_storage: data::StorageClient,
    ctx_handle: MainHandle,
    mut peer_discovered_callback: impl FnMut(&SocketAddr),
) -> io::Result<()> {
    let remote_ip = *download_chans.0.remote_ip();

    let pwp::DownloadChannels(tx, rx) = download_chans;
    let download_fut = download::new_peer(ctx_handle.clone(), rx, tx, content_storage.clone());

    let pwp::UploadChannels(tx, rx) = upload_chans;
    let upload_fut = upload::new_peer(ctx_handle.clone(), rx, tx, content_storage);

    let handle_copy = ctx_handle.clone();
    let extensions_fut = async move {
        if let Some(pwp::ExtendedChannels(tx, rx)) = extended_chans {
            Ok(Some(extensions::new_peer(handle_copy, rx, tx, metainfo_storage).await?))
        } else {
            Ok(None)
        }
    };

    let (download, upload, extensions) = try_join!(download_fut, upload_fut, extensions_fut)?;
    try_join!(
        run_download(download.into(), remote_ip, ctx_handle.clone()),
        run_upload(upload.into(), remote_ip, ctx_handle.clone()),
        maybe_run_extensions!(extensions, remote_ip, ctx_handle, peer_discovered_callback),
    )?;
    Ok(())
}

// ------------------------------------------------------------------------------------------------

pub async fn outgoing_pwp_connection(
    remote_ip: SocketAddr,
    content_storage: data::StorageClient,
    metainfo_storage: data::StorageClient,
    mut ctx_handle: MainHandle,
    pwp_worker_handle: runtime::Handle,
    mut peer_discovered_callback: impl FnMut(&SocketAddr),
) -> io::Result<()> {
    define_with_ctx!(ctx_handle);

    loop {
        with_ctx!(|ctx| ctrl::grant_main_connection_permission(ctx, &remote_ip))?;

        let (info_hash, local_peer_id) = with_ctx!(|ctx| {
            ctx.connecting_to.insert(remote_ip);
            (*ctx.metainfo.info_hash(), *ctx.const_data.local_peer_id())
        });
        let connect_result = channels_for_outgoing_connection(
            &local_peer_id,
            &info_hash,
            EXTENSION_PROTOCOL_ENABLED,
            remote_ip,
            pwp_worker_handle.clone(),
        )
        .await;
        with_ctx!(|ctx| ctx.connecting_to.remove(&remote_ip));
        let (download_chans, upload_chans, extended_chans) = connect_result?;

        if let Err(e) = with_ctx!(|ctx| ctrl::grant_main_connection_permission(ctx, &remote_ip)) {
            if matches!(e, ctrl::GrantError::DuplicateConnection(_)) {
                return Err(e.into());
            }
        }

        let run_result = run_peer_connection(
            download_chans,
            upload_chans,
            extended_chans,
            content_storage.clone(),
            metainfo_storage.clone(),
            ctx_handle.clone(),
            &mut peer_discovered_callback,
        )
        .await;

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
    remote_ip: SocketAddr,
    content_storage: data::StorageClient,
    metainfo_storage: data::StorageClient,
    mut ctx_handle: MainHandle,
    pwp_worker_handle: runtime::Handle,
    mut peer_discovered_callback: impl FnMut(&SocketAddr),
) -> io::Result<()> {
    define_with_ctx!(ctx_handle);

    with_ctx!(|ctx| ctrl::grant_main_connection_permission(ctx, &remote_ip))?;
    let (info_hash, local_peer_id) =
        with_ctx!(|ctx| { (*ctx.metainfo.info_hash(), *ctx.const_data.local_peer_id()) });

    let (download_chans, upload_chans, extended_chans) = channels_for_incoming_connection(
        &local_peer_id,
        &info_hash,
        EXTENSION_PROTOCOL_ENABLED,
        remote_ip,
        stream,
        pwp_worker_handle.clone(),
    )
    .await?;

    let run_result = run_peer_connection(
        download_chans,
        upload_chans,
        extended_chans,
        content_storage.clone(),
        metainfo_storage.clone(),
        ctx_handle.clone(),
        &mut peer_discovered_callback,
    )
    .await;

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

// ------------------------------------------------------------------------------------------------

async fn run_metadata_download(
    download_chans: pwp::DownloadChannels,
    upload_chans: pwp::UploadChannels,
    extended_chans: Option<pwp::ExtendedChannels>,
    ctx_handle: PreliminaryHandle,
) -> io::Result<()> {
    let extended_chans = extended_chans.ok_or_else(|| {
        io::Error::new(io::ErrorKind::Unsupported, "peer doesn't support extension protocol")
    })?;

    async fn handle_download(download_chans: pwp::DownloadChannels) -> io::Result<()> {
        let pwp::DownloadChannels(tx, mut rx) = download_chans;
        loop {
            let msg = rx.receive_message().await?;
            log::trace!("Received {} from {}", msg, tx.remote_ip());
        }
    }
    async fn handle_upload(upload_chans: pwp::UploadChannels) -> io::Result<()> {
        let pwp::UploadChannels(tx, mut rx) = upload_chans;
        loop {
            let msg = rx.receive_message().await?;
            log::trace!("Received {} from {}", msg, tx.remote_ip());
        }
    }
    async fn handle_metadata(
        extended_chans: pwp::ExtendedChannels,
        mut ctx_handle: PreliminaryHandle,
    ) -> io::Result<()> {
        define_with_ctx!(ctx_handle);
        let mut peer = metadata::new_peer(ctx_handle.clone(), extended_chans).await?;
        while with_ctx!(|ctx| !ctrl::verify_metadata(ctx)) {
            match peer {
                metadata::Peer::Disabled(disabled) => {
                    peer = metadata::wait_until_enabled(disabled).await?.into();
                }
                metadata::Peer::Rejector(rejector) => {
                    let until = Instant::now() + sec!(10);
                    peer = metadata::cool_off_rejecting_peer(rejector, until).await?;
                }
                metadata::Peer::Uploader(uploader) => {
                    peer = metadata::download_metadata(uploader).await?;
                }
            }
        }
        Ok(())
    }
    try_join!(
        handle_download(download_chans),
        handle_upload(upload_chans),
        handle_metadata(extended_chans, ctx_handle),
    )?;
    Ok(())
}

pub async fn outgoing_preliminary_connection(
    remote_ip: SocketAddr,
    mut ctx_handle: PreliminaryHandle,
    pwp_worker_handle: runtime::Handle,
) -> io::Result<()> {
    define_with_ctx!(ctx_handle);

    with_ctx!(|ctx| ctrl::grant_preliminary_connection_permission(ctx, &remote_ip))?;
    let (info_hash, local_peer_id) =
        with_ctx!(|ctx| { (*ctx.magnet.info_hash(), *ctx.const_data.local_peer_id()) });

    let (download_chans, upload_chans, extended_chans) = channels_for_outgoing_connection(
        &local_peer_id,
        &info_hash,
        true, // extension_protocol_enabled
        remote_ip,
        pwp_worker_handle,
    )
    .await?;
    with_ctx!(|ctx| ctrl::grant_preliminary_connection_permission(ctx, &remote_ip))?;

    run_metadata_download(download_chans, upload_chans, extended_chans, ctx_handle).await
}

pub async fn incoming_preliminary_connection(
    stream: TcpStream,
    remote_ip: SocketAddr,
    mut ctx_handle: PreliminaryHandle,
    pwp_worker_handle: runtime::Handle,
) -> io::Result<()> {
    define_with_ctx!(ctx_handle);

    with_ctx!(|ctx| ctrl::grant_preliminary_connection_permission(ctx, &remote_ip))?;
    let (info_hash, local_peer_id) =
        with_ctx!(|ctx| { (*ctx.magnet.info_hash(), *ctx.const_data.local_peer_id()) });

    let (download_chans, upload_chans, extended_chans) = channels_for_incoming_connection(
        &local_peer_id,
        &info_hash,
        true, // extension_protocol_enabled
        remote_ip,
        stream,
        pwp_worker_handle,
    )
    .await?;

    run_metadata_download(download_chans, upload_chans, extended_chans, ctx_handle).await
}
