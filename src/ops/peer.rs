mod download;
mod extensions;
mod metadata;
mod tcp;
mod upload;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod testutils;

pub use tcp::run_listener as run_pwp_listener;

use super::connections::{IncomingConnectionPermit, OutgoingConnectionPermit};
use super::{ctrl, ctx};
use crate::utils::local_sync;
use crate::{data, pwp, sec};
use std::io;
use std::rc::Rc;
use std::{net::SocketAddr, time::Duration};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::time::{self, Instant};
use tokio::{runtime, try_join};

type MainHandle = ctx::Handle<ctx::MainCtx>;
type PreliminaryHandle = ctx::Handle<ctx::PreliminaryCtx>;

const EXTENSION_PROTOCOL_ENABLED: bool = true;

const ALL_SUPPORTED_EXTENSIONS: &[pwp::Extension] =
    &[pwp::Extension::Metadata, pwp::Extension::PeerExchange];

const CLIENT_NAME: &str = concat!(env!("CARGO_PKG_NAME"), " ", env!("CARGO_PKG_VERSION"));

const LOCAL_REQQ: usize = 1024 * 2;

// ------------------------------------------------------------------------------------------------

async fn run_download(
    mut peer: download::Peer,
    remote_ip: SocketAddr,
    mut ctx_handle: MainHandle,
) -> io::Result<()> {
    define_with_ctx!(ctx_handle);
    loop {
        with_ctx!(|ctx| ctrl::validate_peer_utility(&remote_ip, ctx))?;
        match peer {
            download::Peer::Idle(idling_peer) => {
                match with_ctx!(|ctx| ctrl::idle_download_next_action(&remote_ip, ctx)) {
                    ctrl::IdleDownloadAction::ActivateDownload => {
                        peer = download::activate(idling_peer).await?;
                    }
                    ctrl::IdleDownloadAction::WaitForUpdates(timeout) => {
                        peer =
                            download::linger(idling_peer.into(), Instant::now() + timeout).await?;
                    }
                }
            }
            download::Peer::Seeder(seeding_peer) => {
                match with_ctx!(|ctx| ctrl::active_download_next_action(&remote_ip, ctx)) {
                    ctrl::SeederDownloadAction::RequestPieces => {
                        peer = download::get_pieces(seeding_peer).await?;
                    }
                    ctrl::SeederDownloadAction::WaitForUpdates(timeout) => {
                        peer =
                            download::linger(seeding_peer.into(), Instant::now() + timeout).await?;
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
        with_ctx!(|ctx| ctrl::validate_peer_utility(&remote_ip, ctx))?;
        match peer {
            upload::Peer::Idle(idling_peer) => {
                match with_ctx!(|ctx| ctrl::idle_upload_next_action(&remote_ip, &ctx.peer_states)) {
                    ctrl::IdleUploadAction::ActivateUploadAndServe(duration) => {
                        let leeching_peer = upload::activate(idling_peer).await?;
                        peer = upload::serve_pieces(leeching_peer, duration).await?;
                    }
                    ctrl::IdleUploadAction::Linger(timeout) => {
                        peer = upload::linger(idling_peer, timeout).await?;
                    }
                }
            }
            upload::Peer::Leech(leeching_peer) => {
                match with_ctx!(|ctx| ctrl::active_upload_next_action(&remote_ip, &ctx.peer_states))
                {
                    ctrl::LeechUploadAction::DeactivateUploadAndLinger(timeout) => {
                        let idling_peer = upload::deactivate(leeching_peer).await?;
                        peer = upload::linger(idling_peer, timeout).await?;
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
) -> io::Result<()> {
    define_with_ctx!(ctx_handle);

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
        )
        .await?;
    }
}

macro_rules! maybe_run_extensions {
    ($peer:expr, $remote_ip:expr, $ctx_handle:expr) => {
        async {
            if let Some(peer) = $peer {
                run_extensions(peer, $remote_ip, $ctx_handle.clone()).await
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
    data: &MainConnectionData,
) -> io::Result<()> {
    let remote_ip = *download_chans.0.remote_ip();

    let pwp::UploadChannels(tx, rx) = upload_chans;
    let upload_fut = upload::new_peer(
        data.ctx_handle.clone(),
        rx,
        tx,
        data.content_storage.clone(),
        data.piece_downloaded_channel.subscribe(),
    );

    let pwp::DownloadChannels(tx, rx) = download_chans;
    let download_fut = download::new_peer(
        data.ctx_handle.clone(),
        rx,
        tx,
        data.content_storage.clone(),
        data.piece_downloaded_channel.clone(),
    );

    let handle_copy = data.ctx_handle.clone();
    let extensions_fut = async move {
        if let Some(pwp::ExtendedChannels(tx, rx)) = extended_chans {
            Ok(Some(
                extensions::new_peer(
                    handle_copy,
                    rx,
                    tx,
                    data.metainfo_storage.clone(),
                    ALL_SUPPORTED_EXTENSIONS,
                    data.peer_discovered_channel.clone(),
                )
                .await?,
            ))
        } else {
            Ok(None)
        }
    };

    // create extensions before upload so that we send extended handshake before bitfield
    let (extensions, download, (upload, reporter)) =
        try_join!(extensions_fut, download_fut, upload_fut)?;
    try_join!(
        run_download(download.into(), remote_ip, data.ctx_handle.clone()),
        run_upload(upload.into(), remote_ip, data.ctx_handle.clone()),
        maybe_run_extensions!(extensions, remote_ip, data.ctx_handle),
        reporter.run(),
    )?;
    Ok(())
}

// ------------------------------------------------------------------------------------------------

pub struct MainConnectionData {
    pub content_storage: data::StorageClient,
    pub metainfo_storage: data::StorageClient,
    pub ctx_handle: MainHandle,
    pub pwp_worker_handle: runtime::Handle,
    pub peer_discovered_channel: local_sync::channel::Sender<SocketAddr>,
    pub piece_downloaded_channel: Rc<broadcast::Sender<usize>>,
}

pub async fn outgoing_pwp_connection(
    remote_ip: SocketAddr,
    permit: OutgoingConnectionPermit<MainConnectionData>,
) -> io::Result<()> {
    let mut handle = permit.0.ctx_handle.clone();
    define_with_ctx!(handle);

    loop {
        let (info_hash, local_peer_id, local_port) = with_ctx!(|ctx| (
            *ctx.metainfo.info_hash(),
            *ctx.const_data.local_peer_id(),
            ctx.const_data.pwp_local_tcp_port(),
        ));
        let (download_chans, upload_chans, extended_chans) = tcp::channels_for_outgoing_connection(
            &local_peer_id,
            &info_hash,
            EXTENSION_PROTOCOL_ENABLED,
            remote_ip,
            local_port,
            permit.0.pwp_worker_handle.clone(),
            false,
        )
        .await?;
        let connected_time = Instant::now();

        let run_result =
            run_peer_connection(download_chans, upload_chans, extended_chans, &permit.0).await;

        match run_result {
            Err(e) if e.kind() != io::ErrorKind::Other && connected_time.elapsed() > sec!(5) => {
                // ErrorKind::Other means we disconnected the peer intentionally, and
                // <5s since connect means peer probably didn't like our handshake
                log::warn!("Peer {remote_ip} disconnected: {e}. Reconnecting in 1s...");
                time::sleep(sec!(1)).await;
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
    permit: IncomingConnectionPermit<MainConnectionData>,
) -> io::Result<()> {
    let mut handle = permit.0.ctx_handle.clone();
    define_with_ctx!(handle);

    let (info_hash, local_peer_id) =
        with_ctx!(|ctx| (*ctx.metainfo.info_hash(), *ctx.const_data.local_peer_id()));

    let (download_chans, upload_chans, extended_chans) = tcp::channels_for_incoming_connection(
        &local_peer_id,
        &info_hash,
        EXTENSION_PROTOCOL_ENABLED,
        remote_ip,
        stream,
        permit.0.pwp_worker_handle.clone(),
    )
    .await?;

    let run_result =
        run_peer_connection(download_chans, upload_chans, extended_chans, &permit.0).await;

    match run_result {
        Err(e) if e.kind() != io::ErrorKind::Other => {
            // ErrorKind::Other means we disconnected the peer intentionally
            log::warn!("Peer {remote_ip} disconnected: {e}. Reconnecting...");
            outgoing_pwp_connection(remote_ip, OutgoingConnectionPermit(permit.0)).await
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

pub struct PreliminaryConnectionData {
    pub ctx_handle: PreliminaryHandle,
    pub pwp_worker_handle: runtime::Handle,
}

pub async fn outgoing_preliminary_connection(
    remote_ip: SocketAddr,
    permit: OutgoingConnectionPermit<PreliminaryConnectionData>,
) -> io::Result<()> {
    let mut handle = permit.0.ctx_handle.clone();
    define_with_ctx!(handle);

    let (info_hash, local_peer_id, local_port) = with_ctx!(|ctx| (
        *ctx.magnet.info_hash(),
        *ctx.const_data.local_peer_id(),
        ctx.const_data.pwp_local_tcp_port(),
    ));

    let (download_chans, upload_chans, extended_chans) = tcp::channels_for_outgoing_connection(
        &local_peer_id,
        &info_hash,
        true, // extension_protocol_enabled
        remote_ip,
        local_port,
        permit.0.pwp_worker_handle.clone(),
        true,
    )
    .await?;

    run_metadata_download(download_chans, upload_chans, extended_chans, handle).await
}

pub async fn incoming_preliminary_connection(
    stream: TcpStream,
    remote_ip: SocketAddr,
    permit: IncomingConnectionPermit<PreliminaryConnectionData>,
) -> io::Result<()> {
    let mut handle = permit.0.ctx_handle.clone();
    define_with_ctx!(handle);

    let (info_hash, local_peer_id) =
        with_ctx!(|ctx| (*ctx.magnet.info_hash(), *ctx.const_data.local_peer_id()));

    let (download_chans, upload_chans, extended_chans) = tcp::channels_for_incoming_connection(
        &local_peer_id,
        &info_hash,
        true, // extension_protocol_enabled
        remote_ip,
        stream,
        permit.0.pwp_worker_handle.clone(),
    )
    .await?;

    run_metadata_download(download_chans, upload_chans, extended_chans, handle).await
}
