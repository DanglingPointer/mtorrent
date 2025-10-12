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

use super::connections::{PeerConnector, PeerReporter};
use super::{ctrl, ctx};
use local_async_utils::prelude::*;
use mtorrent_core::{data, pwp};
use std::io;
use std::rc::Rc;
use std::{net::SocketAddr, time::Duration};
use tokio::sync::broadcast;
use tokio::time::Instant;
use tokio::{runtime, try_join};

type MainHandle = ctx::Handle<ctx::MainCtx>;
type PreliminaryHandle = ctx::Handle<ctx::PreliminaryCtx>;

const EXTENSION_PROTOCOL_ENABLED: bool = true;

const ALL_SUPPORTED_EXTENSIONS: &[pwp::Extension] =
    &[pwp::Extension::Metadata, pwp::Extension::PeerExchange];

const CLIENT_NAME: &str = concat!(env!("CARGO_PKG_NAME"), " ", env!("CARGO_PKG_VERSION"));

const LOCAL_REQQ: usize = 1024 * 2;

const PEX_INTERVAL: Duration = sec!(30);

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

async fn run_optional_extensions(peer: Option<extensions::Peer>) -> io::Result<()> {
    match peer {
        Some(peer) => extensions::run(peer, PEX_INTERVAL).await,
        None => Ok(()),
    }
}

async fn run_peer_connection(
    origin: pwp::PeerOrigin,
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
                    data.peer_reporter.clone(),
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

    data.ctx_handle
        .clone()
        .with(|ctx| ctx.peer_states.set_origin(&remote_ip, origin));

    try_join!(
        run_download(download.into(), remote_ip, data.ctx_handle.clone()),
        run_upload(upload.into(), remote_ip, data.ctx_handle.clone()),
        run_optional_extensions(extensions),
        reporter.run(),
    )?;
    Ok(())
}

pub struct MainConnectionData {
    pub content_storage: data::StorageClient,
    pub metainfo_storage: data::StorageClient,
    pub ctx_handle: MainHandle,
    pub pwp_worker_handle: runtime::Handle,
    pub peer_reporter: PeerReporter,
    pub piece_downloaded_channel: Rc<broadcast::Sender<usize>>,
}

impl PeerConnector for MainConnectionData {
    type PeerConnection =
        (pwp::DownloadChannels, pwp::UploadChannels, Option<pwp::ExtendedChannels>);

    fn max_connections(&self) -> usize {
        200
    }

    fn connect_timeout(&self) -> Duration {
        sec!(15)
    }

    fn max_connect_retries(&self) -> usize {
        2
    }

    async fn outbound_connect_and_handshake(
        &self,
        peer_addr: SocketAddr,
    ) -> io::Result<Self::PeerConnection> {
        let mut handle = self.ctx_handle.clone();
        define_with_ctx!(handle);

        let (info_hash, local_peer_id, local_port) = with_ctx!(|ctx| (
            *ctx.metainfo.info_hash(),
            *ctx.const_data.local_peer_id(),
            ctx.const_data.pwp_local_tcp_port(),
        ));
        tcp::new_outbound_connection(
            &local_peer_id,
            &info_hash,
            EXTENSION_PROTOCOL_ENABLED,
            peer_addr,
            local_port,
            &self.pwp_worker_handle,
        )
        .await
    }

    async fn inbound_connect_and_handshake(
        &self,
        peer_addr: SocketAddr,
        stream: tokio::net::TcpStream,
    ) -> io::Result<Self::PeerConnection> {
        let mut handle = self.ctx_handle.clone();
        define_with_ctx!(handle);

        let (info_hash, local_peer_id) =
            with_ctx!(|ctx| (*ctx.metainfo.info_hash(), *ctx.const_data.local_peer_id()));
        tcp::new_inbound_connection(
            &local_peer_id,
            &info_hash,
            EXTENSION_PROTOCOL_ENABLED,
            peer_addr,
            stream,
            &self.pwp_worker_handle,
        )
        .await
    }

    async fn run_connection(
        &self,
        origin: pwp::PeerOrigin,
        connection: Self::PeerConnection,
    ) -> io::Result<()> {
        let (download_chans, upload_chans, extended_chans) = connection;
        run_peer_connection(origin, download_chans, upload_chans, extended_chans, self).await
    }
}

// ------------------------------------------------------------------------------------------------

async fn run_metadata_download(
    origin: pwp::PeerOrigin,
    download_chans: pwp::DownloadChannels,
    upload_chans: pwp::UploadChannels,
    extended_chans: pwp::ExtendedChannels,
    mut ctx_handle: PreliminaryHandle,
    peer_reporter: PeerReporter,
) -> io::Result<()> {
    ctx_handle.with(|ctx| ctx.reachable_peers.insert(*download_chans.0.remote_ip()));

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
        origin: pwp::PeerOrigin,
        extended_chans: pwp::ExtendedChannels,
        mut ctx_handle: PreliminaryHandle,
        peer_reporter: PeerReporter,
    ) -> io::Result<()> {
        define_with_ctx!(ctx_handle);
        let peer_addr = *extended_chans.0.remote_ip();

        let mut peer =
            metadata::new_peer(ctx_handle.clone(), extended_chans, peer_reporter).await?;
        with_ctx!(|ctx| {
            // the bookkeeping below must be done _after_ creating the peer above,
            // otherwise it will be never undone in the case of a send/recv error
            ctx.peer_states.set_origin(&peer_addr, origin);
        });
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
        handle_metadata(origin, extended_chans, ctx_handle, peer_reporter),
    )?;
    Ok(())
}

pub struct PreliminaryConnectionData {
    pub ctx_handle: PreliminaryHandle,
    pub pwp_worker_handle: runtime::Handle,
    pub peer_reporter: PeerReporter,
}

impl PeerConnector for PreliminaryConnectionData {
    type PeerConnection = (pwp::DownloadChannels, pwp::UploadChannels, pwp::ExtendedChannels);

    fn max_connections(&self) -> usize {
        50
    }

    fn connect_timeout(&self) -> Duration {
        sec!(5)
    }

    fn max_connect_retries(&self) -> usize {
        1
    }

    async fn outbound_connect_and_handshake(
        &self,
        peer_addr: SocketAddr,
    ) -> io::Result<Self::PeerConnection> {
        let mut handle = self.ctx_handle.clone();
        define_with_ctx!(handle);

        let (info_hash, local_peer_id, local_port) = with_ctx!(|ctx| (
            *ctx.magnet.info_hash(),
            *ctx.const_data.local_peer_id(),
            ctx.const_data.pwp_local_tcp_port(),
        ));
        let (dl, ul, ext) = tcp::new_outbound_connection(
            &local_peer_id,
            &info_hash,
            true, // extension_protocol_enabled
            peer_addr,
            local_port,
            &self.pwp_worker_handle,
        )
        .await?;
        let ext = ext.ok_or_else(|| {
            io::Error::new(io::ErrorKind::Unsupported, "peer doesn't support extension protocol")
        })?;
        Ok((dl, ul, ext))
    }

    async fn inbound_connect_and_handshake(
        &self,
        peer_addr: SocketAddr,
        stream: tokio::net::TcpStream,
    ) -> io::Result<Self::PeerConnection> {
        let mut handle = self.ctx_handle.clone();
        define_with_ctx!(handle);

        let (info_hash, local_peer_id) =
            with_ctx!(|ctx| (*ctx.magnet.info_hash(), *ctx.const_data.local_peer_id()));
        let (dl, ul, ext) = tcp::new_inbound_connection(
            &local_peer_id,
            &info_hash,
            true, // extension_protocol_enabled
            peer_addr,
            stream,
            &self.pwp_worker_handle,
        )
        .await?;
        let ext = ext.ok_or_else(|| {
            io::Error::new(io::ErrorKind::Unsupported, "peer doesn't support extension protocol")
        })?;
        Ok((dl, ul, ext))
    }

    async fn run_connection(
        &self,
        origin: pwp::PeerOrigin,
        connection: Self::PeerConnection,
    ) -> io::Result<()> {
        let (download_chans, upload_chans, extended_chans) = connection;
        run_metadata_download(
            origin,
            download_chans,
            upload_chans,
            extended_chans,
            self.ctx_handle.clone(),
            self.peer_reporter.clone(),
        )
        .await
    }
}
