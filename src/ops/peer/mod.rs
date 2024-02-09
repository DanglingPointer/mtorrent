mod download;
mod upload;

#[cfg(test)]
mod tests;

use super::{ctrl, ctx};
use crate::{data, pwp, sec};
use std::{io, net::SocketAddr, ops::Deref};
use tokio::{net::TcpStream, runtime, time::sleep, try_join};

async fn from_incoming_connection(
    stream: TcpStream,
    storage: data::StorageClient,
    mut ctx_handle: ctx::Handle,
    pwp_worker_handle: runtime::Handle,
) -> io::Result<(download::IdlePeer, upload::IdlePeer)> {
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

    let (download_chans, upload_chans, runner) =
        pwp::channels_from_incoming(&local_peer_id, Some(&info_hash), stream)
            .await
            .map_err(|e| {
                log::error!("Failed to establish an incoming connection with {remote_ip}: {e}");
                e
            })?;
    log::info!("Successfully established an incoming connection with {remote_ip}");

    pwp_worker_handle.spawn(async move {
        if let Err(e) = runner.run().await {
            log::debug!("Peer runner exited: {}", e);
        }
    });

    let pwp::DownloadChannels(tx, rx) = download_chans;
    let download_fut = download::new_peer(ctx_handle.clone(), rx, tx, storage.clone());

    let pwp::UploadChannels(tx, rx) = upload_chans;
    let upload_fut = upload::new_peer(ctx_handle, rx, tx, storage);

    let (seeder, leech) = try_join!(download_fut, upload_fut)?;
    Ok((seeder, leech))
}

async fn from_outgoing_connection(
    remote_ip: SocketAddr,
    storage: data::StorageClient,
    mut ctx_handle: ctx::Handle,
    pwp_worker_handle: runtime::Handle,
) -> io::Result<(download::IdlePeer, upload::IdlePeer)> {
    define_with_ctx!(ctx_handle);

    if with_ctx!(|ctx| ctx.peer_states.get(&remote_ip).is_some()) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("already connected to {remote_ip}"),
        ));
    }

    let (info_hash, local_peer_id) =
        with_ctx!(|ctx| { (*ctx.metainfo.info_hash(), *ctx.local_peer_id.deref()) });

    log::debug!("Connecting to {remote_ip}...");
    const MAX_RETRY_COUNT: usize = 3;
    let mut attempts_left = MAX_RETRY_COUNT;
    let mut reconnect_interval = sec!(2);
    let (download_chans, upload_chans, runner) = loop {
        match pwp::channels_from_outgoing(&local_peer_id, &info_hash, remote_ip, None).await {
            Ok(channels) => break channels,
            Err(e) => match e.kind() {
                io::ErrorKind::ConnectionRefused | io::ErrorKind::ConnectionReset
                    if attempts_left > 0 =>
                {
                    sleep(reconnect_interval).await;
                    attempts_left -= 1;
                    reconnect_interval *= 2;
                }
                _ => {
                    log::error!("Failed to establish an outgoing connection to {remote_ip}: {e}");
                    return Err(e);
                }
            },
        }
    };
    log::info!("Successfully established an outgoing connection to {remote_ip}");

    pwp_worker_handle.spawn(async move {
        if let Err(e) = runner.run().await {
            log::debug!("Peer runner exited: {}", e);
        }
    });

    let pwp::DownloadChannels(tx, rx) = download_chans;
    let download_fut = download::new_peer(ctx_handle.clone(), rx, tx, storage.clone());

    let pwp::UploadChannels(tx, rx) = upload_chans;
    let upload_fut = upload::new_peer(ctx_handle, rx, tx, storage);

    let (seeder, leech) = try_join!(download_fut, upload_fut)?;
    Ok((seeder, leech))
}

async fn run_download(
    mut peer: download::Peer,
    remote_ip: SocketAddr,
    mut ctx_handle: ctx::Handle,
) -> io::Result<()> {
    define_with_ctx!(ctx_handle);
    loop {
        match peer {
            download::Peer::Idle(idling_peer) => {
                if with_ctx!(|ctx| ctrl::should_activate_download(&remote_ip, ctx)) {
                    let seeder = download::activate(idling_peer).await?;
                    peer = seeder.into();
                } else {
                    peer = download::linger(idling_peer.into(), sec!(10)).await?;
                }
            }
            download::Peer::Seeder(seeding_peer) => {
                let requests = with_ctx!(|ctx| ctrl::pieces_to_request(&remote_ip, ctx));
                if !requests.is_empty() {
                    peer = download::get_pieces(seeding_peer, requests.iter()).await?;
                } else {
                    peer = download::deactivate(seeding_peer).await?.into();
                }
            }
        }
    }
}

async fn run_upload(
    mut peer: upload::Peer,
    remote_ip: SocketAddr,
    mut ctx_handle: ctx::Handle,
) -> io::Result<()> {
    define_with_ctx!(ctx_handle);
    loop {
        peer = upload::update_peer(peer).await?;
        match peer {
            upload::Peer::Idle(idling_peer) => {
                if with_ctx!(|ctx| ctrl::should_activate_upload(&remote_ip, ctx)) {
                    let leech = upload::activate(idling_peer).await?;
                    peer = leech.into();
                } else {
                    peer = upload::linger(idling_peer, sec!(10)).await?;
                }
            }
            upload::Peer::Leech(leeching_peer) => {
                if with_ctx!(|ctx| ctrl::should_stop_upload(&remote_ip, ctx)) {
                    let idling_peer = upload::deactivate(leeching_peer).await?;
                    peer = upload::linger(idling_peer, sec!(10)).await?;
                } else {
                    peer = upload::serve_pieces(leeching_peer, sec!(30)).await?;
                }
            }
        }
    }
}

pub async fn outgoing_pwp_connection(
    remote_ip: SocketAddr,
    storage: data::StorageClient,
    ctx_handle: ctx::Handle,
    pwp_worker_handle: runtime::Handle,
) -> io::Result<()> {
    let (download, upload) =
        from_outgoing_connection(remote_ip, storage, ctx_handle.clone(), pwp_worker_handle).await?;
    try_join!(
        run_download(download.into(), remote_ip, ctx_handle.clone()),
        run_upload(upload.into(), remote_ip, ctx_handle.clone())
    )?;
    Ok(())
}

pub async fn incoming_pwp_connection(
    stream: TcpStream,
    storage: data::StorageClient,
    ctx_handle: ctx::Handle,
    pwp_worker_handle: runtime::Handle,
) -> io::Result<()> {
    let remote_ip = stream.peer_addr()?;
    let (download, upload) =
        from_incoming_connection(stream, storage, ctx_handle.clone(), pwp_worker_handle).await?;
    try_join!(
        run_download(download.into(), remote_ip, ctx_handle.clone()),
        run_upload(upload.into(), remote_ip, ctx_handle.clone())
    )?;
    Ok(())
}
