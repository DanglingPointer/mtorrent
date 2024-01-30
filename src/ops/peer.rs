use super::{ctx, download, upload};
use crate::{data, pwp, sec};
use std::{io, net::SocketAddr};
use tokio::{net::TcpStream, runtime, time::sleep, try_join};

pub async fn from_incoming_connection(
    stream: TcpStream,
    local_peer_id: [u8; 20],
    storage: data::StorageClient,
    mut ctx_handle: ctx::Handle,
    pwp_worker_handle: runtime::Handle,
) -> io::Result<(download::IdlePeer, upload::IdlePeer)> {
    let remote_ip = stream.peer_addr()?;

    let info_hash: [u8; 20] = ctx_handle.with_ctx(|ctx| *ctx.metainfo.info_hash());
    let (download_chans, upload_chans, runner) =
        pwp::channels_from_incoming(&local_peer_id, Some(&info_hash), stream)
            .await
            .map_err(|e| {
                log::error!("Failed to establish an incoming connection to {remote_ip}: {e}");
                e
            })?;
    log::info!("Successfully established an incoming connection to {remote_ip}");

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

pub async fn from_outgoing_connection(
    remote_ip: SocketAddr,
    local_peer_id: [u8; 20],
    storage: data::StorageClient,
    mut ctx_handle: ctx::Handle,
    pwp_worker_handle: runtime::Handle,
) -> io::Result<(download::IdlePeer, upload::IdlePeer)> {
    const MAX_RETRY_COUNT: usize = 3;
    let mut attempts_left = MAX_RETRY_COUNT;
    let mut reconnect_interval = sec!(2);

    let info_hash: [u8; 20] = ctx_handle.with_ctx(|ctx| *ctx.metainfo.info_hash());
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
