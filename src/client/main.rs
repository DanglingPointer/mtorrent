use crate::utils::peer_id::PeerId;
use crate::utils::{ip, magnet, startup, upnp};
use crate::{data, ops};
use std::io;
use std::net::{SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use tokio::net::TcpStream;
use tokio::{runtime, task};

fn peer_discovered_callback_factory(
    content_storage: data::StorageClient,
    metainfo_storage: data::StorageClient,
    ctx_handle: ops::Handle<ops::MainCtx>,
    pwp_worker_handle: runtime::Handle,
) -> impl FnMut(&SocketAddr) + Clone {
    move |remote_ip| {
        let content_storage = content_storage.clone();
        let metainfo_storage = metainfo_storage.clone();
        let ctx_handle = ctx_handle.clone();
        let pwp_worker_handle = pwp_worker_handle.clone();
        let remote_ip = *remote_ip;
        task::spawn_local(async move {
            let cb = peer_discovered_callback_factory(
                content_storage.clone(),
                metainfo_storage.clone(),
                ctx_handle.clone(),
                pwp_worker_handle.clone(),
            );
            match ops::outgoing_pwp_connection(
                remote_ip,
                content_storage,
                metainfo_storage,
                ctx_handle,
                pwp_worker_handle,
                cb,
            )
            .await
            {
                Ok(_) => (),
                Err(e) => log::error!("Outgoing peer connection to {remote_ip} failed: {e}"),
            }
        });
    }
}

pub async fn single_torrent(
    local_peer_id: PeerId,
    metainfo_uri: &str,
    output_dir: impl AsRef<Path>,
    pwp_runtime: runtime::Handle,
    storage_runtime: runtime::Handle,
) -> io::Result<()> {
    let listener_addr = ip::any_socketaddr_from_hash(&metainfo_uri);
    // get public ip to send correct listening port to trackers later
    let public_pwp_ip = match upnp::PortOpener::new(
        SocketAddrV4::new(ip::get_local_addr()?, listener_addr.port()),
        igd::PortMappingProtocol::TCP,
    )
    .await
    {
        Ok(port_opener) => {
            let public_ipv4 = port_opener.external_ip();
            log::info!("UPnP succeeded, public ip: {}", public_ipv4);
            pwp_runtime.spawn(async move {
                if let Err(e) = port_opener.do_continuous_renewal().await {
                    log::error!("UPnP port renewal failed: {e}");
                }
            });
            SocketAddr::V4(public_ipv4)
        }
        Err(e) => {
            log::error!("UPnP failed: {e}");
            listener_addr
        }
    };

    if Path::new(metainfo_uri).is_file() {
        main_stage(
            local_peer_id,
            listener_addr,
            public_pwp_ip,
            metainfo_uri,
            output_dir,
            pwp_runtime,
            storage_runtime,
            std::iter::empty(),
        )
        .await?;
    } else {
        let (metainfo_filepath, peers) = preliminary_stage(
            local_peer_id,
            listener_addr,
            public_pwp_ip,
            metainfo_uri,
            &output_dir,
            &output_dir,
            pwp_runtime.clone(),
        )
        .await?;
        log::info!("Metadata downloaded successfully, starting content download");
        main_stage(
            local_peer_id,
            listener_addr,
            public_pwp_ip,
            metainfo_filepath,
            &output_dir,
            pwp_runtime,
            storage_runtime,
            peers,
        )
        .await?;
    }
    Ok(())
}

async fn preliminary_stage(
    local_peer_id: PeerId,
    listener_addr: SocketAddr,
    public_pwp_ip: SocketAddr,
    magnet_link: impl AsRef<str>,
    config_dir: impl AsRef<Path>,
    metainfo_dir: impl AsRef<Path>,
    pwp_runtime: runtime::Handle,
) -> io::Result<(impl AsRef<Path>, impl IntoIterator<Item = SocketAddr>)> {
    let magnet_link: magnet::MagnetLink = magnet_link
        .as_ref()
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, Box::new(e)))
        .inspect_err(|e| log::error!("Invalid magnet link: {e}"))?;

    let metainfo_filepath = metainfo_dir
        .as_ref()
        .join(format!("{}.torrent", magnet_link.name().unwrap_or("unnamed")));

    let local_task = task::LocalSet::new();

    let ctx = ops::PreliminaryCtx::new(magnet_link, local_peer_id, public_pwp_ip);

    let incoming_connection_pwp_runtime = pwp_runtime.clone();
    let incoming_connection_ctx = ctx.clone();
    let on_incoming_connection = move |stream: TcpStream| {
        let pwp_runtime = incoming_connection_pwp_runtime.clone();
        let ctx = incoming_connection_ctx.clone();
        task::spawn_local(async move {
            let peer_ip =
                stream.peer_addr().map_or_else(|_| "<N/A>".to_owned(), |ip| format!("{ip}"));
            match ops::incoming_preliminary_connection(stream, ctx, pwp_runtime).await {
                Ok(_) => (),
                Err(e) => log::error!("Incoming peer connection from {peer_ip} failed: {e}"),
            }
        });
    };

    local_task.spawn_local(async move {
        match ops::run_pwp_listener(listener_addr, on_incoming_connection).await {
            Ok(_) => (),
            Err(e) => log::error!("TCP listener exited: {e}"),
        }
    });

    let announce_response_pwp_runtime = pwp_runtime.clone();
    let announce_response_ctx = ctx.clone();
    let on_announce_response = move |response: ops::TrackerResponse| {
        for peer_ip in response.peers {
            let pwp_runtime = announce_response_pwp_runtime.clone();
            let ctx = announce_response_ctx.clone();
            task::spawn_local(async move {
                match ops::outgoing_preliminary_connection(peer_ip, ctx, pwp_runtime).await {
                    Ok(_) => (),
                    Err(e) => log::error!("Outgoing peer connection to {peer_ip} failed: {e}"),
                }
            });
        }
    };

    let tracker_ctx = ctx.clone();
    let tracker_config_dir = PathBuf::from(config_dir.as_ref());
    local_task.spawn_local(async move {
        ops::make_preliminary_announces(tracker_ctx, tracker_config_dir, on_announce_response)
            .await;
    });

    let metainfo_filepath_copy = metainfo_filepath.clone();
    let peers = local_task
        .run_until(async move {
            ops::periodic_metadata_check(ctx, config_dir, metainfo_filepath_copy).await
        })
        .await?;
    Ok((metainfo_filepath, peers))
}

#[allow(clippy::too_many_arguments)]
async fn main_stage(
    local_peer_id: PeerId,
    listener_addr: SocketAddr,
    public_pwp_ip: SocketAddr,
    metainfo_filepath: impl AsRef<Path>,
    output_dir: impl AsRef<Path>,
    pwp_runtime: runtime::Handle,
    storage_runtime: runtime::Handle,
    extra_peers: impl IntoIterator<Item = SocketAddr>,
) -> io::Result<()> {
    let metainfo = startup::read_metainfo(&metainfo_filepath)
        .inspect_err(|e| log::error!("Invalid metainfo file: {e}"))?;

    let content_dir = Path::new(output_dir.as_ref())
        .join(metainfo_filepath.as_ref().file_stem().unwrap_or_default());

    let (content_storage, content_storage_server) =
        startup::create_content_storage(&metainfo, content_dir)?;
    storage_runtime.spawn(async move {
        content_storage_server.run().await;
    });
    let (metainfo_storage, metainfo_storage_server) =
        startup::create_metainfo_storage(&metainfo_filepath)?;
    storage_runtime.spawn(async move {
        metainfo_storage_server.run().await;
    });

    let local_task = task::LocalSet::new();

    let ctx = ops::MainCtx::new(metainfo, local_peer_id, public_pwp_ip)?;

    let incoming_connection_content_storage = content_storage.clone();
    let incoming_connection_metainfo_storage = metainfo_storage.clone();
    let incoming_connection_pwp_runtime = pwp_runtime.clone();
    let incoming_connection_ctx = ctx.clone();
    let on_incoming_connection = move |stream: TcpStream| {
        let content_storage = incoming_connection_content_storage.clone();
        let metainfo_storage = incoming_connection_metainfo_storage.clone();
        let pwp_runtime = incoming_connection_pwp_runtime.clone();
        let ctx = incoming_connection_ctx.clone();
        task::spawn_local(async move {
            let peer_ip =
                stream.peer_addr().map_or_else(|_| "<N/A>".to_owned(), |ip| format!("{ip}"));
            let cb = peer_discovered_callback_factory(
                content_storage.clone(),
                metainfo_storage.clone(),
                ctx.clone(),
                pwp_runtime.clone(),
            );
            match ops::incoming_pwp_connection(
                stream,
                content_storage,
                metainfo_storage,
                ctx,
                pwp_runtime,
                cb,
            )
            .await
            {
                Ok(_) => (),
                Err(e) => log::error!("Incoming peer connection from {peer_ip} failed: {e}"),
            }
        });
    };

    local_task.spawn_local(async move {
        match ops::run_pwp_listener(listener_addr, on_incoming_connection).await {
            Ok(_) => (),
            Err(e) => log::error!("TCP listener exited: {e}"),
        }
    });

    let mut cb = peer_discovered_callback_factory(
        content_storage.clone(),
        metainfo_storage.clone(),
        ctx.clone(),
        pwp_runtime.clone(),
    );

    {
        let _g = local_task.enter();
        for peer_ip in extra_peers.into_iter() {
            cb(&peer_ip);
        }
    }

    let on_announce_response = move |response: ops::TrackerResponse| {
        for peer_ip in response.peers {
            cb(&peer_ip);
        }
    };
    let tracker_ctx = ctx.clone();
    let config_dir = PathBuf::from(output_dir.as_ref());
    local_task.spawn_local(async move {
        ops::make_periodic_announces(tracker_ctx, config_dir, on_announce_response).await;
    });

    local_task
        .run_until(async move {
            ops::periodic_state_dump(ctx, output_dir).await;
        })
        .await;
    Ok(())
}
