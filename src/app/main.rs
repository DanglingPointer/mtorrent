use crate::ops;
use crate::utils::peer_id::PeerId;
use crate::utils::{fifo, ip, magnet, startup, upnp};
use futures::StreamExt;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::Path;
use std::rc::Rc;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::{runtime, task};

pub async fn single_torrent(
    local_peer_id: PeerId,
    metainfo_uri: &str,
    output_dir: impl AsRef<Path>,
    pwp_runtime: runtime::Handle,
    storage_runtime: runtime::Handle,
    use_upnp: bool,
) -> io::Result<()> {
    let listener_addr =
        SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), ip::port_from_hash(&metainfo_uri));
    // get public ip to send correct listening port to trackers and peers later
    let public_pwp_ip = if use_upnp {
        match upnp::PortOpener::new(
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
        }
    } else {
        listener_addr
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

const MAX_PRELIMINARY_CONNECTIONS: usize = 10;
const MAX_PEER_CONNECTIONS: usize = 200;

macro_rules! log {
    ($e:expr, $($arg:tt)+) => {{
        let lvl = if $e.kind() == io::ErrorKind::Other {
            log::Level::Error
        } else {
            log::Level::Debug
        };
        log::log!(lvl, $($arg)+);
    }}
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
    let extra_peers: Vec<SocketAddr> = magnet_link.peers().cloned().collect();

    let metainfo_filepath = metainfo_dir
        .as_ref()
        .join(format!("{}.torrent", magnet_link.name().unwrap_or("unnamed")));

    let local_task = task::LocalSet::new();

    let ctx =
        ops::PreliminaryCtx::new(magnet_link, local_peer_id, public_pwp_ip, listener_addr.port());

    let (peer_discovered_sink, mut peer_discovered_src) = fifo::channel::<SocketAddr>();
    let (mut outgoing_ctrl, mut incoming_ctrl) = ops::connection_control(
        MAX_PRELIMINARY_CONNECTIONS,
        ops::PreliminaryConnectionData {
            ctx_handle: ctx.clone(),
            pwp_worker_handle: pwp_runtime,
        },
    );
    local_task.spawn_local(async move {
        while let Some(peer_addr) = peer_discovered_src.next().await {
            if let Some(permit) = outgoing_ctrl.issue_permit(peer_addr).await {
                task::spawn_local(async move {
                    log::debug!("Connecting to {peer_addr}...");
                    ops::outgoing_preliminary_connection(peer_addr, permit).await.unwrap_or_else(
                        |e| log!(e, "Outgoing peer connection to {peer_addr} failed: {e}"),
                    );
                });
            } else {
                log::debug!("Outgoing connection to {peer_addr} denied");
            }
        }
    });

    let on_incoming_connection = move |stream: TcpStream, peer_ip: SocketAddr| {
        if let Some(permit) = incoming_ctrl.issue_permit() {
            task::spawn_local(async move {
                ops::incoming_preliminary_connection(stream, peer_ip, permit)
                    .await
                    .unwrap_or_else(|e| {
                        log!(e, "Incoming peer connection from {peer_ip} failed: {e}")
                    });
            });
        } else {
            log::info!("Incoming connection from {peer_ip} rejected");
        }
    };
    local_task.spawn_local(async move {
        match ops::run_pwp_listener(listener_addr, on_incoming_connection).await {
            Ok(_) => (),
            Err(e) => log::error!("TCP listener exited: {e}"),
        }
    });

    for peer_ip in extra_peers {
        peer_discovered_sink.send(peer_ip);
    }

    let tracker_ctx = ctx.clone();
    let config_dir = config_dir.as_ref().to_owned();
    local_task.spawn_local(async move {
        ops::make_preliminary_announces(tracker_ctx, config_dir, peer_discovered_sink).await;
    });

    let metainfo_filepath_copy = metainfo_filepath.clone();
    let peers = local_task
        .run_until(async move { ops::periodic_metadata_check(ctx, metainfo_filepath_copy).await })
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

    let content_dir = output_dir
        .as_ref()
        .join(metainfo_filepath.as_ref().file_stem().unwrap_or_default());

    let (content_storage, content_storage_server) =
        startup::create_content_storage(&metainfo, &content_dir)?;
    storage_runtime.spawn(async move {
        content_storage_server.run().await;
    });
    let (metainfo_storage, metainfo_storage_server) =
        startup::create_metainfo_storage(&metainfo_filepath)?;
    storage_runtime.spawn(async move {
        metainfo_storage_server.run().await;
    });

    let local_task = task::LocalSet::new();

    let ctx: ops::Handle<_> =
        ops::MainCtx::new(metainfo, local_peer_id, public_pwp_ip, listener_addr.port())?;

    let (peer_discovered_sink, mut peer_discovered_src) = fifo::channel::<SocketAddr>();
    let (mut outgoing_ctrl, mut incoming_ctrl) = ops::connection_control(
        MAX_PEER_CONNECTIONS,
        ops::MainConnectionData {
            content_storage,
            metainfo_storage,
            ctx_handle: ctx.clone(),
            pwp_worker_handle: pwp_runtime,
            peer_discovered_channel: peer_discovered_sink.clone(),
            piece_downloaded_channel: Rc::new(broadcast::Sender::new(2048)),
        },
    );
    local_task.spawn_local(async move {
        while let Some(peer_addr) = peer_discovered_src.next().await {
            if let Some(permit) = outgoing_ctrl.issue_permit(peer_addr).await {
                task::spawn_local(async move {
                    ops::outgoing_pwp_connection(peer_addr, permit).await.unwrap_or_else(|e| {
                        log!(e, "Outgoing peer connection to {peer_addr} failed: {e}")
                    });
                });
            } else {
                log::debug!("Outgoing connection to {peer_addr} denied");
            }
        }
    });

    let on_incoming_connection = move |stream: TcpStream, peer_ip: SocketAddr| {
        if let Some(permit) = incoming_ctrl.issue_permit() {
            task::spawn_local(async move {
                ops::incoming_pwp_connection(stream, peer_ip, permit).await.unwrap_or_else(|e| {
                    log!(e, "Incoming peer connection from {peer_ip} failed: {e}")
                });
            });
        } else {
            log::info!("Incoming connection from {peer_ip} rejected");
        }
    };
    local_task.spawn_local(async move {
        match ops::run_pwp_listener(listener_addr, on_incoming_connection).await {
            Ok(_) => (),
            Err(e) => log::error!("TCP listener exited: {e}"),
        }
    });

    for peer_ip in extra_peers {
        peer_discovered_sink.send(peer_ip);
    }

    let tracker_ctx = ctx.clone();
    let config_dir = output_dir.as_ref().to_owned();
    local_task.spawn_local(async move {
        ops::make_periodic_announces(tracker_ctx, config_dir, peer_discovered_sink).await;
    });

    local_task
        .run_until(async move {
            ops::periodic_state_dump(ctx, content_dir).await;
        })
        .await;
    Ok(())
}
