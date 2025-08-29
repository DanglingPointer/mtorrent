use crate::ops;
use crate::utils::startup;
use futures_util::FutureExt;
use mtorrent_core::{pwp, trackers};
use mtorrent_dht as dht;
use mtorrent_utils::peer_id::PeerId;
use mtorrent_utils::{ip, magnet, upnp};
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use tokio::sync::broadcast;
use tokio::{runtime, task};
use tokio_util::sync::CancellationToken;

pub async fn single_torrent(
    local_peer_id: PeerId,
    metainfo_uri: &str,
    output_dir: impl AsRef<Path>,
    dht_handle: Option<dht::CmdSender>,
    pwp_runtime: runtime::Handle,
    storage_runtime: runtime::Handle,
    use_upnp: bool,
) -> io::Result<()> {
    #[cfg(debug_assertions)]
    {
        let orig_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic_info| {
            orig_hook(panic_info);
            std::process::exit(1);
        }));
    }
    let listener_addr =
        SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), ip::port_from_hash(&metainfo_uri));
    // get public ip to send correct listening port to trackers and peers later
    let public_pwp_ip = if use_upnp {
        match upnp::PortOpener::new(
            SocketAddrV4::new(ip::get_local_addr()?, listener_addr.port()).into(),
            upnp::PortMappingProtocol::TCP,
            None,
        )
        .await
        {
            Ok(port_opener) => {
                let public_ip = port_opener.external_ip();
                log::info!("UPnP for PWP succeeded, public ip: {public_ip}");
                pwp_runtime.spawn(async move {
                    if let Err(e) = port_opener.run_continuous_renewal().await {
                        log::error!("UPnP port renewal for PWP failed: {e}");
                    }
                });
                public_ip
            }
            Err(e) => {
                log::error!("UPnP for PWP failed: {e}");
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
            dht_handle,
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
            dht_handle.clone(),
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
            dht_handle,
            pwp_runtime,
            storage_runtime,
            peers,
        )
        .await?;
    }
    Ok(())
}

const MAX_PRELIMINARY_CONNECTIONS: usize = 50;
const MAX_PEER_CONNECTIONS: usize = 200;

#[expect(clippy::too_many_arguments)]
async fn preliminary_stage(
    local_peer_id: PeerId,
    listener_addr: SocketAddr,
    public_pwp_ip: SocketAddr,
    magnet_link: impl AsRef<str>,
    config_dir: impl AsRef<Path>,
    metainfo_dir: impl AsRef<Path>,
    dht_handle: Option<dht::CmdSender>,
    pwp_runtime: runtime::Handle,
) -> io::Result<(impl AsRef<Path>, impl IntoIterator<Item = SocketAddr>)> {
    let magnet_link: magnet::MagnetLink = magnet_link
        .as_ref()
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, Box::new(e)))
        .inspect_err(|e| log::error!("Invalid magnet link: {e}"))?;
    let info_hash: [u8; 20] = *magnet_link.info_hash();

    let (tracker_client, trackers_mgr) = trackers::init();
    pwp_runtime.spawn(trackers_mgr.run());

    let mut tasks = task::JoinSet::new();

    let extra_peers: Vec<SocketAddr> = magnet_link.peers().cloned().collect();

    let metainfo_filepath = metainfo_dir
        .as_ref()
        .join(format!("{}.torrent", magnet_link.name().unwrap_or("unnamed")));

    let ctx =
        ops::PreliminaryCtx::new(magnet_link, local_peer_id, public_pwp_ip, listener_addr.port());

    let canceller = CancellationToken::new();

    let (peer_reporter, connect_throttle) =
        ops::connect_control(|peer_reporter| ops::CancellingConnectHandler {
            connector: Rc::new(ops::PreliminaryConnectionData {
                ctx_handle: ctx.clone(),
                pwp_worker_handle: pwp_runtime.clone(),
                peer_reporter: peer_reporter.clone(),
            }),
            max_connections: MAX_PRELIMINARY_CONNECTIONS,
            canceller: canceller.clone(),
        });
    tasks.spawn_local(connect_throttle.run());

    dht_handle.map(|dht_cmds| {
        tasks.spawn_local(ops::run_dht_search(
            info_hash,
            dht_cmds,
            peer_reporter.clone(),
            public_pwp_ip.port(),
        ))
    });

    pwp_runtime.spawn(
        ops::run_pwp_listener(listener_addr, peer_reporter.clone(), canceller.clone()).map(
            |result| match result {
                Ok(_) => (),
                Err(e) => log::error!("TCP listener exited: {e}"),
            },
        ),
    );

    tasks.spawn_local(ops::make_preliminary_announces(
        ctx.clone(),
        tracker_client,
        PathBuf::from(config_dir.as_ref()),
        peer_reporter.clone(),
    ));

    tasks.spawn_local(async move {
        for peer_addr in extra_peers {
            peer_reporter.report_discovered_new(peer_addr, pwp::PeerOrigin::Other).await;
        }
    });

    let peers =
        ops::periodic_metadata_check(ctx, metainfo_filepath.clone(), canceller.drop_guard())
            .await?;
    tasks.shutdown().await;
    Ok((metainfo_filepath, peers))
}

#[expect(clippy::too_many_arguments)]
async fn main_stage(
    local_peer_id: PeerId,
    listener_addr: SocketAddr,
    public_pwp_ip: SocketAddr,
    metainfo_filepath: impl AsRef<Path>,
    output_dir: impl AsRef<Path>,
    dht_handle: Option<dht::CmdSender>,
    pwp_runtime: runtime::Handle,
    storage_runtime: runtime::Handle,
    extra_peers: impl IntoIterator<Item = SocketAddr>,
) -> io::Result<()> {
    let metainfo = startup::read_metainfo(&metainfo_filepath)
        .inspect_err(|e| log::error!("Invalid metainfo file: {e}"))?;
    let info_hash: [u8; 20] = *metainfo.info_hash();

    let content_dir = output_dir
        .as_ref()
        .join(metainfo_filepath.as_ref().file_stem().unwrap_or_default());

    let (content_storage, content_storage_server) =
        startup::create_content_storage(&metainfo, &content_dir)?;
    storage_runtime.spawn(content_storage_server.run());

    let (metainfo_storage, metainfo_storage_server) =
        startup::create_metainfo_storage(&metainfo_filepath)?;
    storage_runtime.spawn(metainfo_storage_server.run());

    let (tracker_client, trackers_mgr) = trackers::init();
    pwp_runtime.spawn(trackers_mgr.run());

    let mut tasks = task::JoinSet::new();

    let ctx: ops::Handle<_> =
        ops::MainCtx::new(metainfo, local_peer_id, public_pwp_ip, listener_addr.port())?;

    let canceller = CancellationToken::new();

    let (peer_reporter, connect_throttle) =
        ops::connect_control(|peer_reporter| ops::CancellingConnectHandler {
            connector: Rc::new(ops::MainConnectionData {
                content_storage,
                metainfo_storage,
                ctx_handle: ctx.clone(),
                pwp_worker_handle: pwp_runtime.clone(),
                peer_reporter: peer_reporter.clone(),
                piece_downloaded_channel: Rc::new(broadcast::Sender::new(2048)),
            }),
            max_connections: MAX_PEER_CONNECTIONS,
            canceller: canceller.clone(),
        });
    tasks.spawn_local(connect_throttle.run());

    dht_handle.map(|dht_cmds| {
        tasks.spawn_local(ops::run_dht_search(
            info_hash,
            dht_cmds,
            peer_reporter.clone(),
            public_pwp_ip.port(),
        ))
    });

    pwp_runtime.spawn(
        ops::run_pwp_listener(listener_addr, peer_reporter.clone(), canceller.clone()).map(
            |result| match result {
                Ok(_) => (),
                Err(e) => log::error!("TCP listener exited: {e}"),
            },
        ),
    );

    tasks.spawn_local(ops::make_periodic_announces(
        ctx.clone(),
        tracker_client,
        PathBuf::from(output_dir.as_ref()),
        peer_reporter.clone(),
    ));

    let extra_peers: Vec<_> = extra_peers.into_iter().collect();
    tasks.spawn_local(async move {
        for peer_addr in extra_peers {
            peer_reporter.report_discovered_new(peer_addr, pwp::PeerOrigin::Other).await;
        }
    });

    ops::periodic_state_dump(ctx, content_dir, canceller.drop_guard()).await;
    tasks.shutdown().await;
    Ok(())
}
