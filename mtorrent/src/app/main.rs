use crate::ops;
use crate::utils::{listener, startup};
use futures_util::FutureExt;
use mtorrent_core::{input, pwp, trackers};
use mtorrent_dht as dht;
use mtorrent_utils::peer_id::PeerId;
use mtorrent_utils::{info_stopwatch, ip, upnp};
use std::borrow::Borrow;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use tokio::sync::broadcast;
use tokio::{runtime, task};
use tokio_util::sync::CancellationToken;

/// Configuration for a single torrent download.
#[derive(Debug, Clone)]
pub struct Config {
    pub local_peer_id: PeerId,
    /// Parent directory for the downloaded content.
    pub output_dir: PathBuf,
    /// Directory for saving logs and persistent state (e.g. known trackers).
    pub config_dir: PathBuf,
    /// Whether to use UPnP for port mapping.
    pub use_upnp: bool,
    /// Local TCP port used for peer wire protocol sockets (both inbound and outbound).
    pub pwp_port: Option<u16>,
}

/// Context for a single torrent download.
#[derive(Clone)]
pub struct Context {
    /// Handle to the DHT system if present.
    pub dht_handle: Option<dht::CommandSink>,
    /// Handle to a Tokio runtime that will be used for peer wire protocol I/O and communication with trackers.
    pub pwp_runtime: runtime::Handle,
    /// Handle to a Tokio runtime that will be used for scheduling filesystem operations on the downloaded data.
    pub storage_runtime: runtime::Handle,
}

/// Download a single torrent given a magnet link or a path to its metainfo file.
/// This function will exit once the download is complete or a fatal error has occurred.
pub async fn single_torrent(
    metainfo_uri: impl AsRef<str>,
    mut listener: impl listener::StateListener,
    cfg: Config,
    ctx: impl Borrow<Context>,
) -> io::Result<()> {
    #[cfg(debug_assertions)]
    {
        let orig_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic_info| {
            orig_hook(panic_info);
            std::process::exit(1);
        }));
    }
    let ctx: &Context = ctx.borrow();

    let listener_addr = SocketAddr::new(
        Ipv4Addr::UNSPECIFIED.into(),
        cfg.pwp_port.unwrap_or_else(|| ip::port_from_hash(&metainfo_uri.as_ref())),
    );
    // get public ip to send correct listening port to trackers and peers later
    let public_pwp_ip = if cfg.use_upnp {
        match upnp::PortOpener::new(
            SocketAddrV4::new(ip::get_local_addr()?, listener_addr.port()).into(),
            upnp::PortMappingProtocol::TCP,
            cfg.pwp_port,
        )
        .await
        {
            Ok(port_opener) => {
                let public_ip = port_opener.external_ip();
                log::info!("UPnP for PWP succeeded, public ip: {public_ip}");
                ctx.pwp_runtime.spawn(async move {
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

    if Path::new(metainfo_uri.as_ref()).is_file() {
        main_stage(
            cfg.local_peer_id,
            listener_addr,
            public_pwp_ip,
            metainfo_uri.as_ref(),
            cfg.output_dir,
            cfg.config_dir,
            &mut listener,
            ctx,
            std::iter::empty(),
        )
        .await?;
    } else {
        let (metainfo_filepath, peers) = preliminary_stage(
            cfg.local_peer_id,
            listener_addr,
            public_pwp_ip,
            metainfo_uri,
            &cfg.output_dir,
            cfg.config_dir.to_owned(),
            &mut listener,
            ctx.dht_handle.as_ref(),
            &ctx.pwp_runtime,
        )
        .await?;
        log::info!("Metadata downloaded successfully, starting content download");
        main_stage(
            cfg.local_peer_id,
            listener_addr,
            public_pwp_ip,
            metainfo_filepath,
            &cfg.output_dir,
            cfg.config_dir.to_owned(),
            &mut listener,
            ctx,
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
    metainfo_dir: impl AsRef<Path>,
    config_dir: impl AsRef<Path> + 'static,
    listener: &mut impl listener::StateListener,
    dht_handle: Option<&dht::CommandSink>,
    pwp_runtime: &runtime::Handle,
) -> io::Result<(PathBuf, impl IntoIterator<Item = SocketAddr> + 'static)> {
    let magnet_link: input::MagnetLink = magnet_link
        .as_ref()
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, Box::new(e)))
        .inspect_err(|e| log::error!("Invalid magnet link: {e}"))?;
    let _sw =
        info_stopwatch!("Preliminary stage for torrent '{}'", magnet_link.name().unwrap_or("n/a"));

    let (tracker_client, trackers_mgr) = trackers::init();
    pwp_runtime.spawn(trackers_mgr.run());

    let mut tasks = task::JoinSet::new();

    let extra_peers: Vec<SocketAddr> = magnet_link.peers().cloned().collect();

    let metainfo_filepath = metainfo_dir
        .as_ref()
        .join(format!("{}.torrent", magnet_link.name().unwrap_or("unnamed")));

    let info_hash: [u8; 20] = *magnet_link.info_hash();

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
            dht_cmds.clone(),
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
        peer_reporter.clone(),
        config_dir,
    ));

    tasks.spawn_local(async move {
        for peer_addr in extra_peers {
            peer_reporter.report_discovered_new(peer_addr, pwp::PeerOrigin::Other).await;
        }
    });

    let peers = ops::periodic_metadata_check(
        ctx,
        metainfo_filepath.clone(),
        listener,
        canceller.drop_guard(),
    )
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
    config_dir: impl AsRef<Path> + 'static,
    listener: &mut impl listener::StateListener,
    handles: impl Borrow<Context>,
    extra_peers: impl IntoIterator<Item = SocketAddr>,
) -> io::Result<()> {
    let handles: &Context = handles.borrow();

    let metainfo = startup::read_metainfo(&metainfo_filepath)
        .inspect_err(|e| log::error!("Invalid metainfo file: {e}"))?;
    let _sw = info_stopwatch!("Main stage for torrent '{}'", metainfo.name().unwrap_or("n/a"));

    let content_dir = output_dir
        .as_ref()
        .join(metainfo_filepath.as_ref().file_stem().unwrap_or_default());

    let (content_storage, content_storage_server) =
        startup::create_content_storage(&metainfo, &content_dir)?;
    handles.storage_runtime.spawn(content_storage_server.run());

    let (metainfo_storage, metainfo_storage_server) =
        startup::create_metainfo_storage(&metainfo_filepath)?;
    handles.storage_runtime.spawn(metainfo_storage_server.run());

    let (tracker_client, trackers_mgr) = trackers::init();
    handles.pwp_runtime.spawn(trackers_mgr.run());

    let mut tasks = task::JoinSet::new();

    let info_hash: [u8; 20] = *metainfo.info_hash();

    let ctx: ops::Handle<_> =
        ops::MainCtx::new(metainfo, local_peer_id, public_pwp_ip, listener_addr.port())?;

    let canceller = CancellationToken::new();

    let (peer_reporter, connect_throttle) =
        ops::connect_control(|peer_reporter| ops::CancellingConnectHandler {
            connector: Rc::new(ops::MainConnectionData {
                content_storage,
                metainfo_storage,
                ctx_handle: ctx.clone(),
                pwp_worker_handle: handles.pwp_runtime.clone(),
                peer_reporter: peer_reporter.clone(),
                piece_downloaded_channel: Rc::new(broadcast::Sender::new(2048)),
            }),
            max_connections: MAX_PEER_CONNECTIONS,
            canceller: canceller.clone(),
        });
    tasks.spawn_local(connect_throttle.run());

    handles.dht_handle.as_ref().map(|dht_cmds| {
        tasks.spawn_local(ops::run_dht_search(
            info_hash,
            dht_cmds.clone(),
            peer_reporter.clone(),
            public_pwp_ip.port(),
        ))
    });

    handles.pwp_runtime.spawn(
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
        peer_reporter.clone(),
        config_dir,
    ));

    let extra_peers: Vec<_> = extra_peers.into_iter().collect();
    tasks.spawn_local(async move {
        for peer_addr in extra_peers {
            peer_reporter.report_discovered_new(peer_addr, pwp::PeerOrigin::Other).await;
        }
    });

    ops::periodic_state_dump(ctx, content_dir, listener, canceller.drop_guard()).await;
    tasks.shutdown().await;
    Ok(())
}
