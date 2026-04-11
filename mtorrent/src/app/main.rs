use crate::ops;
use crate::utils::{listener, startup};
use mtorrent_core::{input, pwp, trackers};
use mtorrent_dht as dht;
use mtorrent_utils::peer_id::PeerId;
use mtorrent_utils::{info_stopwatch, net, upnp};
use std::borrow::Borrow;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use tokio::sync::broadcast;
use tokio::{join, runtime, task};

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
    /// Network interface to bind all sockets to.
    pub bind_interface: Option<String>,
}

/// Context for a single torrent download.
#[derive(Clone)]
pub struct Context {
    /// Handle to the DHT system if present.
    pub dht_handle: Option<dht::CommandSink>,
    /// Handle to a Tokio runtime that will be used for peer wire protocol I/O and communication
    /// with trackers.
    pub pwp_runtime: runtime::Handle,
    /// Handle to a Tokio runtime that will be used for scheduling filesystem operations on the
    /// downloaded data.
    pub storage_runtime: runtime::Handle,
}

#[derive(Clone)]
struct Handles<'h> {
    dht: Option<&'h dht::CommandSink>,
    pwp_runtime: &'h runtime::Handle,
    storage_runtime: &'h runtime::Handle,
    utp: ops::UtpHandle,
    trackers: trackers::Client,
}

#[derive(Clone)]
struct Params {
    local_peer_id: PeerId,
    internal_pwp_port: u16,
    external_pwp_port: u16,
    local_ip_v4: Ipv4Addr,
    local_ip_v6: Ipv6Addr,
    bind_interface: Option<String>,
}

async fn start_upnp(
    internal_port: u16,
    desired_external_port: Option<u16>,
    proto: upnp::PortMappingProtocol,
    interface: Option<&str>,
) -> u16 {
    let Ok(mut port_opener) =
        upnp::PortOpener::new(proto, internal_port, desired_external_port, interface)
            .await
            .inspect_err(|e| log::error!("UPnP: {proto:?} port mapping failed: {e}"))
    else {
        return internal_port;
    };

    let external_addr = port_opener.external_addr();
    log::info!("UPnP: {proto:?} port mapping succeeded, public addr: {external_addr}");

    task::spawn(async move {
        if let Err(e) = port_opener.run_continuous_renewal().await {
            log::error!("UPnP: {proto:?} port renewal for PWP failed: {e}");
        }
    });
    external_addr.port()
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

    let internal_pwp_port =
        cfg.pwp_port.unwrap_or_else(|| net::port_from_hash(&metainfo_uri.as_ref()));
    let local_addr_v4 = net::get_bind_addr_v4(cfg.bind_interface.as_deref());
    let local_addr_v6 = net::get_bind_addr_v6(cfg.bind_interface.as_deref());

    // create port mappings and get external port to send correct listening port to trackers and
    // peers later
    let external_pwp_port = if cfg.use_upnp {
        let _g = ctx.pwp_runtime.enter();
        let (_external_tcp_port, external_udp_port) = join!(
            start_upnp(
                internal_pwp_port,
                cfg.pwp_port,
                upnp::PortMappingProtocol::TCP,
                cfg.bind_interface.as_deref()
            ),
            start_upnp(
                internal_pwp_port,
                cfg.pwp_port,
                upnp::PortMappingProtocol::UDP,
                cfg.bind_interface.as_deref()
            ),
        );
        external_udp_port
    } else {
        internal_pwp_port
    };

    let utp_handle = ops::launch_utp(
        &ctx.pwp_runtime,
        SocketAddrV4::new(local_addr_v4, internal_pwp_port),
        SocketAddrV6::new(local_addr_v6, internal_pwp_port, 0, 0),
        cfg.bind_interface.clone(),
    );

    let (tracker_client, trackers_mgr) = trackers::init(trackers::Config {
        bind_interface: cfg.bind_interface.clone(),
    });
    ctx.pwp_runtime.spawn(trackers_mgr.run());

    let handles = Handles {
        dht: ctx.dht_handle.as_ref(),
        pwp_runtime: &ctx.pwp_runtime,
        storage_runtime: &ctx.storage_runtime,
        utp: utp_handle,
        trackers: tracker_client,
    };

    let params = Params {
        local_peer_id: cfg.local_peer_id,
        internal_pwp_port,
        external_pwp_port,
        local_ip_v4: local_addr_v4,
        local_ip_v6: local_addr_v6,
        bind_interface: cfg.bind_interface,
    };

    if Path::new(metainfo_uri.as_ref()).is_file() {
        main_stage(
            params,
            metainfo_uri.as_ref(),
            cfg.output_dir,
            cfg.config_dir,
            &mut listener,
            handles,
            std::iter::empty(),
        )
        .await?;
    } else {
        let (metainfo_filepath, peers) = preliminary_stage(
            params.clone(),
            metainfo_uri,
            &cfg.output_dir,
            cfg.config_dir.to_owned(),
            &mut listener,
            handles.clone(),
        )
        .await?;
        log::info!("Metadata downloaded successfully, starting content download");
        main_stage(
            params,
            metainfo_filepath,
            &cfg.output_dir,
            cfg.config_dir.to_owned(),
            &mut listener,
            handles,
            peers,
        )
        .await?;
    }
    Ok(())
}

async fn preliminary_stage(
    params: Params,
    magnet_link: impl AsRef<str>,
    metainfo_dir: impl AsRef<Path>,
    config_dir: impl AsRef<Path> + 'static,
    listener: &mut impl listener::StateListener,
    handles: Handles<'_>,
) -> io::Result<(PathBuf, impl IntoIterator<Item = SocketAddr> + 'static)> {
    let magnet_link: input::MagnetLink = magnet_link
        .as_ref()
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, Box::new(e)))
        .inspect_err(|e| log::error!("Invalid magnet link: {e}"))?;
    let _sw =
        info_stopwatch!("Preliminary stage for torrent '{}'", magnet_link.name().unwrap_or("n/a"));

    let extra_peers: Vec<SocketAddr> = magnet_link.peers().cloned().collect();
    let metainfo_filepath = metainfo_dir
        .as_ref()
        .join(format!("{}.torrent", magnet_link.name().unwrap_or("unnamed")));
    let info_hash: [u8; 20] = *magnet_link.info_hash();

    let ctx = ops::PreliminaryCtx::new(
        magnet_link,
        params.local_peer_id,
        params.external_pwp_port,
        params.internal_pwp_port,
        params.local_ip_v4,
        params.local_ip_v6,
        params.bind_interface.clone(),
    );

    let mut tasks = task::JoinSet::new();

    let (peer_reporter, connect_throttle) =
        ops::connect_control(|peer_reporter| ops::PreliminaryConnectionData {
            ctx_handle: ctx.clone(),
            pwp_worker_handle: handles.pwp_runtime.clone(),
            peer_reporter: peer_reporter.clone(),
            utp_handle: handles.utp.clone(),
        });
    tasks.spawn_local(connect_throttle.run());

    if let Err(e) = handles.utp.restart(peer_reporter.clone()).await {
        log::error!("Failed to restart uTP: {e}");
    }
    if let Err(e) = handles.trackers.abort_all().await {
        log::error!("Failed to abort tracker announces: {e}");
    }

    handles.dht.map(|dht_cmds| {
        tasks.spawn_local(ops::run_dht_search(
            info_hash,
            dht_cmds.clone(),
            peer_reporter.clone(),
            params.external_pwp_port,
        ))
    });

    tasks.spawn_on(
        ops::run_pwp_listener(
            SocketAddr::new(params.local_ip_v4.into(), params.internal_pwp_port),
            params.bind_interface.clone(),
            peer_reporter.clone(),
        ),
        handles.pwp_runtime,
    );

    tasks.spawn_on(
        ops::run_pwp_listener(
            SocketAddr::new(params.local_ip_v6.into(), params.internal_pwp_port),
            params.bind_interface,
            peer_reporter.clone(),
        ),
        handles.pwp_runtime,
    );

    tasks.spawn_local(ops::make_preliminary_announces(
        ctx.clone(),
        handles.trackers,
        peer_reporter.clone(),
        config_dir,
    ));

    tasks.spawn_local(async move {
        for peer_addr in extra_peers {
            peer_reporter.report_discovered(peer_addr, pwp::PeerOrigin::Other).await;
        }
    });

    let peers = ops::periodic_metadata_check(ctx, metainfo_filepath.clone(), listener).await?;
    tasks.shutdown().await;
    Ok((metainfo_filepath, peers))
}

async fn main_stage(
    params: Params,
    metainfo_filepath: impl AsRef<Path>,
    output_dir: impl AsRef<Path>,
    config_dir: impl AsRef<Path> + 'static,
    listener: &mut impl listener::StateListener,
    handles: Handles<'_>,
    extra_peers: impl IntoIterator<Item = SocketAddr>,
) -> io::Result<()> {
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

    let info_hash: [u8; 20] = *metainfo.info_hash();

    let ctx: ops::Handle<_> = ops::MainCtx::new(
        metainfo,
        params.local_peer_id,
        params.external_pwp_port,
        params.internal_pwp_port,
        params.local_ip_v4,
        params.local_ip_v6,
        params.bind_interface.clone(),
    )?;

    let mut tasks = task::JoinSet::new();

    let (peer_reporter, connect_throttle) =
        ops::connect_control(|peer_reporter| ops::MainConnectionData {
            content_storage,
            metainfo_storage,
            ctx_handle: ctx.clone(),
            pwp_worker_handle: handles.pwp_runtime.clone(),
            peer_reporter: peer_reporter.clone(),
            piece_downloaded_channel: Rc::new(broadcast::Sender::new(2048)),
            utp_handle: handles.utp.clone(),
        });
    tasks.spawn_local(connect_throttle.run());

    if let Err(e) = handles.utp.restart(peer_reporter.clone()).await {
        log::error!("Failed to restart uTP: {e}");
    }
    if let Err(e) = handles.trackers.abort_all().await {
        log::error!("Failed to abort tracker announces: {e}");
    }

    handles.dht.map(|dht_cmds| {
        tasks.spawn_local(ops::run_dht_search(
            info_hash,
            dht_cmds.clone(),
            peer_reporter.clone(),
            params.external_pwp_port,
        ))
    });

    tasks.spawn_on(
        ops::run_pwp_listener(
            SocketAddr::new(params.local_ip_v4.into(), params.internal_pwp_port),
            params.bind_interface.clone(),
            peer_reporter.clone(),
        ),
        handles.pwp_runtime,
    );

    tasks.spawn_on(
        ops::run_pwp_listener(
            SocketAddr::new(params.local_ip_v6.into(), params.internal_pwp_port),
            params.bind_interface,
            peer_reporter.clone(),
        ),
        handles.pwp_runtime,
    );

    tasks.spawn_local(ops::make_periodic_announces(
        ctx.clone(),
        handles.trackers,
        peer_reporter.clone(),
        config_dir,
    ));

    let extra_peers: Vec<_> = extra_peers.into_iter().collect();
    tasks.spawn_local(async move {
        for peer_addr in extra_peers {
            peer_reporter.report_discovered(peer_addr, pwp::PeerOrigin::Other).await;
        }
    });

    ops::periodic_state_dump(ctx, content_dir, listener).await;
    tasks.shutdown().await;
    Ok(())
}
