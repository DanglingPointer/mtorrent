use futures_util::FutureExt;
use mtorrent_dht as dht;
use mtorrent_utils::{info_stopwatch, net, upnp, worker};
use std::io;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::{runtime, select, task};

/// Startup configuration for the DHT system.
#[derive(Debug, Clone)]
pub struct Config {
    /// Local UDP port to bind to.
    pub local_port: u16,
    /// Optional name of the network interface to use (e.g. "eth0" or "lo").
    pub bind_interface: Option<String>,
    /// Maximum number of concurrent outbound queries in flight. Unlimited if `None`.
    pub max_concurrent_queries: Option<usize>,
    /// Directory for storing data persistent across boots.
    pub config_dir: PathBuf,
    /// Create and maintain port mappings via UPnP.
    pub use_upnp: bool,
    /// Override default bootstrap nodes.
    pub bootstrap_nodes_override: Option<Vec<String>>,
    /// Timeout for outbound queries. Uses an internal default if `None`.
    pub query_timeout: Option<Duration>,
}

/// Handle to the DHT runtime. Dropping it will shut down DHT and block until it exits.
pub struct Handle {
    // stop signal must be declared _before_ join handle so that it's dropped first
    _stop_guard: oneshot::Sender<()>,
    _join_guard: worker::simple::Handle,
}

/// Spawn a thread with a Tokio runtime running the DHT system, and return its handle and command
/// sender. Dropping either of the two will stop DHT.
pub fn launch_dht_node_runtime(cfg: Config) -> io::Result<(Handle, dht::CommandSink)> {
    let (cmd_sender, cmd_server) = dht::setup_commands();
    let (stop_sender, stop_receiver) = oneshot::channel();

    // We build the runtime by hand instead of using `worker::with_local_runtime` because DHT needs
    // to drive its own shutdown: `stop_receiver` is passed into `dht_main` so it can cancel the
    // running tasks and remove the UPnP port mapping before the runtime is torn down, rather than
    // relying on the worker's generic stop signal.
    let worker_handle = worker::without_runtime(
        worker::simple::Config {
            name: "dht".to_owned(),
            ..Default::default()
        },
        move || {
            let rt = runtime::Builder::new_current_thread()
                .max_blocking_threads(1)
                .enable_all()
                .build_local(Default::default())
                .expect("Failed to build DHT runtime");
            rt.block_on(dht_main(cmd_server, stop_receiver, cfg));
            rt.shutdown_timeout(Duration::ZERO);
        },
    )?;

    Ok((
        Handle {
            _join_guard: worker_handle,
            _stop_guard: stop_sender,
        },
        cmd_sender,
    ))
}

async fn start_upnp(
    local_port: u16,
    interface: Option<String>,
) -> io::Result<(upnp::PortMapperHandle, task::JoinHandle<()>)> {
    // try create a port mapping with the same port number
    let (mut upnp_handle, port_mapper) =
        upnp::init(upnp::PortMappingProtocol::UDP, local_port, Some(local_port), interface);

    // start periodic renewal of the port mapping. It will stop automatically when the handle is
    // dropped
    let join_handle = task::spawn(port_mapper.run().map(|_| ()));

    match upnp_handle.get_external_addr().await {
        Ok(external_addr) => {
            log::info!("UPnP for DHT succeeded, public ip: {}", external_addr);
        }
        Err(e) => {
            log::error!("UPnP for DHT failed: {e}");
        }
    }

    Ok((upnp_handle, join_handle))
}

async fn dht_main(
    cmd_server: dht::CommandSource,
    canceller: oneshot::Receiver<()>,
    Config {
        local_port,
        bind_interface,
        max_concurrent_queries,
        config_dir,
        use_upnp,
        bootstrap_nodes_override,
        query_timeout,
    }: Config,
) {
    let _sw = info_stopwatch!("DHT");

    let local_ipv4 = net::get_bind_addr_v4(bind_interface.as_deref());
    let socket =
        match net::bound_udp_socket((local_ipv4, local_port).into(), bind_interface.as_deref()) {
            Err(e) => {
                log::error!("Failed to create a UDP socket for DHT: {e}");
                return;
            }
            Ok(socket) => socket,
        };

    // launch UPnP and keep its handle alive until DHT exits
    let upnp = if use_upnp {
        start_upnp(local_port, bind_interface).await.ok()
    } else {
        None
    };

    let mut tasks = task::JoinSet::new();

    let (outgoing_msgs_sink, incoming_msgs_source, udp_runner) = dht::setup_udp(socket);
    tasks.spawn_local(udp_runner.run());

    let (client, server, queries_runner) = dht::setup_queries(
        outgoing_msgs_sink,
        incoming_msgs_source,
        max_concurrent_queries,
        query_timeout,
    );
    tasks.spawn_local(queries_runner.run());

    let mut processor = dht::Processor::new(config_dir, client);
    if let Some(nodes) = bootstrap_nodes_override {
        processor.set_bootstrap_nodes(nodes);
    }
    tasks.spawn_local(processor.run(server, cmd_server));

    let join_all = async move { while tasks.join_next().await.is_some() {} };

    select! {
        biased;
        _ = join_all => (),
        _ = canceller => (),
    }

    // remove port mapping
    if let Some((port_mapper_handle, upnp_join_handle)) = upnp {
        drop(port_mapper_handle);
        _ = upnp_join_handle.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mtorrent_dht::Command;
    use std::fs;

    #[test]
    fn test_aborting_dht_doesnt_deadlock() {
        let config_dir = "test_aborting_dht_doesnt_deadlock";
        fs::create_dir_all(config_dir).unwrap();

        let (handle, cmds) = launch_dht_node_runtime(Config {
            local_port: 0,
            bind_interface: None,
            max_concurrent_queries: None,
            config_dir: config_dir.into(),
            use_upnp: false,
            bootstrap_nodes_override: Some(Vec::new()),
            query_timeout: None,
        })
        .unwrap();

        drop(handle);

        let ret = cmds.blocking_send(Command::AddNode {
            addr: "127.0.0.1:2345".parse().unwrap(),
        });
        assert!(ret.is_err());

        _ = fs::remove_dir_all(config_dir);
    }

    #[test]
    fn test_stopping_dht_doesnt_deadlock() {
        let config_dir = "test_stopping_dht_doesnt_deadlock";
        fs::create_dir_all(config_dir).unwrap();

        let (mut handle, cmds) = launch_dht_node_runtime(Config {
            local_port: 0,
            bind_interface: None,
            max_concurrent_queries: None,
            config_dir: config_dir.into(),
            use_upnp: false,
            bootstrap_nodes_override: Some(Vec::new()),
            query_timeout: None,
        })
        .unwrap();

        drop(cmds);

        while handle._stop_guard.closed().now_or_never().is_none() {
            std::thread::yield_now();
        }

        _ = fs::remove_dir_all(config_dir);
    }
}
