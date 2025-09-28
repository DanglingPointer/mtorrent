use mtorrent_dht as dht;
use mtorrent_utils::info_stopwatch;
use mtorrent_utils::{ip, upnp, worker};
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use tokio::net::UdpSocket;
use tokio::{join, task};

pub fn launch_node_runtime(
    local_port: u16,
    max_concurrent_queries: Option<usize>,
    config_dir: PathBuf,
    use_upnp: bool,
) -> io::Result<(worker::rt::Handle, dht::CommandSink)> {
    let (cmd_sender, cmd_server) = dht::setup_commands();

    let worker_handle = worker::with_local_runtime(worker::rt::Config {
        name: "dht".to_owned(),
        io_enabled: true,
        time_enabled: true,
        ..Default::default()
    })?;
    worker_handle.runtime_handle().spawn(async move {
        // spawn_local() must be called from the dht thread
        task::spawn_local(dht_main(
            cmd_server,
            local_port,
            config_dir,
            max_concurrent_queries,
            use_upnp,
        ));
    });

    Ok((worker_handle, cmd_sender))
}

async fn start_upnp(local_port: u16) -> io::Result<()> {
    let local_addr = ip::get_local_addr()?;

    // try create a port mapping with the same port number
    let port_opener = upnp::PortOpener::new(
        SocketAddr::new(local_addr.into(), local_port),
        upnp::PortMappingProtocol::UDP,
        Some(local_port),
    )
    .await
    .map_err(io::Error::other)?;

    log::info!("UPnP for DHT succeeded, public ip: {}", port_opener.external_ip());

    // start periodic renewal of port mapping. It will stop and remove the mapping
    // automatically once the DHT runtime shuts down
    task::spawn_local(async move {
        if let Err(e) = port_opener.run_continuous_renewal().await {
            log::error!("UPnP port renewal for DHT failed: {e}");
        }
    });
    Ok(())
}

async fn dht_main(
    cmd_server: dht::CommandSource,
    local_port: u16,
    config_dir: PathBuf,
    max_concurrent_queries: Option<usize>,
    use_upnp: bool,
) {
    let _sw = info_stopwatch!("DHT");

    let socket = match UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, local_port)).await {
        Err(e) => return log::error!("Failed to create a UDP socket for DHT: {e}"),
        Ok(socket) => socket,
    };

    if use_upnp && let Err(e) = start_upnp(local_port).await {
        log::error!("UPnP for DHT failed: {e}");
    }

    let (outgoing_msgs_sink, incoming_msgs_source, udp_runner) = dht::setup_udp(socket);

    let (client, server, queries_runner) =
        dht::setup_queries(outgoing_msgs_sink, incoming_msgs_source, max_concurrent_queries);

    let processor = dht::Processor::new(config_dir, client);

    join!(udp_runner.run(), queries_runner.run(), processor.run(server, cmd_server));
}
