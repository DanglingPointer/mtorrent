use crate::utils::{ip, upnp, worker};
use crate::{dht, info_stopwatch};
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::{join, runtime, task};

pub fn launch_node_runtime(
    local_port: u16,
    max_concurrent_queries: Option<usize>,
    config_dir: PathBuf,
    use_upnp: bool,
) -> (worker::simple::Handle, dht::CmdSender) {
    let (cmd_sender, cmd_server) = dht::setup_cmds();

    let worker_handle = worker::without_runtime(
        worker::simple::Config {
            name: "dht".to_owned(),
            ..Default::default()
        },
        move || {
            runtime::Builder::new_current_thread()
                .max_blocking_threads(1) // should not be using these
                .enable_all()
                .build_local(&Default::default())
                .expect("Failed to build DHT runtime")
                .block_on(dht_main(
                    cmd_server,
                    local_port,
                    config_dir,
                    max_concurrent_queries,
                    use_upnp,
                ));
        },
    );

    (worker_handle, cmd_sender)
}

async fn start_upnp(local_port: u16) -> io::Result<()> {
    let local_addr = ip::get_local_addr()?;

    // try create a port mapping with the same port number
    let port_opener = upnp::PortOpener::new(
        SocketAddr::new(local_addr.into(), local_port),
        igd_next::PortMappingProtocol::UDP,
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
    cmd_server: dht::CmdServer,
    local_port: u16,
    config_dir: PathBuf,
    max_concurrent_queries: Option<usize>,
    use_upnp: bool,
) {
    let _sw = info_stopwatch!("DHT");

    let socket = match dht::create_ipv4_socket(local_port).await {
        Err(e) => return log::error!("Failed to create a UDP socket for DHT: {e}"),
        Ok(socket) => socket,
    };

    if use_upnp {
        if let Err(e) = start_upnp(local_port).await {
            log::error!("UPnP for DHT failed: {e}");
        }
    }

    let (outgoing_msgs_sink, incoming_msgs_source, udp_runner) = dht::setup_udp(socket);
    let (client, server, queries_runner) =
        dht::setup_routing(outgoing_msgs_sink, incoming_msgs_source, max_concurrent_queries);

    let processor = dht::Processor::new(config_dir, client);

    let (udp_result, queries_result, _) =
        join!(udp_runner.run(), queries_runner.run(), processor.run(server, cmd_server));

    if let Err(e) = udp_result {
        log::warn!("UDP exited with error: {e}");
    }
    if let Err(e) = queries_result {
        log::warn!("Queries exited with error: {e}");
    }
}
