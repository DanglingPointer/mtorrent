use crate::{dht, info_stopwatch, utils::worker};
use std::path::Path;
use tokio::{join, runtime, sync::mpsc};

pub fn launch_node_runtime(
    local_port: u16,
    max_concurrent_queries: Option<usize>,
    config_dir: impl AsRef<Path> + Send + 'static,
) -> (worker::simple::Handle, mpsc::Sender<dht::Command>) {
    let (cmd_sender, cmd_server) = dht::setup_cmds();

    let worker_handle = worker::without_runtime(
        worker::simple::Config {
            name: "dht".to_owned(),
            ..Default::default()
        },
        move || {
            runtime::Builder::new_current_thread()
                .enable_all()
                .build_local(&Default::default())
                .expect("Failed to build DHT runtime")
                .block_on(dht_main(cmd_server, local_port, config_dir, max_concurrent_queries));
        },
    );

    (worker_handle, cmd_sender)
}

async fn dht_main(
    cmd_server: dht::CmdServer,
    local_port: u16,
    config_dir: impl AsRef<Path>,
    max_concurrent_queries: Option<usize>,
) {
    let _sw = info_stopwatch!("DHT");

    let socket = match dht::create_ipv4_socket(local_port).await {
        Err(e) => return log::error!("Failed to create a UDP socket for DHT: {e}"),
        Ok(socket) => socket,
    };

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
