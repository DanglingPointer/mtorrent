use crate::{dht, info_stopwatch, utils::worker};
use tokio::{join, runtime, sync::mpsc};

pub fn launch_node_runtime(
    local_port: u16,
    max_concurrent_queries: Option<usize>,
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
                .block_on(dht_main(cmd_server, local_port, max_concurrent_queries));
        },
    );

    (worker_handle, cmd_sender)
}

async fn dht_main(
    cmd_server: dht::CmdServer,
    local_port: u16,
    max_concurrent_queries: Option<usize>,
) {
    let _sw = info_stopwatch!("DHT");

    let socket = match dht::create_ipv4_socket(local_port).await {
        Err(e) => return log::error!("Failed to create a UDP socket for DHT: {e}"),
        Ok(socket) => socket,
    };

    let local_id = dht::U160::from(rand::random::<[u8; 20]>()); // TODO: read from config file

    let (outgoing_msgs_sink, incoming_msgs_source, udp_runner) = dht::setup_udp(socket);
    let (client, server, queries_runner) =
        dht::setup_routing(outgoing_msgs_sink, incoming_msgs_source, max_concurrent_queries);

    let processor = dht::Processor::new(
        local_id,
        client,
        vec!["router.bittorrent.com:6881", "dht.transmissionbt.com:6881"], // TODO: read from config file
    );

    let (udp_result, queries_result, _) =
        join!(udp_runner.run(), queries_runner.run(), processor.run(server, cmd_server));

    if let Err(e) = udp_result {
        log::warn!("UDP exited with error: {e}");
    }
    if let Err(e) = queries_result {
        log::warn!("Queries exited with error: {e}");
    }
}
