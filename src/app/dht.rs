use crate::{dht, info_stopwatch, utils::worker};
use std::io;
use tokio::{join, runtime, sync::mpsc};

pub fn launch_node_runtime(
    local_port: u16,
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
                .block_on(async move {
                    let _sw = info_stopwatch!("DHT");

                    let socket = dht::create_ipv4_socket(local_port).await.inspect_err(|e| {
                        log::error!("Failed to create a UDP socket for DHT: {e}")
                    })?;

                    let local_id = dht::U160::from(rand::random::<[u8; 20]>()); // TODO: read from config file

                    let (outgoing_msgs_sink, incoming_msgs_source, udp_runner) =
                        dht::setup_udp(socket);
                    let (client, server, queries_runner) =
                        dht::setup_routing(outgoing_msgs_sink, incoming_msgs_source);

                    let processor = dht::Processor::new(local_id, client);

                    let (udp_result, queries_result, _) = join!(
                        udp_runner.run(),
                        queries_runner.run(),
                        processor.run(server, cmd_server)
                    );

                    if let Err(e) = udp_result {
                        log::warn!("UDP exited with error: {e}");
                    }
                    if let Err(e) = queries_result {
                        log::warn!("Queries exited with error: {e:?}");
                    }
                    io::Result::Ok(())
                })
                .expect("Failed to start DHT runtime");
        },
    );

    (worker_handle, cmd_sender)
}
