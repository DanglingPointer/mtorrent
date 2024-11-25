use mtorrent::{app, dht};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};

/// Example usage:
/// ```
/// ./target/release/dht_node "router.bittorrent.com:6881" "dht.transmissionbt.com:6881"
/// ```
fn main() -> io::Result<()> {
    simple_logger::SimpleLogger::new()
        .with_threads(false)
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent::dht", log::LevelFilter::Trace)
        .with_module_level("dht_node", log::LevelFilter::Trace)
        .init()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, Box::new(e)))?;

    let nodes: Vec<SocketAddr> = std::env::args()
        .filter_map(|arg| arg.to_socket_addrs().ok())
        .flatten()
        .filter(SocketAddr::is_ipv4)
        .collect();

    let (_worker, cmds) = app::dht::launch_node_runtime(6881);

    for node in nodes {
        cmds.try_send(dht::Command::AddNode { addr: node }).unwrap();
    }

    Ok(())
}
