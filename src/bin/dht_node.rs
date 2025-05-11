use clap::Parser;
use mtorrent::{app, dht};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

#[derive(Parser)]
#[command(version, about = "Standalone DHT node")]
struct Args {
    /// Addresses of initial nodes used for bootstrapping
    nodes: Vec<String>,

    /// Shut down after the specified period of time
    #[arg(short, long, value_name = "SECONDS")]
    duration: Option<u64>,

    /// Max simultaneous outstanding queries
    #[arg(short, long)]
    parallel_queries: Option<usize>,
}

/// Example usage:
/// ```
/// ./target/release/dht_node "router.bittorrent.com:6881" "dht.transmissionbt.com:6881" --duration=72
/// ```
fn main() -> io::Result<()> {
    simple_logger::SimpleLogger::new()
        .with_threads(false)
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent::dht", log::LevelFilter::Trace)
        .with_module_level("dht_node", log::LevelFilter::Trace)
        .init()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, Box::new(e)))?;

    let args = Args::parse();

    let nodes: Vec<SocketAddr> = args
        .nodes
        .into_iter()
        .filter_map(|arg| arg.to_socket_addrs().ok())
        .flatten()
        .filter(SocketAddr::is_ipv4)
        .collect();

    let (_worker, cmds) = app::dht::launch_node_runtime(6881, args.parallel_queries);

    for node in nodes {
        cmds.try_send(dht::Command::AddNode { addr: node }).unwrap();
    }

    if let Some(timeout) = args.duration {
        std::thread::sleep(Duration::from_secs(timeout));
        drop(cmds);
    }

    Ok(())
}
