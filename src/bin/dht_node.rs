use clap::Parser;
use mtorrent::utils::magnet::MagnetLink;
use mtorrent::{app, dht};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;
use tokio::sync::mpsc;

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

    /// Magnet link to the torrent to search for
    #[arg(short, long)]
    target_magnet: Option<MagnetLink>,
}

/// Example usage:
/// ```
/// ./target/release/dht_node "router.bittorrent.com:6881" "dht.transmissionbt.com:6881" --duration=72
/// ```
fn main() -> io::Result<()> {
    #[cfg(debug_assertions)]
    {
        let orig_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic_info| {
            orig_hook(panic_info);
            std::process::exit(1);
        }));
    }
    simple_logger::SimpleLogger::new()
        .with_threads(false)
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent::dht", log::LevelFilter::Info)
        .with_module_level("mtorrent::app::dht", log::LevelFilter::Debug)
        .with_module_level("dht_node", log::LevelFilter::Debug)
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

    let search_results_channel = if let Some(magnet_link) = args.target_magnet {
        std::thread::sleep(Duration::from_secs(10));
        log::info!("Starting search for peers");

        let (sender, receiver) = mpsc::channel(512);
        cmds.try_send(dht::Command::FindPeers {
            info_hash: (*magnet_link.info_hash()).into(),
            callback: sender,
            local_peer_port: 6881,
        })
        .unwrap();
        Some(receiver)
    } else {
        None
    };

    if let Some(timeout) = args.duration {
        std::thread::sleep(Duration::from_secs(timeout));
        drop(cmds);
    }

    if let Some(mut channel) = search_results_channel {
        let discovered_peers: Vec<_> =
            std::iter::from_fn(move || channel.try_recv().ok()).collect();
        log::info!("Discovered peers: {discovered_peers:?}");
    }

    Ok(())
}
