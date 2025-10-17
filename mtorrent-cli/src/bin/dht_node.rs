use clap::Parser;
use mtorrent::app;
use mtorrent_core::input::MagnetLink;
use mtorrent_dht as dht;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::time::Duration;
use std::{env, fs, io, iter};
use tokio::sync::mpsc;

#[derive(Parser)]
#[command(version, about = "Standalone DHT node")]
struct Args {
    /// Addresses of extra nodes used for bootstrapping
    #[arg(short, long)]
    nodes: Option<Vec<String>>,

    /// Shut down after the specified period of time
    #[arg(short, long, value_name = "SECONDS")]
    duration: Option<u64>,

    /// Max simultaneous outstanding queries
    #[arg(short, long)]
    parallel_queries: Option<usize>,

    /// Magnet link to the torrent to search for
    #[arg(short, long)]
    target_magnet: Option<MagnetLink>,

    /// Output file with discovered peers
    #[arg(short, long)]
    output_file: Option<PathBuf>,

    /// Local UDP port to bind to
    #[arg(long)]
    port: Option<u16>,

    /// Disable UPnP
    #[arg(long)]
    no_upnp: bool,
}

/// Example usage:
/// ```bash
/// ./target/release/dht_node --duration=30
/// ./target/release/dht_node --nodes '"router.bittorrent.com:6881" "dht.transmissionbt.com:6881"' --duration=72
/// ./target/release/dht_node --duration=10 -t "magnet:?xt=urn:btih:1EBD3DBFBB25C1333F51C99C7EE670FC2A1727C9" -o peers.txt
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
        .with_level(log::LevelFilter::Info)
        .with_module_level("mtorrent_dht", log::LevelFilter::Debug)
        .with_module_level("mtorrent::app::dht", log::LevelFilter::Debug)
        .with_module_level("dht_node", log::LevelFilter::Debug)
        .init()
        .map_err(io::Error::other)?;

    let args = Args::parse();

    let extra_nodes: Vec<SocketAddr> = if let Some(nodes) = args.nodes {
        nodes
            .into_iter()
            .filter_map(|arg| arg.to_socket_addrs().ok())
            .flatten()
            .filter(SocketAddr::is_ipv4)
            .collect()
    } else {
        vec![]
    };

    let config_dir = env::current_dir()?;
    let (_worker, cmds) = app::dht::launch_dht_node_runtime(app::dht::Config {
        local_port: args.port.unwrap_or(6881),
        max_concurrent_queries: args.parallel_queries,
        config_dir,
        use_upnp: !args.no_upnp,
        bootstrap_nodes_override: None,
    })?;

    for node in extra_nodes {
        cmds.blocking_send(dht::Command::AddNode { addr: node }).unwrap();
    }

    let search_results_channel = if let Some(magnet_link) = args.target_magnet {
        let (sender, receiver) = mpsc::channel(512);
        cmds.blocking_send(dht::Command::FindPeers {
            info_hash: *magnet_link.info_hash(),
            callback: sender,
            local_peer_port: args.port.unwrap_or(6881),
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
        let discovered_peers: Vec<_> = iter::from_fn(move || channel.try_recv().ok()).collect();
        log::info!("Discovered peers ({}): {discovered_peers:?}", discovered_peers.len());
        if let Some(output_file) = args.output_file {
            fs::write(
                &output_file,
                discovered_peers
                    .into_iter()
                    .map(|addr| addr.to_string())
                    .collect::<Vec<_>>()
                    .join("\n"),
            )?;
        }
    }

    Ok(())
}
