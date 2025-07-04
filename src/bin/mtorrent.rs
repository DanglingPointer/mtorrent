use clap::Parser;
use mtorrent::app::dht;
use mtorrent::utils::peer_id::PeerId;
use mtorrent::utils::worker;
use mtorrent::{app, info_stopwatch};
use std::io;
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Magnet link or path to a .torrent file
    metainfo_uri: String,

    /// Output folder
    #[arg(short, long, value_name = "PATH")]
    output: Option<PathBuf>,

    /// Disable UPnP
    #[arg(long)]
    no_upnp: bool,

    /// Disable DHT
    #[arg(long)]
    no_dht: bool,
}

fn main() -> io::Result<()> {
    simple_logger::SimpleLogger::new()
        .with_threads(false)
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent", log::LevelFilter::Info)
        .with_module_level("mtorrent::ops::search", log::LevelFilter::Debug)
        // .with_module_level("mtorrent::ops::peer::metadata", log::LevelFilter::Debug)
        // .with_module_level("mtorrent::ops::peer::extensions", log::LevelFilter::Debug)
        // .with_module_level("mtorrent::pwp::channels", log::LevelFilter::Trace)
        .init()
        .map_err(io::Error::other)?;

    let _sw = info_stopwatch!("mtorrent");

    let cli = Cli::parse();

    let output_dir = if let Some(cli_arg) = cli.output {
        cli_arg
    } else {
        let metainfo_filepath = Path::new(&cli.metainfo_uri);
        let parent_folder = if metainfo_filepath.is_file() {
            metainfo_filepath.parent()
        } else {
            None
        };
        if let Some(parent_folder) = parent_folder {
            parent_folder.into()
        } else {
            std::env::current_dir()?
        }
    };

    let storage_worker = worker::with_runtime(worker::rt::Config {
        name: "storage".to_owned(),
        io_enabled: false,
        time_enabled: false,
        ..Default::default()
    });

    let pwp_worker = worker::with_runtime(worker::rt::Config {
        name: "pwp".to_owned(),
        io_enabled: true,
        time_enabled: true,
        ..Default::default()
    });

    let (_dht_worker, dht_cmds) = if !cli.no_dht {
        let (dht_worker, dht_cmds) =
            dht::launch_node_runtime(6881, None, output_dir.clone(), !cli.no_upnp);
        (Some(dht_worker), Some(dht_cmds))
    } else {
        (None, None)
    };

    let peer_id = PeerId::generate_new();

    tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(32) // used for DNS resolution
        .enable_all()
        .build_local(&Default::default())?
        .block_on(app::main::single_torrent(
            peer_id,
            &cli.metainfo_uri,
            output_dir,
            dht_cmds,
            pwp_worker.runtime_handle(),
            storage_worker.runtime_handle(),
            !cli.no_upnp,
        ))?;

    Ok(())
}
