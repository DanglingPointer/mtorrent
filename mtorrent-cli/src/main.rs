use clap::Parser;
use local_async_utils::prelude::*;
use mtorrent::app;
use mtorrent::app::dht;
use mtorrent::utils::listener;
use mtorrent_utils::peer_id::PeerId;
use mtorrent_utils::{info_stopwatch, worker};
use std::io;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Magnet link or path to a .torrent file
    metainfo_uri: String,

    /// Output folder
    #[arg(short, long, value_name = "PATH")]
    output: Option<PathBuf>,

    /// Folder to write config files to
    #[arg(long, value_name = "PATH")]
    config_dir: Option<PathBuf>,

    /// Disable UPnP
    #[arg(long)]
    no_upnp: bool,

    /// Disable DHT
    #[arg(long)]
    no_dht: bool,
}

struct SnapshotLogger;

impl listener::StateListener for SnapshotLogger {
    const INTERVAL: Duration = sec!(10);

    fn on_snapshot(&mut self, snapshot: listener::StateSnapshot<'_>) -> ControlFlow<()> {
        log::info!("Periodic state dump:\n{snapshot}");
        ControlFlow::Continue(())
    }
}

fn main() -> io::Result<()> {
    simple_logger::SimpleLogger::new()
        .with_threads(false)
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent", log::LevelFilter::Info)
        .with_module_level("mtorrent_core", log::LevelFilter::Info)
        .with_module_level("mtorrent_utils", log::LevelFilter::Info)
        .with_module_level("mtorrent_dht", log::LevelFilter::Info)
        // .with_module_level("mtorrent::ops::connections", log::LevelFilter::Debug)
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

    let local_data_dir = if let Some(dir) = cli.config_dir {
        dir
    } else {
        match dirs_next::data_local_dir()
            .or_else(dirs_next::data_dir)
            .or_else(dirs_next::config_dir)
        {
            Some(dir) => dir,
            None => std::env::current_dir()?,
        }
    };

    let storage_worker = worker::with_runtime(worker::rt::Config {
        name: "storage".to_owned(),
        io_enabled: false,
        time_enabled: false,
        ..Default::default()
    })?;

    let pwp_worker = worker::with_runtime(worker::rt::Config {
        name: "pwp".to_owned(),
        io_enabled: true,
        time_enabled: true,
        ..Default::default()
    })?;

    let (_dht_worker, dht_cmds) = if !cli.no_dht {
        let (dht_worker, dht_cmds) = dht::launch_dht_node_runtime(dht::Config {
            local_port: 6881,
            max_concurrent_queries: None,
            config_dir: local_data_dir.clone(),
            use_upnp: !cli.no_upnp,
        })?;
        (Some(dht_worker), Some(dht_cmds))
    } else {
        (None, None)
    };

    let peer_id = PeerId::generate_new();

    tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1) // unused
        .enable_all()
        .build_local(Default::default())?
        .block_on(app::main::single_torrent(
            cli.metainfo_uri,
            &mut SnapshotLogger,
            app::main::Config {
                local_peer_id: peer_id,
                config_dir: local_data_dir,
                output_dir,
                use_upnp: !cli.no_upnp,
                pwp_port: None,
            },
            app::main::Context {
                dht_handle: dht_cmds,
                pwp_runtime: pwp_worker.runtime_handle().clone(),
                storage_runtime: storage_worker.runtime_handle().clone(),
            },
        ))?;

    Ok(())
}
