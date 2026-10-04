use clap::Parser;
use local_async_utils::prelude::*;
use mtorrent::app;
use mtorrent::app::dht;
use mtorrent::utils::listener;
use mtorrent_base::input;
use mtorrent_utils::peer_id::PeerId;
use mtorrent_utils::{info_stopwatch, worker};
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::signal;

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

    /// Port for peer connections (both TCP and uTP)
    #[arg(short, long)]
    port: Option<u16>,

    /// Name of network interface to bind all sockets to (e.g. "eth0" or "lo").
    #[arg(short, long)]
    interface: Option<String>,

    /// Disable UPnP
    #[arg(long)]
    no_upnp: bool,

    /// Disable DHT
    #[arg(long)]
    no_dht: bool,

    /// Keep seeding after the download is complete (until interrupted)
    #[arg(long)]
    seed: bool,

    /// Download only the files with these comma-separated 0-based indices (see --list-files).
    /// Other files created by mtorrent are deleted when the download stops
    #[arg(
        short,
        long,
        value_name = "INDICES",
        value_delimiter = ',',
        conflicts_with_all = ["seed", "list_files"],
    )]
    files: Vec<usize>,

    /// Print the files of a .torrent file with their indices and exit
    #[arg(long, conflicts_with_all = ["seed", "files"])]
    list_files: bool,
}

fn list_files(metainfo_uri: &str) -> io::Result<()> {
    if !Path::new(metainfo_uri).is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--list-files requires a path to a .torrent file",
        ));
    }
    let metainfo = input::Metainfo::from_file(metainfo_uri)?;

    let files: Vec<(usize, PathBuf)> = if let Some(files) = metainfo.files() {
        files.collect()
    } else {
        let length = metainfo
            .length()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no length in metainfo"))?;
        let name = metainfo.name().unwrap_or("unnamed");
        vec![(length, PathBuf::from(name))]
    };

    let index_width = files.len().saturating_sub(1).to_string().len();
    let length_width = files.iter().map(|(len, _)| len.to_string().len()).max().unwrap_or(0);
    for (index, (length, path)) in files.iter().enumerate() {
        println!("{index:>index_width$}  {length:>length_width$}  {}", path.display());
    }
    Ok(())
}

struct SnapshotLogger;

impl listener::StateListener for SnapshotLogger {
    const INTERVAL: Duration = sec!(10);

    fn on_snapshot(&mut self, snapshot: listener::StateSnapshot<'_>) {
        log::info!("Periodic state dump:\n{snapshot}");
    }
}

fn main() -> io::Result<()> {
    simple_logger::SimpleLogger::new()
        .with_threads(false)
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent", log::LevelFilter::Info)
        .with_module_level("mtorrent_base", log::LevelFilter::Info)
        .with_module_level("mtorrent_utils", log::LevelFilter::Info)
        .with_module_level("mtorrent_dht", log::LevelFilter::Info)
        // .with_module_level("mtorrent_base::utp", log::LevelFilter::Debug)
        // .with_module_level("mtorrent::core::connections", log::LevelFilter::Debug)
        // .with_module_level("mtorrent::core::peer::metadata", log::LevelFilter::Debug)
        // .with_module_level("mtorrent::core::peer::extensions", log::LevelFilter::Debug)
        // .with_module_level("mtorrent_base::pwp", log::LevelFilter::Trace)
        .init()
        .map_err(io::Error::other)?;

    let _sw = info_stopwatch!("mtorrent");

    let cli = Cli::parse();

    if cli.list_files {
        return list_files(&cli.metainfo_uri);
    }

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
        match dirs::data_local_dir().or_else(dirs::data_dir).or_else(dirs::config_dir) {
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

    let pwp_worker = worker::with_local_runtime(worker::rt::Config {
        name: "pwp".to_owned(),
        io_enabled: true,
        time_enabled: true,
        #[cfg(coverage)]
        stack_size: 1024 * 1024,
        ..Default::default()
    })?;

    // spawn ctrl+c handler on pwp runtime because it requires I/O
    let ctrl_c_handle = pwp_worker.runtime_handle().spawn(async {
        _ = signal::ctrl_c().await;
    });

    let (_dht_worker, dht_cmds) = if !cli.no_dht {
        let (dht_worker, dht_cmds) = dht::launch_dht_node_runtime(dht::Config {
            local_port: 6881,
            bind_interface: cli.interface.clone(),
            max_concurrent_queries: None,
            config_dir: local_data_dir.clone(),
            use_upnp: !cli.no_upnp,
            bootstrap_nodes_override: None,
            query_timeout: None,
        })?;
        (Some(dht_worker), Some(dht_cmds))
    } else {
        (None, None)
    };

    let peer_id = PeerId::generate_new();

    let _outcome = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1) // unused
        .enable_time()
        .build_local(Default::default())?
        .block_on(app::main::single_torrent(
            cli.metainfo_uri,
            &mut SnapshotLogger,
            async { _ = ctrl_c_handle.await },
            app::main::Config {
                local_peer_id: peer_id,
                config_dir: local_data_dir,
                output_dir,
                use_upnp: !cli.no_upnp,
                pwp_port: cli.port,
                bind_interface: cli.interface,
                download_strategy: Default::default(),
                mode: if cli.seed {
                    app::main::Mode::Seeder
                } else {
                    app::main::Mode::Leech
                },
                file_selection: if cli.files.is_empty() {
                    app::main::FileSelection::All
                } else {
                    app::main::FileSelection::Only(cli.files)
                },
            },
            app::main::Context {
                dht_handle: dht_cmds,
                pwp_runtime: pwp_worker.runtime_handle().clone(),
                storage_runtime: storage_worker.runtime_handle().clone(),
            },
        ))?;

    Ok(())
}
