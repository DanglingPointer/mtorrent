use mtorrent::utils::peer_id::PeerId;
use mtorrent::utils::worker;
use mtorrent::{client, info_stopwatch};
use std::io;
use std::path::{Path, PathBuf};

fn main() -> io::Result<()> {
    simple_logger::SimpleLogger::new()
        .with_threads(false)
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent", log::LevelFilter::Info)
        .with_module_level("mtorrent::ops::peer::metadata", log::LevelFilter::Debug)
        .with_module_level("mtorrent::ops::peer::extensions", log::LevelFilter::Debug)
        // .with_module_level("mtorrent::pwp::channels", log::LevelFilter::Trace)
        .init()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, Box::new(e)))?;

    let _sw = info_stopwatch!("mtorrentv2");

    let mut args = std::env::args();

    let metainfo_uri = args
        .nth(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "metainfo uri not specified"))?;

    let output_dir = if let Some(arg) = args.next() {
        PathBuf::from(arg)
    } else if Path::new(&metainfo_uri).is_file() {
        if let Some(parent) = Path::new(&metainfo_uri).parent() {
            parent.into()
        } else {
            std::env::current_dir()?
        }
    } else {
        std::env::current_dir()?
    };

    let storage_runtime = worker::with_runtime(worker::rt::Config {
        name: "storage".to_owned(),
        io_enabled: false,
        time_enabled: false,
        ..Default::default()
    });

    let pwp_runtime = worker::with_runtime(worker::rt::Config {
        name: "pwp".to_owned(),
        io_enabled: true,
        time_enabled: true,
        ..Default::default()
    });

    let peer_id = PeerId::generate_new();

    tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(
        client::main::single_torrent(
            peer_id,
            &metainfo_uri,
            output_dir,
            pwp_runtime.runtime_handle(),
            storage_runtime.runtime_handle(),
        ),
    )?;

    Ok(())
}
