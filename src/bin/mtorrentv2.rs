use mtorrent::utils::peer_id::PeerId;
use mtorrent::utils::worker;
use mtorrent::{client, info_stopwatch};
use std::io;

fn main() -> io::Result<()> {
    simple_logger::SimpleLogger::new()
        .with_threads(true)
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent", log::LevelFilter::Debug)
        .init()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{e}")))?;

    let _sw = info_stopwatch!("mtorrentv2");

    let mut args = std::env::args();

    let metainfo_path = args.nth(1).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "metainfo file not specified")
    })?;

    let output_dir = if let Some(arg) = args.next() {
        arg
    } else {
        "mtorrentv2_output".to_owned()
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

    client::single_torrent_main(
        peer_id,
        metainfo_path,
        output_dir,
        pwp_runtime.runtime_handle(),
        storage_runtime.runtime_handle(),
    )?;

    Ok(())
}
