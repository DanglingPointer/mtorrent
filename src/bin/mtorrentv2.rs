use mtorrent::client;
use mtorrent::utils::peer_id::PeerId;
use mtorrent::utils::worker;
use std::io;

fn main() -> io::Result<()> {
    simple_logger::SimpleLogger::new()
        .with_threads(true)
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent", log::LevelFilter::Debug)
        .init()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{e}")))?;

    let metainfo_path = if let Some(arg) = std::env::args().nth(1) {
        arg
    } else {
        "tests/example.torrent".to_string()
    };

    let output_dir = if let Some(arg) = std::env::args().nth(2) {
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
