use log::{debug, error, info, Level};
use mtorrent::ctrl::OperationHandler;
use mtorrent::data::Storage;
use mtorrent::tracker::utils;
use mtorrent::utils::benc;
use mtorrent::utils::dispatch::Dispatcher;
use mtorrent::utils::meta::Metainfo;
use mtorrent::utils::upnp;
use std::net::SocketAddrV4;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{env, fs, io};

fn read_metainfo<P: AsRef<Path>>(metainfo_filepath: P) -> io::Result<Metainfo> {
    let file_content = fs::read(metainfo_filepath)?;
    let root_entity = benc::Element::from_bytes(&file_content)?;
    Metainfo::try_from(root_entity)
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "Invalid metainfo file"))
}

fn generate_local_peer_id() -> [u8; 20] {
    let mut ret = [0u8; 20];
    let maj = str::parse::<u8>(env!("CARGO_PKG_VERSION_MAJOR")).unwrap_or(b'x');
    let min = str::parse::<u8>(env!("CARGO_PKG_VERSION_MINOR")).unwrap_or(b'x');
    let pat = str::parse::<u8>(env!("CARGO_PKG_VERSION_PATCH")).unwrap_or(b'x');

    let s = format!("-mt0{}{}{}-", maj, min, pat);
    ret[..8].copy_from_slice(s.as_bytes());

    for b in &mut ret[8..] {
        *b = rand::random::<u8>() % (127 - 32) + 32;
    }
    ret
}

fn main() -> io::Result<()> {
    simple_logger::init_with_level(Level::Debug)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}", e)))?;

    let metainfo = read_metainfo(if let Some(arg) = env::args().nth(1) {
        arg
    } else {
        "tests/example.torrent".to_string()
    })?;
    info!("Successfully consumed metainfo file for '{}'", metainfo.name().unwrap_or("<unknown>"));

    let tracker_addrs = utils::get_udp_tracker_addrs(&metainfo);
    for addr in &tracker_addrs {
        info!("UDP tracker found: {}", addr);
    }

    let local_internal_ip = SocketAddrV4::new(utils::get_local_ip()?, 23015);
    info!("Local internal ip address: {}", local_internal_ip);

    let output_dir = if let Some(arg) = env::args().nth(2) {
        arg
    } else {
        "test_output".to_string()
    };
    let _ = fs::remove_dir_all(&output_dir);
    let filekeeper = if let Some(files) = metainfo.files() {
        Storage::new(output_dir, files)?
    } else {
        let name = match metainfo.name() {
            Some(s) => s.to_string(),
            None => String::from_utf8_lossy(metainfo.info_hash()).to_string(),
        };
        Storage::new(
            output_dir,
            [(
                metainfo.length().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "No 'length' in metainfo file")
                })?,
                PathBuf::from(name),
            )]
            .into_iter(),
        )?
    };

    let port_opener_result = upnp::PortOpener::new(
        local_internal_ip,
        igd::PortMappingProtocol::TCP,
        Duration::from_secs(60),
    );
    let local_external_ip = match &port_opener_result {
        Ok(port_opener) => {
            debug!("UPnP succeeded");
            info!("Local external ip address: {}", port_opener.external_ip());
            port_opener.external_ip()
        }
        Err(e) => {
            error!("UPnP failed: {}", e);
            local_internal_ip
        }
    };

    let local_peer_id = generate_local_peer_id();
    info!("Local peer id: {}", String::from_utf8_lossy(&local_peer_id));

    let ctrl = OperationHandler::new(
        metainfo,
        filekeeper,
        local_internal_ip,
        local_external_ip,
        local_peer_id,
    )
    .unwrap();

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let mut dispatcher = Dispatcher::new(ctrl);
            while dispatcher.dispatch_one().await {}
        });

    Ok(())
}
