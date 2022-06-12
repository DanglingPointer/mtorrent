use log::{debug, error, info, Level};
use mtorrent::benc;
use mtorrent::ctrl::OperationController;
use mtorrent::dispatch::Dispatcher;
use mtorrent::port_opener::PortOpener;
use mtorrent::storage::meta::MetaInfo;
use mtorrent::tracker::utils;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::Path;
use std::{env, fs, io, num::ParseIntError};

fn read_metainfo<P: AsRef<Path>>(metainfo_filepath: P) -> io::Result<MetaInfo> {
    let file_content = fs::read(metainfo_filepath)?;
    let root_entity = benc::Element::from_bytes(&file_content)?;
    MetaInfo::try_from(root_entity)
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "Invalid metainfo file"))
}

fn get_local_ip() -> io::Result<Ipv4Addr> {
    let hostname_out = std::process::Command::new("hostname").arg("-I").output()?;
    let ipv4_string = String::from_utf8_lossy(&hostname_out.stdout)
        .split_once(" ")
        .ok_or(io::Error::new(io::ErrorKind::Other, "Unexpected output from 'hostname -I'"))?
        .0
        .to_string();
    ipv4_string
        .parse::<Ipv4Addr>()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
}

fn generate_local_peer_id() -> Result<[u8; 20], ParseIntError> {
    let mut ret = [0u8; 20];
    let maj = str::parse::<u8>(env!("CARGO_PKG_VERSION_MAJOR"))?;
    let min = str::parse::<u8>(env!("CARGO_PKG_VERSION_MINOR"))?;
    let pat = str::parse::<u8>(env!("CARGO_PKG_VERSION_PATCH"))?;

    let s = format!("-mt0{}{}{}-", maj, min, pat);
    ret[..8].copy_from_slice(s.as_bytes());

    for b in &mut ret[8..] {
        *b = rand::random::<u8>() % (127 - 32) + 32;
    }
    Ok(ret)
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

    let local_internal_ip = SocketAddrV4::new(get_local_ip()?, 6889);
    info!("Local internal ip address: {}", local_internal_ip);

    let port_opener_result = PortOpener::new(local_internal_ip, igd::PortMappingProtocol::TCP);
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

    let local_peer_id = match generate_local_peer_id() {
        Ok(id) => id,
        Err(_) => [0xaeu8; 20],
    };
    info!("Local peer id: {}", String::from_utf8_lossy(&local_peer_id));

    let ctrl =
        OperationController::new(metainfo, local_internal_ip, local_external_ip, local_peer_id)
            .unwrap();

    async_io::block_on(async move {
        let mut dispatcher = Dispatcher::new(ctrl);
        while dispatcher.dispatch_one().await {}
    });

    Ok(())
}
