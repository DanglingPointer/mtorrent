use log::{info, Level};
use mtorrent::tracker::utils;
use mtorrent::utils::upnp;
use std::net::SocketAddrV4;
use std::thread::sleep;
use std::time::Duration;

#[ignore]
#[test]
fn test_port_opener() {
    simple_logger::init_with_level(Level::Debug).unwrap();

    let local_ip = utils::get_local_ip().unwrap();
    let local_internal_ip = SocketAddrV4::new(local_ip, 23015);
    let port_opener = upnp::PortOpener::new(
        local_internal_ip,
        igd::PortMappingProtocol::TCP,
        Duration::from_secs(10),
    )
    .unwrap();
    info!("port opener created, external ip: {}", port_opener.external_ip());
    sleep(Duration::from_secs(1));
    drop(port_opener);
    info!("port opener dropped");
}
