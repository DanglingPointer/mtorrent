use log::{info, Level};
use mtorrent::sec;
use mtorrent::tracker::utils;
use mtorrent::utils::upnp;
use std::net::SocketAddrV4;
use tokio::time;

#[ignore]
#[tokio::test]
async fn test_async_port_opener() {
    simple_logger::init_with_level(Level::Debug).unwrap();

    let local_ip = utils::get_local_ip().unwrap();
    let local_internal_ip = SocketAddrV4::new(local_ip, 23015);
    let port_opener = upnp::PortOpener::new(local_internal_ip, igd::PortMappingProtocol::TCP)
        .await
        .unwrap();
    info!("port opener created, external ip: {}", port_opener.external_ip());
    time::sleep(sec!(1)).await;
    drop(port_opener);
    info!("port opener dropped");
}
