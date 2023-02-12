use log::{info, Level};
use mtorrent::utils::upnp;
use std::io;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::thread::sleep;
use std::time::Duration;

fn get_local_ip() -> io::Result<Ipv4Addr> {
    let hostname_out = std::process::Command::new("hostname").arg("-I").output()?;
    let ipv4_string = String::from_utf8_lossy(&hostname_out.stdout)
        .split_once(' ')
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "Unexpected output from 'hostname -I'")
        })?
        .0
        .to_string();
    ipv4_string
        .parse::<Ipv4Addr>()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
}

#[ignore]
#[test]
fn test_port_opener() {
    simple_logger::init_with_level(Level::Debug).unwrap();

    let local_ip = get_local_ip().unwrap();
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
