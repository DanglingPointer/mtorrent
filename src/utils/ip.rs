use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::{io, ops};

#[cfg(target_family = "unix")]
pub fn get_local_addr() -> io::Result<Ipv4Addr> {
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

#[cfg(target_family = "windows")]
pub fn get_local_addr() -> io::Result<Ipv4Addr> {
    // naively uses first connected adapter
    ipconfig::get_adapters()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{e}")))?
        .iter()
        .filter(|adapter| matches!(adapter.oper_status(), ipconfig::OperStatus::IfOperStatusUp))
        .flat_map(ipconfig::Adapter::ip_addresses)
        .find_map(|addr| match addr {
            IpAddr::V4(ipv4) => Some(*ipv4),
            _ => None,
        })
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "IPv4 not found"))
}

const DYNAMIC_PORT_RANGE: ops::Range<u32> = 49152..65536;

pub fn any_socketaddr_from_hash(h: &impl Hash) -> SocketAddr {
    let mut hasher = DefaultHasher::default();
    h.hash(&mut hasher);
    let hashed = hasher.finish();
    let port = hashed % DYNAMIC_PORT_RANGE.len() as u64 + DYNAMIC_PORT_RANGE.start as u64;
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port as u16))
}
