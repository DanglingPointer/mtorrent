use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::Ipv4Addr;
use std::{io, ops};

#[cfg(target_family = "unix")]
pub fn get_local_addr() -> io::Result<Ipv4Addr> {
    let hostname_out = std::process::Command::new("hostname").arg("-I").output()?;
    let ipv4_string = String::from_utf8_lossy(&hostname_out.stdout)
        .split_once(' ')
        .ok_or_else(|| io::Error::other("Unexpected output from 'hostname -I'"))?
        .0
        .to_string();
    ipv4_string.parse::<Ipv4Addr>().map_err(io::Error::other)
}

#[cfg(target_family = "windows")]
pub fn get_local_addr() -> io::Result<Ipv4Addr> {
    // naively uses first connected adapter
    ipconfig::get_adapters()
        .map_err(io::Error::other)?
        .iter()
        .filter(|adapter| matches!(adapter.oper_status(), ipconfig::OperStatus::IfOperStatusUp))
        .flat_map(ipconfig::Adapter::ip_addresses)
        .find_map(|addr| match addr {
            std::net::IpAddr::V4(ipv4) => Some(*ipv4),
            _ => None,
        })
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "IPv4 not found"))
}

const DYNAMIC_PORT_RANGE: ops::Range<u32> = 49152..65536;

pub fn port_from_hash(h: &impl Hash) -> u16 {
    let mut hasher = DefaultHasher::default();
    h.hash(&mut hasher);
    let hashed = hasher.finish();
    let port = hashed % DYNAMIC_PORT_RANGE.len() as u64 + DYNAMIC_PORT_RANGE.start as u64;
    port as u16
}
