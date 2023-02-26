use crate::utils::meta::Metainfo;
use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

pub(super) fn parse_binary_ipv4_peers(data: &[u8]) -> impl Iterator<Item = SocketAddr> + '_ {
    fn to_addr_and_port(src: &[u8]) -> Option<SocketAddr> {
        let (addr_data, port_data) = src.split_at(4);
        Some(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::from(u32::from_be_bytes(addr_data.try_into().ok()?))),
            u16::from_be_bytes(port_data.try_into().ok()?),
        ))
    }
    data.chunks_exact(6).filter_map(to_addr_and_port)
}

pub fn get_udp_tracker_addrs(metainfo: &Metainfo) -> Vec<String> {
    match metainfo.announce_list() {
        Some(announce_list) => announce_list
            .flatten()
            .filter_map(|uri| {
                let udp_addr = uri.strip_prefix("udp://")?;
                if let Some(stripped) = udp_addr.strip_suffix("/announce") {
                    Some(stripped.to_string())
                } else {
                    Some(udp_addr.to_string())
                }
            })
            .collect(),
        None => Vec::new(),
    }
}

pub fn get_http_tracker_addrs(metainfo: &Metainfo) -> Vec<String> {
    match metainfo.announce_list() {
        Some(announce_list) => announce_list
            .flatten()
            .filter_map(|url| {
                if url.starts_with("http://") || url.starts_with("https://") {
                    Some(url.to_string())
                } else {
                    None
                }
            })
            .collect(),
        None => Vec::new(),
    }
}

pub fn get_local_ip() -> io::Result<Ipv4Addr> {
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
