use crate::utils::meta::Metainfo;
use std::collections::HashSet;
use std::iter;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

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

pub fn get_udp_tracker_addrs(metainfo: &Metainfo) -> HashSet<String> {
    fn filter_uri(uri: &str) -> Option<String> {
        let udp_addr = uri.strip_prefix("udp://")?;
        if let Some(stripped) = udp_addr.strip_suffix("/announce") {
            Some(stripped.to_string())
        } else {
            Some(udp_addr.to_string())
        }
    }
    if let Some(announce_list) = metainfo.announce_list() {
        announce_list.flatten().filter_map(filter_uri).collect()
    } else if let Some(Some(url)) = metainfo.announce().map(filter_uri) {
        iter::once(url).collect()
    } else {
        Default::default()
    }
}

pub fn get_http_tracker_addrs(metainfo: &Metainfo) -> HashSet<String> {
    fn filter_url(url: &str) -> Option<String> {
        if url.starts_with("http://") || url.starts_with("https://") {
            Some(url.to_string())
        } else {
            None
        }
    }
    if let Some(announce_list) = metainfo.announce_list() {
        announce_list.flatten().filter_map(filter_url).collect()
    } else if let Some(Some(url)) = metainfo.announce().map(filter_url) {
        iter::once(url).collect()
    } else {
        Default::default()
    }
}
