use crate::storage::meta::MetaInfo;
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

pub fn get_udp_tracker_addrs(metainfo: &MetaInfo) -> Vec<String> {
    let mut udp_trackers = Vec::<String>::new();
    if let Some(announce_list) = metainfo.announce_list() {
        for list in announce_list {
            for addr in list {
                if let Some(addr) = addr.strip_prefix("udp://") {
                    if let Some(stripped_addr) = addr.strip_suffix("/announce") {
                        udp_trackers.push(stripped_addr.to_string());
                    } else {
                        udp_trackers.push(addr.to_string());
                    }
                }
            }
        }
    }
    udp_trackers
}
