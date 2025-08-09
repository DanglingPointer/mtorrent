pub mod http;
pub mod udp;
pub mod url;

use mtorrent_utils::ip;
use std::net::SocketAddr;

fn parse_binary_ipv4_peers(data: &[u8]) -> impl Iterator<Item = SocketAddr> + '_ {
    ip::SocketAddrV4BytesIter(data).map(SocketAddr::V4)
}

fn parse_binary_ipv6_peers(data: &[u8]) -> impl Iterator<Item = SocketAddr> + '_ {
    ip::SocketAddrV6BytesIter(data).map(SocketAddr::V6)
}
