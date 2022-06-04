use std::net::{IpAddr, Ipv4Addr, SocketAddr};

pub(super) fn parse_binary_ipv4_peers(data: &[u8]) -> impl Iterator<Item = SocketAddr> + '_ {
    fn to_addr_and_port(src: &[u8]) -> Result<SocketAddr, ()> {
        let (addr_data, port_data) = src.split_at(4);
        Ok(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::from(u32::from_be_bytes(addr_data.try_into().map_err(|_| ())?))),
            u16::from_be_bytes(port_data.try_into().map_err(|_| ())?),
        ))
    }
    data.chunks_exact(6).filter_map(|data| to_addr_and_port(data).ok())
}
