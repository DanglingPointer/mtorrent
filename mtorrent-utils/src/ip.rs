use bytes::Buf;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::{Ipv4Addr, SocketAddrV4, SocketAddrV6};
use std::{io, ops};

/// Get local (non-loopback) IPv4.
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

/// Get local (non-loopback) IPv4.
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

/// Get a random port from the dynamic range (49152 <= port < 65536) based on the hash.
pub fn port_from_hash(h: &impl Hash) -> u16 {
    let mut hasher = DefaultHasher::default();
    h.hash(&mut hasher);
    let hashed = hasher.finish();
    let port = hashed % DYNAMIC_PORT_RANGE.len() as u64 + DYNAMIC_PORT_RANGE.start as u64;
    port as u16
}

/// Iterator decoding compact representation of IPv4 addresses from bytes (6 bytes per address).
pub struct SocketAddrV4BytesIter<'d>(pub &'d [u8]);

impl<'d> Iterator for SocketAddrV4BytesIter<'d> {
    type Item = SocketAddrV4;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.remaining() >= 6 {
            let ip = self.0.get_u32();
            let port = self.0.get_u16();
            Some(SocketAddrV4::new(ip.into(), port))
        } else {
            None
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<'d> ExactSizeIterator for SocketAddrV4BytesIter<'d> {
    fn len(&self) -> usize {
        self.0.len() / 6
    }
}

/// Iterator decoding compact representation of IPv6 addresses from bytes (18 bytes per address).
pub struct SocketAddrV6BytesIter<'d>(pub &'d [u8]);

impl<'d> Iterator for SocketAddrV6BytesIter<'d> {
    type Item = SocketAddrV6;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.remaining() >= 18 {
            let ip = self.0.get_u128();
            let port = self.0.get_u16();
            Some(SocketAddrV6::new(ip.into(), port, 0, 0))
        } else {
            None
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<'d> ExactSizeIterator for SocketAddrV6BytesIter<'d> {
    fn len(&self) -> usize {
        self.0.len() / 18
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter;
    use std::net::Ipv6Addr;

    #[test]
    fn test_socketaddr_v4_iter() {
        fn random_addr() -> SocketAddrV4 {
            SocketAddrV4::new(Ipv4Addr::from_bits(rand::random()), rand::random())
        }
        let addrs: Vec<SocketAddrV4> = iter::repeat_with(random_addr).take(10).collect();
        let data = {
            let mut buf = Vec::new();
            for addr in &addrs {
                buf.extend_from_slice(&addr.ip().octets());
                buf.extend_from_slice(&addr.port().to_be_bytes());
            }
            buf
        };
        assert_eq!(SocketAddrV4BytesIter(&data).len(), addrs.len());
        assert_eq!(SocketAddrV4BytesIter(&data).collect::<Vec<_>>(), addrs);
    }

    #[test]
    fn test_socketaddr_v6_iter() {
        fn random_addr() -> SocketAddrV6 {
            SocketAddrV6::new(Ipv6Addr::from_bits(rand::random()), rand::random(), 0, 0)
        }
        let addrs: Vec<SocketAddrV6> = iter::repeat_with(random_addr).take(10).collect();
        let data = {
            let mut buf = Vec::new();
            for addr in &addrs {
                buf.extend_from_slice(&addr.ip().octets());
                buf.extend_from_slice(&addr.port().to_be_bytes());
            }
            buf
        };
        assert_eq!(SocketAddrV6BytesIter(&data).len(), addrs.len());
        assert_eq!(SocketAddrV6BytesIter(&data).collect::<Vec<_>>(), addrs);
    }
}
