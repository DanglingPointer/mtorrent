use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::{io, ops};
use tokio::net::TcpSocket;

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
        .map_err(|e| io::Error::new(io::ErrorKind::Other, Box::new(e)))
}

#[cfg(target_family = "windows")]
pub fn get_local_addr() -> io::Result<Ipv4Addr> {
    // naively uses first connected adapter
    ipconfig::get_adapters()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, Box::new(e)))?
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

pub fn any_ipv4_socketaddr_from_hash(h: &impl Hash) -> SocketAddr {
    let mut hasher = DefaultHasher::default();
    h.hash(&mut hasher);
    let hashed = hasher.finish();
    let port = hashed % DYNAMIC_PORT_RANGE.len() as u64 + DYNAMIC_PORT_RANGE.start as u64;
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port as u16))
}

pub fn bound_tcp_socket(local_addr: SocketAddr) -> io::Result<TcpSocket> {
    let socket = match local_addr {
        SocketAddr::V4(_) => TcpSocket::new_v4()?,
        SocketAddr::V6(_) => TcpSocket::new_v6()?,
    };

    // To use the same local addr and port for outgoing PWP connections and for TCP listener,
    // (in order to deal with endpoint-independent NAT mappings, https://www.rfc-editor.org/rfc/rfc5128#section-2.3)
    // we need to set SO_REUSEADDR on Windows, and SO_REUSEADDR and SO_REUSEPORT on Linux.
    // See https://stackoverflow.com/a/14388707/4432988 for details.
    socket.set_reuseaddr(true)?;
    #[cfg(not(windows))]
    socket.set_reuseport(true)?;
    socket.bind(local_addr)?;
    Ok(socket)
}
