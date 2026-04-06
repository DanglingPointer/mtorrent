use bytes::Buf;
use socket2::SockRef;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::time::Duration;
use std::{io, ops};
use tokio::net::{TcpSocket, UdpSocket};

#[cfg(not(windows))]
pub(crate) fn get_local_addr(mut predicate: impl FnMut(&IpAddr) -> bool) -> Option<IpAddr> {
    sysinfo::Networks::new_with_refreshed_list()
        .values()
        .flat_map(sysinfo::NetworkData::ip_networks)
        .filter_map(|network| predicate(&network.addr).then_some(network.addr))
        .next()
}

#[cfg(windows)]
pub(crate) fn get_local_addr(predicate: impl FnMut(&IpAddr) -> bool) -> Option<IpAddr> {
    // can't use sysinfo on Windows because it doesn't return SW adapters, e.g. loopback
    let adapters = ipconfig::get_adapters().ok()?;
    adapters
        .iter()
        .filter(|adapter| matches!(adapter.oper_status(), ipconfig::OperStatus::IfOperStatusUp))
        .flat_map(ipconfig::Adapter::ip_addresses)
        .cloned()
        .find(predicate)
}

// ------------------------------------------------------------------------------------------------

#[cfg(windows)]
fn get_adapter_addrs<'a>(
    adapters: impl IntoIterator<Item = &'a ipconfig::Adapter>,
    iface: &str,
) -> impl Iterator<Item = &'a IpAddr> {
    adapters
        .into_iter()
        .filter(move |adapter| {
            matches!(adapter.oper_status(), ipconfig::OperStatus::IfOperStatusUp)
                && (adapter.adapter_name() == iface || adapter.friendly_name() == iface)
        })
        .flat_map(ipconfig::Adapter::ip_addresses)
}

/// Get the local IPv4 address to bind to, based on the specified network interface (if any).
pub fn get_bind_addr_v4(interface: Option<&str>) -> Ipv4Addr {
    let Some(iface) = interface else {
        return Ipv4Addr::UNSPECIFIED;
    };

    #[cfg(windows)]
    if let Ok(adapters) = ipconfig::get_adapters() {
        let found = get_adapter_addrs(&adapters, iface).find_map(|addr| match addr {
            IpAddr::V4(ipv4) => Some(*ipv4),
            _ => None,
        });
        debug_assert!(found.is_some(), "failed to find network adapter");
        return found.unwrap_or(Ipv4Addr::UNSPECIFIED);
    }

    #[cfg(not(windows))]
    if let Some(network_data) = sysinfo::Networks::new_with_refreshed_list().get(iface) {
        let found = network_data.ip_networks().iter().find_map(|network| match network.addr {
            IpAddr::V4(ipv4) => Some(ipv4),
            _ => None,
        });
        debug_assert!(found.is_some(), "failed to find network adapter");
        return found.unwrap_or(Ipv4Addr::UNSPECIFIED);
    }

    Ipv4Addr::UNSPECIFIED
}

/// Get the local IPv6 address to bind to, based on the specified network interface (if any).
pub fn get_bind_addr_v6(interface: Option<&str>) -> Ipv6Addr {
    let Some(iface) = interface else {
        return Ipv6Addr::UNSPECIFIED;
    };

    #[cfg(windows)]
    if let Ok(adapters) = ipconfig::get_adapters() {
        let found = get_adapter_addrs(&adapters, iface).find_map(|addr| match addr {
            IpAddr::V6(ipv6) => Some(*ipv6),
            _ => None,
        });
        debug_assert!(found.is_some(), "failed to find network adapter");
        return found.unwrap_or(Ipv6Addr::UNSPECIFIED);
    }

    #[cfg(not(windows))]
    if let Some(network_data) = sysinfo::Networks::new_with_refreshed_list().get(iface) {
        let found = network_data.ip_networks().iter().find_map(|network| match network.addr {
            IpAddr::V6(ipv6) => Some(ipv6),
            _ => None,
        });
        debug_assert!(found.is_some(), "failed to find network adapter");
        return found.unwrap_or(Ipv6Addr::UNSPECIFIED);
    }

    Ipv6Addr::UNSPECIFIED
}

// ------------------------------------------------------------------------------------------------

const MIN_UDP_BUF_SIZE: usize = 64 * 1024;

/// Create a UDP socket bound to the specified local address and network interface (if any).
/// The SO_RCVBUF and SO_SNDBUF options are set to at least 64 KB, to avoid dropping large UDP
/// packets.
pub fn bound_udp_socket(local_addr: SocketAddr, interface: Option<&str>) -> io::Result<UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(local_addr),
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;

    // set options
    if local_addr.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    if let Ok(buf_size) = socket.send_buffer_size()
        && buf_size < MIN_UDP_BUF_SIZE
    {
        socket.set_send_buffer_size(MIN_UDP_BUF_SIZE)?;
    }
    if let Ok(buf_size) = socket.recv_buffer_size()
        && buf_size < MIN_UDP_BUF_SIZE
    {
        socket.set_recv_buffer_size(MIN_UDP_BUF_SIZE)?;
    }
    socket.set_nonblocking(true)?;

    // bind
    if let Some(interface) = interface {
        bind_to_interface(&socket, interface)?;
    }
    socket.bind(&local_addr.into())?;

    // convert to tokio socket
    let std_socket = std::net::UdpSocket::from(socket);
    UdpSocket::from_std(std_socket)
}

/// Create a TCP socket bound to the specified local address and network interface (if any).
/// The following socket options are set on the created socket:
/// - SO_REUSEADDR (on all platforms) and SO_REUSEPORT (on unix)
/// - SO_LINGER with 0 timeout, to avoid TIME_WAIT
/// - TCP_NODELAY
pub fn bound_tcp_socket(local_addr: SocketAddr, interface: Option<&str>) -> io::Result<TcpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(local_addr),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;

    // set options
    if local_addr.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    // To use the same local addr and port for outgoing PWP connections and for TCP listener,
    // (in order to deal with endpoint-independent NAT mappings, https://www.rfc-editor.org/rfc/rfc5128#section-2.3)
    // we need to set SO_REUSEADDR on Windows, and SO_REUSEADDR and SO_REUSEPORT on Linux.
    // See https://stackoverflow.com/a/14388707/4432988 for details.
    socket.set_reuse_address(true)?;
    #[cfg(not(windows))]
    socket.set_reuse_port(true)?;
    // To avoid putting socket into TIME_WAIT when disconnecting someone, enable SO_LINGER with 0
    // timeout See https://stackoverflow.com/a/71975993
    socket.set_linger(Some(Duration::ZERO))?;
    socket.set_tcp_nodelay(true)?;
    socket.set_nonblocking(true)?;

    // bind
    if let Some(interface) = interface {
        bind_to_interface(&socket, interface)?;
    }
    socket.bind(&local_addr.into())?;

    // convert to tokio socket
    #[cfg(any(unix, all(target_os = "wasi", not(target_env = "p1"))))]
    unsafe {
        use std::os::fd::{FromRawFd, IntoRawFd};
        Ok(FromRawFd::from_raw_fd(socket.into_raw_fd()))
    }
    #[cfg(windows)]
    unsafe {
        use std::os::windows::io::{FromRawSocket, IntoRawSocket};
        Ok(FromRawSocket::from_raw_socket(socket.into_raw_socket()))
    }
}

// ------------------------------------------------------------------------------------------------

/// Bind a socket to a specific network interface. Does nothing on Windows.
#[cfg(target_os = "windows")]
fn bind_to_interface<'s>(_socket: impl Into<SockRef<'s>>, _interface: &str) -> io::Result<()> {
    // not supported on Windows
    Ok(())
}

/// Bind a socket to a specific network interface.
#[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
fn bind_to_interface<'s>(socket: impl Into<SockRef<'s>>, interface: &str) -> io::Result<()> {
    let socket = socket.into();

    socket.bind_device(Some(interface.as_bytes()))?;
    Ok(())
}

/// Bind a socket to a specific network interface.
#[cfg(any(
    target_os = "illumos",
    target_os = "ios",
    target_os = "macos",
    target_os = "solaris",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "watchos",
))]
fn bind_to_interface<'s>(socket: impl Into<SockRef<'s>>, interface: &str) -> io::Result<()> {
    let socket = socket.into();

    let interface = std::ffi::CString::new(interface)?;
    let idx = unsafe { libc::if_nametoindex(interface.as_ptr()) };
    let Some(idx) = std::num::NonZeroU32::new(idx) else {
        return Err(io::Error::new(io::ErrorKind::NotFound, "interface not found"));
    };

    let local_addr = socket.local_addr()?;
    let Some(local_addr) = local_addr.as_socket() else {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "socket is not AF_INET"));
    };
    match local_addr {
        SocketAddr::V4(_) => socket.bind_device_by_index_v4(Some(idx))?,
        SocketAddr::V6(_) => socket.bind_device_by_index_v6(Some(idx))?,
    }

    Ok(())
}

// ------------------------------------------------------------------------------------------------

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

    fn loopback_interface() -> &'static str {
        if cfg!(target_os = "windows") {
            "Loopback Pseudo-Interface 1"
        } else if cfg!(target_os = "macos") {
            "lo0"
        } else {
            "lo"
        }
    }

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

    #[test]
    fn test_get_bind_addr() {
        let iface = loopback_interface();

        let addr = get_bind_addr_v4(Some(iface));
        assert_eq!(addr, Ipv4Addr::LOCALHOST);

        let addr = get_bind_addr_v6(Some(iface));
        assert!(addr.is_loopback() || addr.is_unicast_link_local());
    }

    #[test]
    fn test_get_local_addr() {
        let addr = get_local_addr(|addr| addr.is_loopback() && addr.is_ipv4());
        assert_eq!(addr, Some(IpAddr::V4(Ipv4Addr::LOCALHOST)));

        let addr = get_local_addr(|addr| !addr.is_loopback() && !addr.is_unspecified());
        let addr = addr.expect("failed to find non-loopback address");
        assert!(!addr.is_loopback());
        assert!(!addr.is_unspecified());
    }

    #[test]
    fn test_bound_tcp_sockets_reuse_ipv4_addr() {
        let iface = loopback_interface();

        let sock1 = bound_tcp_socket((Ipv4Addr::LOCALHOST, 0).into(), Some(iface)).unwrap();
        let local_addr1 = sock1.local_addr().unwrap();
        assert!(local_addr1.ip().is_loopback());
        assert!(local_addr1.is_ipv4());
        assert_ne!(local_addr1.port(), 0);

        let sock2 = bound_tcp_socket(local_addr1, Some(iface)).unwrap();
        assert_eq!(sock1.local_addr().unwrap(), sock2.local_addr().unwrap());
        let local_addr2 = sock2.local_addr().unwrap();
        assert_eq!(local_addr1, local_addr2);
    }

    #[test]
    fn test_bound_tcp_sockets_reuse_ipv6_addr() {
        let iface = loopback_interface();

        let sock1 = bound_tcp_socket((Ipv6Addr::LOCALHOST, 0).into(), Some(iface)).unwrap();
        let SocketAddr::V6(local_addr1) = sock1.local_addr().unwrap() else {
            panic!("expected IPv6 address");
        };
        assert!(local_addr1.ip().is_loopback());
        assert_ne!(local_addr1.port(), 0);

        let sock2 = bound_tcp_socket(local_addr1.into(), Some(iface)).unwrap();
        assert_eq!(sock1.local_addr().unwrap(), sock2.local_addr().unwrap());
        let local_addr2 = sock2.local_addr().unwrap();
        assert_eq!(SocketAddr::V6(local_addr1), local_addr2);
    }

    #[test]
    fn test_bound_tcp_sockets_ipv4_and_ipv6_same_port() {
        let iface = loopback_interface();

        let sock1 = bound_tcp_socket((Ipv4Addr::LOCALHOST, 0).into(), Some(iface)).unwrap();
        let local_addr1 = sock1.local_addr().unwrap();
        assert!(local_addr1.ip().is_loopback());
        assert!(local_addr1.is_ipv4());
        assert_ne!(local_addr1.port(), 0);

        let sock2 = bound_tcp_socket((Ipv6Addr::LOCALHOST, local_addr1.port()).into(), Some(iface))
            .unwrap();
        let SocketAddr::V6(local_addr2) = sock2.local_addr().unwrap() else {
            panic!("expected IPv6 address");
        };
        assert!(local_addr2.ip().is_loopback());
        assert_eq!(local_addr2.port(), local_addr1.port());
    }

    #[tokio::test]
    async fn test_bound_udp_sockets_ipv4_and_ipv6_same_port() {
        let iface = loopback_interface();

        let sock1 = bound_udp_socket((Ipv4Addr::LOCALHOST, 0).into(), Some(iface)).unwrap();
        let local_addr1 = sock1.local_addr().unwrap();
        assert!(local_addr1.ip().is_loopback());
        assert!(local_addr1.is_ipv4());
        assert_ne!(local_addr1.port(), 0);

        let sock2 = bound_udp_socket((Ipv6Addr::LOCALHOST, local_addr1.port()).into(), Some(iface))
            .unwrap();
        let SocketAddr::V6(local_addr2) = sock2.local_addr().unwrap() else {
            panic!("expected IPv6 address");
        };
        assert!(local_addr2.ip().is_loopback());
        assert_eq!(local_addr2.port(), local_addr1.port());

        let sock1_ref = SockRef::from(&sock1);
        let sock2_ref = SockRef::from(&sock2);
        let sock1_send_buf = sock1_ref.send_buffer_size().unwrap();
        let sock1_recv_buf = sock1_ref.recv_buffer_size().unwrap();
        let sock2_send_buf = sock2_ref.send_buffer_size().unwrap();
        let sock2_recv_buf = sock2_ref.recv_buffer_size().unwrap();

        assert!(
            sock1_send_buf >= MIN_UDP_BUF_SIZE,
            "sock1 send buffer size is too small: {sock1_send_buf}"
        );
        assert!(
            sock1_recv_buf >= MIN_UDP_BUF_SIZE,
            "sock1 receive buffer size is too small: {sock1_recv_buf}"
        );
        assert!(
            sock2_send_buf >= MIN_UDP_BUF_SIZE,
            "sock2 send buffer size is too small: {sock2_send_buf}"
        );
        assert!(
            sock2_recv_buf >= MIN_UDP_BUF_SIZE,
            "sock2 receive buffer size is too small: {sock2_recv_buf}"
        );
    }
}
