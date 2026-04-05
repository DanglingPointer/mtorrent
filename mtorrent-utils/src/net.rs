use bytes::Buf;
use socket2::SockRef;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};
use std::{io, ops};

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

/// Get the local IP address to bind to, based on the specified network interface (if any).
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

/// Get the local IP address to bind to, based on the specified network interface (if any).
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

#[doc(hidden)]
pub fn set_so_sndbuf_internal<'s>(socket: impl Into<SockRef<'s>>, value: usize, module: &str) {
    if let Err(e) = socket.into().set_send_buffer_size(value) {
        log::warn!(target: module, "Failed to set socket send buffer size: {e}");
    }
}

#[doc(hidden)]
pub fn set_so_rcvbuf_internal<'s>(socket: impl Into<SockRef<'s>>, value: usize, module: &str) {
    if let Err(e) = socket.into().set_recv_buffer_size(value) {
        log::warn!(target: module, "Failed to set socket receive buffer size: {e}");
    }
}

/// Set SO_SNDBUF on a socket.
#[macro_export]
macro_rules! set_so_sndbuf {
    ($sock:expr, $size:expr) => {{
        $crate::net::set_so_sndbuf_internal($sock, $size, std::module_path!());
    }};
}

/// Set SO_RCVBUF on a socket.
#[macro_export]
macro_rules! set_so_rcvbuf {
    ($sock:expr, $size:expr) => {{
        $crate::net::set_so_rcvbuf_internal($sock, $size, std::module_path!());
    }};
}

// ------------------------------------------------------------------------------------------------

/// Bind a socket to a specific network interface. Does nothing on Windows.
#[cfg(target_os = "windows")]
pub fn bind_to_interface<'s>(_socket: impl Into<SockRef<'s>>, _interface: &str) -> io::Result<()> {
    // not supported on Windows
    Ok(())
}

/// Bind a socket to a specific network interface.
#[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
pub fn bind_to_interface<'s>(socket: impl Into<SockRef<'s>>, interface: &str) -> io::Result<()> {
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
pub fn bind_to_interface<'s>(socket: impl Into<SockRef<'s>>, interface: &str) -> io::Result<()> {
    let socket = socket.into();

    let interface = std::ffi::CString::new(interface)?;
    let idx = unsafe { libc::if_nametoindex(interface.as_ptr()) };
    let Some(idx) = std::num::NonZeroU32::new(idx) else {
        // If the index is 0, check errno and return an I/O error.
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "error converting interface name to index",
        ));
    };

    let Ok(local_addr) = socket.local_addr() else {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "socket not bound"));
    };
    let Some(local_addr) = local_addr.as_socket() else {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "socket is not AF_INET"));
    };
    match local_addr {
        std::net::SocketAddr::V4(_) => socket.bind_device_by_index_v4(Some(idx))?,
        std::net::SocketAddr::V6(_) => socket.bind_device_by_index_v6(Some(idx))?,
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

    #[cfg(not(windows))]
    #[test]
    fn test_network_interfaces() {
        for i in 0..10 {
            let mut name_buf = [0u8; 128];
            let name = unsafe { libc::if_indextoname(i, name_buf.as_mut_ptr() as *mut i8) };
            if name.is_null() {
                continue;
            }
            let name = unsafe { std::ffi::CStr::from_ptr(name) }.to_string_lossy();
            println!("Interface {}: {}", i, name);

            let idx = unsafe { libc::if_nametoindex(name_buf.as_ptr() as *const i8) };
            assert_eq!(idx, i);
        }
    }

    #[test]
    fn test_get_bind_addr() {
        let iface = if cfg!(target_os = "windows") {
            "Loopback Pseudo-Interface 1"
        } else if cfg!(target_os = "macos") {
            "lo0"
        } else {
            "lo"
        };

        let addr = get_bind_addr_v4(Some(iface));
        assert_eq!(addr, Ipv4Addr::LOCALHOST);

        let addr = get_bind_addr_v6(Some(iface));
        assert!(addr.is_loopback() || addr.is_unicast_link_local());
    }

    #[test]
    fn test_local_addr_predicate() {
        let addr = get_local_addr(|addr| addr.is_loopback() && addr.is_ipv4());
        assert_eq!(addr, Some(IpAddr::V4(Ipv4Addr::LOCALHOST)));

        let addr = get_local_addr(|addr| !addr.is_loopback() && !addr.is_unspecified());
        let addr = addr.expect("failed to find non-loopback address");
        assert!(!addr.is_loopback());
        assert!(!addr.is_unspecified());
    }
}
