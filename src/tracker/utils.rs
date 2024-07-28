use crate::utils::metainfo::Metainfo;
use std::iter;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

pub(super) fn parse_binary_ipv4_peers(data: &[u8]) -> impl Iterator<Item = SocketAddr> + '_ {
    fn to_addr_and_port(src: &[u8]) -> Option<SocketAddr> {
        let addr_data = src.first_chunk::<4>()?;
        let port_data = src.last_chunk::<2>()?;
        Some(SocketAddr::new(Ipv4Addr::from(*addr_data).into(), u16::from_be_bytes(*port_data)))
    }
    data.chunks_exact(6).filter_map(to_addr_and_port)
}

pub(super) fn parse_binary_ipv6_peers(data: &[u8]) -> impl Iterator<Item = SocketAddr> + '_ {
    fn to_addr_and_port(src: &[u8]) -> Option<SocketAddr> {
        let addr_data = src.first_chunk::<16>()?;
        let port_data = src.last_chunk::<2>()?;
        Some(SocketAddr::new(Ipv6Addr::from(*addr_data).into(), u16::from_be_bytes(*port_data)))
    }
    data.chunks_exact(18).filter_map(to_addr_and_port)
}

pub fn trackers_from_metainfo(metainfo: &Metainfo) -> Box<dyn Iterator<Item = &str> + '_> {
    if let Some(announce_list) = metainfo.announce_list() {
        Box::new(announce_list.flatten())
    } else if let Some(url) = metainfo.announce() {
        Box::new(iter::once(url))
    } else {
        Box::new(iter::empty())
    }
}

pub fn get_udp_tracker_addr<'t, T: AsRef<str> + 't>(tracker: &'t T) -> Option<&'t str> {
    let tracker = tracker.as_ref();
    let udp_addr = tracker.strip_prefix("udp://")?;
    Some(udp_addr.strip_suffix("/announce").unwrap_or(udp_addr))
}

pub fn get_http_tracker_addr<'t, T: AsRef<str> + 't>(tracker: &'t T) -> Option<&'t str> {
    let tracker = tracker.as_ref();
    (tracker.starts_with("http://") || tracker.starts_with("https://")).then_some(tracker)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_extract_udp_trackers() {
        let trackers = [
            "udp://open.stealth.si:80/announce",
            "udp://tracker.opentrackr.org:1337/announce",
            "udp://tracker.tiny-vps.com:6969",
            "https://example.com",
            "udp://tracker.internetwarriors.net:1337/announce",
            "udp://tracker.skyts.net:6969/announce",
            "http://example.com",
        ];
        let expected = [
            "open.stealth.si:80",
            "tracker.opentrackr.org:1337",
            "tracker.tiny-vps.com:6969",
            "tracker.internetwarriors.net:1337",
            "tracker.skyts.net:6969",
        ];
        let actual: HashSet<_> = trackers
            .into_iter()
            .filter_map(|tracker| get_udp_tracker_addr(&tracker).map(ToString::to_string))
            .collect();
        assert_eq!(actual, expected.into_iter().map(Into::into).collect());
    }

    #[test]
    fn test_extract_http_trackers() {
        let trackers = [
            "udp://open.stealth.si:80/announce",
            "udp://tracker.opentrackr.org:1337/announce",
            "udp://tracker.tiny-vps.com:6969",
            "https://example.com",
            "udp://tracker.internetwarriors.net:1337/announce",
            "udp://tracker.skyts.net:6969/announce",
            "http://example.com",
        ];
        let expected = ["https://example.com", "http://example.com"];
        let actual: HashSet<_> = trackers
            .into_iter()
            .filter_map(|tracker| get_http_tracker_addr(&tracker).map(ToString::to_string))
            .collect();
        assert_eq!(actual, expected.into_iter().map(ToOwned::to_owned).collect());
    }
}
