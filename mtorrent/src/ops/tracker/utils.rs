use mtorrent_utils::ip;
use mtorrent_utils::metainfo::Metainfo;
use std::iter;
use std::net::SocketAddr;
use std::str::FromStr;
use thiserror::Error;

pub(super) fn parse_binary_ipv4_peers(data: &[u8]) -> impl Iterator<Item = SocketAddr> + '_ {
    ip::SocketAddrV4BytesIter(data).map(SocketAddr::V4)
}

pub(super) fn parse_binary_ipv6_peers(data: &[u8]) -> impl Iterator<Item = SocketAddr> + '_ {
    ip::SocketAddrV6BytesIter(data).map(SocketAddr::V6)
}

pub(super) fn trackers_from_metainfo(metainfo: &Metainfo) -> Box<dyn Iterator<Item = &str> + '_> {
    if let Some(announce_list) = metainfo.announce_list() {
        Box::new(announce_list.flatten())
    } else if let Some(url) = metainfo.announce() {
        Box::new(iter::once(url))
    } else {
        Box::new(iter::empty())
    }
}

#[derive(Error, Debug)]
pub enum UrlParseError {
    #[error("invalid URL: {0}")]
    NotAUrl(#[from] url::ParseError),
    #[error("invalid UDP URL: {0}")]
    InvalidUdpUrl(&'static str),
    #[error("unsupported scheme: {0}")]
    UnsupportedScheme(String),
}

pub enum TrackerUrl {
    Http(String),
    Udp(String),
}

impl FromStr for TrackerUrl {
    type Err = UrlParseError;

    fn from_str(url: &str) -> Result<Self, Self::Err> {
        let parsed_url = url::Url::parse(url)?;
        match parsed_url.scheme() {
            "http" | "https" => Ok(TrackerUrl::Http(url.to_owned())),
            "udp" => Ok(TrackerUrl::Udp(format!(
                "{}:{}",
                parsed_url.host_str().ok_or(UrlParseError::InvalidUdpUrl("no host"))?,
                parsed_url.port().ok_or(UrlParseError::InvalidUdpUrl("no port"))?
            ))),
            scheme => Err(UrlParseError::UnsupportedScheme(scheme.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mtorrent_utils::metainfo;
    use std::{collections::HashSet, fs};

    #[test]
    fn test_extract_http_and_udp_trackers() {
        let trackers = [
            "udp://open.stealth.si:80/announce",
            "udp://tracker.opentrackr.org:1337/announce",
            "udp://tracker.tiny-vps.com:6969",
            "https://example.com",
            "udp://tracker.internetwarriors.net:1337/",
            "udp://tracker.skyts.net:6969/announce330",
            "http://example.com",
            "udp://[2001:db8:85a3:8d3:1319:8a2e:370:7348]:6969/",
        ];
        let expected_udp_trackers = [
            "open.stealth.si:80",
            "tracker.opentrackr.org:1337",
            "tracker.tiny-vps.com:6969",
            "tracker.internetwarriors.net:1337",
            "tracker.skyts.net:6969",
            "[2001:db8:85a3:8d3:1319:8a2e:370:7348]:6969",
        ];
        let expected_http_trackers = ["https://example.com", "http://example.com"];

        let mut udp_trackers = HashSet::new();
        let mut http_trackers = HashSet::new();
        for tracker in trackers {
            match tracker.parse::<TrackerUrl>().unwrap() {
                TrackerUrl::Http(addr) => http_trackers.insert(addr),
                TrackerUrl::Udp(addr) => udp_trackers.insert(addr),
            };
        }

        assert_eq!(http_trackers, expected_http_trackers.into_iter().map(Into::into).collect());
        assert_eq!(udp_trackers, expected_udp_trackers.into_iter().map(Into::into).collect());
    }

    #[test]
    fn test_extract_trackers_from_metainfo() {
        fn get_udp_tracker_addr(tracker: &str) -> Option<String> {
            match tracker.parse::<TrackerUrl>() {
                Ok(TrackerUrl::Udp(url)) => Some(url),
                _ => None,
            }
        }

        fn get_http_tracker_addr(tracker: &str) -> Option<String> {
            match tracker.parse::<TrackerUrl>() {
                Ok(TrackerUrl::Http(url)) => Some(url),
                _ => None,
            }
        }

        let data = fs::read("tests/assets/example.torrent").unwrap();
        let info = metainfo::Metainfo::new(&data).unwrap();

        let mut http_iter = trackers_from_metainfo(&info).filter_map(get_http_tracker_addr);
        assert_eq!("http://tracker.trackerfix.com:80/announce", http_iter.next().unwrap());
        assert!(http_iter.next().is_none());
        let udp_trackers = trackers_from_metainfo(&info)
            .filter_map(get_udp_tracker_addr)
            .collect::<HashSet<_>>();
        assert_eq!(4, udp_trackers.len());
        assert!(udp_trackers.contains("9.rarbg.me:2720"));
        assert!(udp_trackers.contains("9.rarbg.to:2740"));
        assert!(udp_trackers.contains("tracker.fatkhoala.org:13780"));
        assert!(udp_trackers.contains("tracker.tallpenguin.org:15760"));

        let data = fs::read("tests/assets/pcap.torrent").unwrap();
        let info = metainfo::Metainfo::new(&data).unwrap();

        let mut http_iter = trackers_from_metainfo(&info).filter_map(get_http_tracker_addr);
        assert_eq!("http://localhost:8000/announce", http_iter.next().unwrap());
        assert!(http_iter.next().is_none());
    }
}
