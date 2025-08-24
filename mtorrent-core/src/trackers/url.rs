use std::str::FromStr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum UrlParseError {
    #[error("invalid URL: {0}")]
    NotAUrl(#[from] url::ParseError),
    #[error("invalid UDP URL: {0}")]
    InvalidUdpUrl(&'static str),
    #[error("unsupported scheme: {0}")]
    UnsupportedScheme(String),
}

#[derive(Debug, PartialEq, Eq, Clone)]
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

impl From<TrackerUrl> for String {
    fn from(url: TrackerUrl) -> Self {
        match url {
            TrackerUrl::Http(url) => url,
            TrackerUrl::Udp(addr) => format!("udp://{addr}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

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
}
