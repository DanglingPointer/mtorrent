use core::fmt;
use std::error::Error;
use std::net::SocketAddr;
use std::{iter, net, str};

pub struct MagnetLink {
    info_hash: [u8; 20],
    name: Option<String>,
    trackers: Vec<String>,
    peers: Vec<SocketAddr>,
}

#[derive(Debug)]
pub enum ParseError {
    MalformedUri(url::ParseError),
    UnsupportedScheme(String),
    NoInfoHash,
    InvalidInfoHash(Box<dyn Error>),
    InvalidPeerAddr(net::AddrParseError),
}

impl From<url::ParseError> for ParseError {
    fn from(e: url::ParseError) -> Self {
        ParseError::MalformedUri(e)
    }
}

impl From<net::AddrParseError> for ParseError {
    fn from(e: net::AddrParseError) -> Self {
        ParseError::InvalidPeerAddr(e)
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::MalformedUri(e) => write!(f, "malformed uri: {e}"),
            ParseError::UnsupportedScheme(s) => write!(f, "invalid scheme: {s}"),
            ParseError::NoInfoHash => write!(f, "no info hash"),
            ParseError::InvalidInfoHash(e) => write!(f, "invalid info hash: {e}"),
            ParseError::InvalidPeerAddr(e) => write!(f, "invalid peer addr: {e}"),
        }
    }
}

impl Error for ParseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ParseError::MalformedUri(e) => Some(e),
            ParseError::InvalidInfoHash(e) => Some(e.as_ref()),
            ParseError::InvalidPeerAddr(e) => Some(e),
            _ => None,
        }
    }
}

impl str::FromStr for MagnetLink {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parsed = url::Url::parse(s)?;
        if parsed.scheme() != "magnet" {
            return Err(ParseError::UnsupportedScheme(parsed.scheme().to_string()));
        }
        let mut info_hash = None;
        let mut name = None;
        let mut trackers = Vec::<String>::new();
        let mut peers = Vec::<SocketAddr>::new();
        for (key, value) in parsed.query_pairs() {
            match key.as_ref() {
                "xt" => match value.strip_prefix("urn:btih:") {
                    Some(hex_str) if hex_str.len() == 40 => {
                        let mut bytes = [0u8; 20];
                        for (src, dest) in
                            iter::zip(hex_str.as_bytes().chunks_exact(2), bytes.iter_mut())
                        {
                            let src_str = str::from_utf8(src)
                                .map_err(|e| ParseError::InvalidInfoHash(Box::new(e)))?;
                            *dest = u8::from_str_radix(src_str, 16)
                                .map_err(|e| ParseError::InvalidInfoHash(Box::new(e)))?;
                        }
                        info_hash = Some(bytes);
                    }
                    _ => return Err(ParseError::InvalidInfoHash(format!("{value}").into())),
                },
                "dn" => name = Some(value.to_string()),
                "tr" => trackers.push(value.to_string()),
                "x.pe" => peers.push(value.parse()?),
                _ => (),
            }
        }
        Ok(Self {
            info_hash: info_hash.ok_or(ParseError::NoInfoHash)?,
            name,
            trackers,
            peers,
        })
    }
}

impl MagnetLink {
    pub fn info_hash(&self) -> &[u8; 20] {
        &self.info_hash
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn trackers(&self) -> impl ExactSizeIterator<Item = &str> {
        self.trackers.iter().map(AsRef::as_ref)
    }

    pub fn peers(&self) -> impl ExactSizeIterator<Item = &SocketAddr> {
        self.peers.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_magnet_link() {
        let link = "magnet:?xt=urn:btih:1EBD3DBFBB25C1333F51C99C7EE670FC2A1727C9&dn=Dune.Part.Two.2024.1080p.HD-TS.X264-EMIN3M%5BTGx%5D&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.tiny-vps.com%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Fexplodie.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.cyberia.is%3A6969%2Fannounce&tr=udp%3A%2F%2Fipv4.tracker.harry.lu%3A80%2Fannounce&tr=udp%3A%2F%2Fp4p.arenabg.com%3A1337%2Fannounce&tr=udp%3A%2F%2Ftracker.birkenwald.de%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.moeking.me%3A6969%2Fannounce&tr=udp%3A%2F%2Fopentor.org%3A2710%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fuploads.gamecoast.net%3A6969%2Fannounce&tr=https%3A%2F%2Ftracker.foreverpirates.co%3A443%2Fannounce&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce&tr=http%3A%2F%2Ftracker.openbittorrent.com%3A80%2Fannounce&tr=udp%3A%2F%2Fopentracker.i2p.rocks%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.internetwarriors.net%3A1337%2Fannounce&tr=udp%3A%2F%2Ftracker.leechers-paradise.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fcoppersurfer.tk%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.zer0day.to%3A1337%2Fannounce";

        let magnet = link.parse::<MagnetLink>().unwrap();

        assert_eq!(
            &[
                30, 189, 61, 191, 187, 37, 193, 51, 63, 81, 201, 156, 126, 230, 112, 252, 42, 23,
                39, 201
            ],
            magnet.info_hash()
        );
        assert_eq!(Some("Dune.Part.Two.2024.1080p.HD-TS.X264-EMIN3M[TGx]"), magnet.name());
        assert_eq!(
            vec![
                "udp://open.stealth.si:80/announce",
                "udp://tracker.tiny-vps.com:6969/announce",
                "udp://tracker.opentrackr.org:1337/announce",
                "udp://tracker.torrent.eu.org:451/announce",
                "udp://explodie.org:6969/announce",
                "udp://tracker.cyberia.is:6969/announce",
                "udp://ipv4.tracker.harry.lu:80/announce",
                "udp://p4p.arenabg.com:1337/announce",
                "udp://tracker.birkenwald.de:6969/announce",
                "udp://tracker.moeking.me:6969/announce",
                "udp://opentor.org:2710/announce",
                "udp://tracker.dler.org:6969/announce",
                "udp://uploads.gamecoast.net:6969/announce",
                "https://tracker.foreverpirates.co:443/announce",
                "udp://tracker.opentrackr.org:1337/announce",
                "http://tracker.openbittorrent.com:80/announce",
                "udp://opentracker.i2p.rocks:6969/announce",
                "udp://tracker.internetwarriors.net:1337/announce",
                "udp://tracker.leechers-paradise.org:6969/announce",
                "udp://coppersurfer.tk:6969/announce",
                "udp://tracker.zer0day.to:1337/announce"
            ],
            magnet.trackers().collect::<Vec<_>>()
        );
    }

    #[test]
    fn parse_magnet_link_with_invalid_info_hash() {
        let link = "magnet:?xt=urn:btih:1EBD3DBFBB25C1333F51C99C7EE670FC2A1727C"; // missing last character
        assert!(matches!(link.parse::<MagnetLink>(), Err(ParseError::InvalidInfoHash(_))));

        let link = "magnet:?xt=urn:btih:1EBD3DBFBB25C1333F51C99C7EE670FC2A1727C99"; // too many characters
        assert!(matches!(link.parse::<MagnetLink>(), Err(ParseError::InvalidInfoHash(_))));
    }
}
