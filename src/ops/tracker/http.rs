use super::utils;
use crate::utils::benc;
use local_async_utils::prelude::*;
use reqwest::Url;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::{fmt, io, str};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("[http]{0}")]
    Http(#[from] reqwest::Error),
    #[error("[benc]{0}")]
    Benc(#[from] benc::ParseError),
    #[error("[response]{0}")]
    Response(String),
    #[error("unsupported")]
    Unsupported,
}

impl From<Error> for io::Error {
    fn from(e: Error) -> Self {
        io::Error::other(format!("{e}"))
    }
}

const APP_USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

#[derive(Clone)]
pub struct Client(reqwest::Client);

impl Client {
    pub fn new() -> Result<Self, Error> {
        let inner = reqwest::Client::builder()
            .gzip(true)
            .user_agent(APP_USER_AGENT)
            .timeout(sec!(30))
            .build()?;
        Ok(Client(inner))
    }

    pub async fn announce(
        &self,
        request_builder: TrackerRequestBuilder,
    ) -> Result<AnnounceResponseContent, Error> {
        let announce_url = request_builder.build_announce();
        log::debug!("Sending announce request to {announce_url}");

        let response_data =
            self.0.get(announce_url).send().await?.error_for_status()?.bytes().await?;
        let bencoded = benc::Element::from_bytes(&response_data)?;
        log::debug!("Received announce response: {bencoded}");

        let content = AnnounceResponseContent::from_benc(bencoded)
            .ok_or(Error::Benc(benc::ParseError::ExternalError("Unexpected bencoding".into())))?;

        match content.failure_reason() {
            Some(reason) => Err(Error::Response(reason.to_string())),
            None => Ok(content),
        }
    }

    #[cfg_attr(not(test), expect(dead_code))]
    pub async fn scrape(
        &self,
        request_builder: TrackerRequestBuilder,
    ) -> Result<benc::Element, Error> {
        let scrape_url = request_builder.build_scrape().ok_or(Error::Unsupported)?;
        log::debug!("Sending scrape request to {scrape_url}");

        let response_data =
            self.0.get(scrape_url).send().await?.error_for_status()?.bytes().await?;
        let bencoded = benc::Element::from_bytes(&response_data)?;
        log::debug!("Scrape response: {bencoded}");

        // TODO: parse response
        Ok(bencoded)
    }
}

pub struct TrackerRequestBuilder {
    base_url: Url,
    query: String,
}

impl TryFrom<&str> for TrackerRequestBuilder {
    type Error = url::ParseError;

    fn try_from(announce_url: &str) -> Result<Self, Self::Error> {
        Ok(TrackerRequestBuilder {
            base_url: Url::parse(announce_url)?,
            query: String::with_capacity(128),
        })
    }
}

#[derive(Clone, Copy)]
pub enum AnnounceEvent {
    Started,
    Stopped,
    Completed,
}

impl TrackerRequestBuilder {
    pub fn info_hash(&mut self, data: &[u8]) -> &mut Self {
        self.append_bytes("info_hash", data)
    }

    pub fn peer_id(&mut self, data: &[u8]) -> &mut Self {
        self.append_bytes("peer_id", data)
    }

    pub fn port(&mut self, port: u16) -> &mut Self {
        self.append_tostring("port", port)
    }

    pub fn bytes_uploaded(&mut self, count: usize) -> &mut Self {
        self.append_tostring("uploaded", count)
    }

    pub fn bytes_downloaded(&mut self, count: usize) -> &mut Self {
        self.append_tostring("downloaded", count)
    }

    pub fn bytes_left(&mut self, count: usize) -> &mut Self {
        self.append_tostring("left", count)
    }

    pub fn event(&mut self, event: AnnounceEvent) -> &mut Self {
        let value = match event {
            AnnounceEvent::Started => "started",
            AnnounceEvent::Stopped => "stopped",
            AnnounceEvent::Completed => "completed",
        };
        self.append_tostring("event", value)
    }

    pub fn numwant(&mut self, num_want: usize) -> &mut Self {
        self.append_tostring("numwant", num_want)
    }

    pub fn compact_support(&mut self) -> &mut Self {
        self.query.push_str("&compact=1");
        self
    }

    pub fn no_peer_id(&mut self) -> &mut Self {
        self.query.push_str("&no_peer_id=1");
        self
    }

    fn build_announce(mut self) -> Url {
        if let Some(substr) = self.query.get(1..) {
            self.base_url.set_query(Some(substr));
        }
        self.base_url
    }

    fn build_scrape(mut self) -> Option<Url> {
        if self.base_url.path() != "/announce" {
            None
        } else {
            self.base_url.set_path("scrape");
            if let Some(substr) = self.query.get(1..) {
                self.base_url.set_query(Some(substr));
            }
            Some(self.base_url)
        }
    }

    fn append_bytes(&mut self, name: &str, data: &[u8]) -> &mut Self {
        let value = form_urlencoded::byte_serialize(data).collect::<String>();
        self.query.push('&');
        self.query.push_str(name);
        self.query.push('=');
        self.query.push_str(value.as_str());
        self
    }

    fn append_tostring<T: ToString>(&mut self, name: &str, value: T) -> &mut Self {
        let mut encoder = form_urlencoded::Serializer::new(String::with_capacity(64));
        encoder.append_pair(name, value.to_string().as_str());
        self.query.push('&');
        self.query.push_str(encoder.finish().as_str());
        self
    }
}

// -------------------------------------------------------------------------------------------------

pub struct AnnounceResponseContent {
    root: BTreeMap<String, benc::Element>,
}

impl AnnounceResponseContent {
    pub fn from_benc(e: benc::Element) -> Option<Self> {
        match e {
            benc::Element::Dictionary(dict) => Some(AnnounceResponseContent {
                root: benc::convert_dictionary(dict),
            }),
            _ => None,
        }
    }

    fn failure_reason(&self) -> Option<&str> {
        if let Some(benc::Element::ByteString(data)) = self.root.get("failure reason") {
            str::from_utf8(data).ok()
        } else {
            None
        }
    }

    pub fn warning_message(&self) -> Option<&str> {
        if let Some(benc::Element::ByteString(data)) = self.root.get("warning message") {
            str::from_utf8(data).ok()
        } else {
            None
        }
    }

    pub fn interval(&self) -> Option<usize> {
        if let Some(benc::Element::Integer(data)) = self.root.get("interval") {
            usize::try_from(*data).ok()
        } else {
            None
        }
    }

    pub fn tracker_id(&self) -> Option<&str> {
        if let Some(benc::Element::ByteString(data)) = self.root.get("tracker id") {
            str::from_utf8(data).ok()
        } else {
            None
        }
    }

    pub fn complete(&self) -> Option<usize> {
        if let Some(benc::Element::Integer(data)) = self.root.get("complete") {
            usize::try_from(*data).ok()
        } else {
            None
        }
    }

    pub fn incomplete(&self) -> Option<usize> {
        if let Some(benc::Element::Integer(data)) = self.root.get("incomplete") {
            usize::try_from(*data).ok()
        } else {
            None
        }
    }

    pub fn peers(&self) -> Option<Vec<SocketAddr>> {
        match (self.root.get("peers"), self.root.get("peers6")) {
            (None, None) => None,
            (peers, ipv6_peers) => {
                let mut all_peers = Vec::new();
                match peers {
                    Some(benc::Element::ByteString(data)) => {
                        all_peers.extend(utils::parse_binary_ipv4_peers(data));
                    }
                    Some(benc::Element::List(list)) => {
                        all_peers.extend(dictionary_peers(list));
                    }
                    _ => (),
                }
                if let Some(benc::Element::ByteString(data)) = ipv6_peers {
                    all_peers.extend(utils::parse_binary_ipv6_peers(data))
                }
                Some(all_peers)
            }
        }
    }
}

impl fmt::Display for AnnounceResponseContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(warning) = self.warning_message() {
            write!(f, "warning={warning} ")?;
        }
        if let Some(tracker_id) = self.tracker_id() {
            write!(f, "tracker_id={tracker_id} ")?;
        }
        if let Some(interval) = self.interval() {
            write!(f, "interval={interval} ")?;
        }
        if let Some(complete) = self.complete() {
            write!(f, "complete={complete} ")?;
        }
        if let Some(incomplete) = self.incomplete() {
            write!(f, "incomplete={incomplete} ")?;
        }
        if let Some(peers) = self.peers() {
            write!(f, "peers={peers:?}")?;
        }
        Ok(())
    }
}

fn dictionary_peers(data: &[benc::Element]) -> impl Iterator<Item = SocketAddr> + '_ {
    fn to_addr_and_port(dict: &BTreeMap<benc::Element, benc::Element>) -> Option<SocketAddr> {
        let ip = dict.get(&benc::Element::from("ip"))?;
        let port = dict.get(&benc::Element::from("port"))?;
        match (ip, port) {
            (benc::Element::ByteString(ip), benc::Element::Integer(port)) => Some(SocketAddr::new(
                str::from_utf8(ip).ok()?.parse().ok()?,
                u16::try_from(*port).ok()?,
            )),
            _ => None,
        }
    }
    data.iter().filter_map(|e: &benc::Element| -> Option<SocketAddr> {
        match e {
            benc::Element::Dictionary(dict) => to_addr_and_port(dict),
            _ => None,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_announce_uri() {
        let hash =
            b"\x12\x34\x56\x78\x9a\xbc\xde\xf1\x23\x45\x67\x89\xab\xcd\xef\x12\x34\x56\x78\x9a";
        let url_base = "http://example.com/announce";

        let mut builder = TrackerRequestBuilder::try_from(url_base).unwrap();
        builder
            .info_hash(hash)
            .bytes_left(42)
            .bytes_uploaded(3)
            .no_peer_id()
            .numwant(50);

        let uri = builder.build_announce();

        assert_eq!(
            "http://example.com/announce?info_hash=%124Vx%9A%BC%DE%F1%23Eg%89%AB%CD%EF%124Vx%9A&left=42&uploaded=3&no_peer_id=1&numwant=50",
            uri.as_str()
        );
    }

    #[test]
    fn test_scrape_uri() {
        let hash =
            b"\x12\x34\x56\x78\x9a\xbc\xde\xf1\x23\x45\x67\x89\xab\xcd\xef\x12\x34\x56\x78\x9a";
        let url_base = "http://example.com/announce";

        let mut builder = TrackerRequestBuilder::try_from(url_base).unwrap();
        builder.info_hash(hash);
        let uri = builder.build_scrape();

        assert_eq!(
            "http://example.com/scrape?info_hash=%124Vx%9A%BC%DE%F1%23Eg%89%AB%CD%EF%124Vx%9A",
            uri.unwrap().as_str()
        );
    }
    #[test]
    fn test_unsupported_scrape_uri() {
        let url_base = "http://example.com";

        let builder = TrackerRequestBuilder::try_from(url_base).unwrap();
        let uri = builder.build_scrape();

        assert!(uri.is_none());
    }

    #[test]
    fn test_parse_ipv4_and_ipv6_in_announce_response() {
        let response_data = "d8:completei146e10:incompletei4e8:intervali1800e5:peersld2:ip14:185.125.190.597:peer id20:T03I--00RleC9iXCylpi4:porti6902eed2:ip36:2a01:e0a:352:2450:211:32ff:fed8:cacb7:peer id20:-TR2930-r6di5h9fx1t74:porti63810eed2:ip39:2600:1700:dc40:2830:c423:6cff:fe78:e2ea7:peer id20:-TR3000-j0qob7o6v6xt4:porti51413eed2:ip36:2001:9e8:f123:700:211:32ff:fe97:ebfe7:peer id20:-TR2930-3118vqmbf7b84:porti16881eeee";

        let entity = benc::Element::from_bytes(response_data.as_bytes()).unwrap();
        let response_content = AnnounceResponseContent::from_benc(entity)
            .ok_or(Error::Benc(benc::ParseError::ExternalError("Unexpected bencoding".into())))
            .unwrap();

        let peers = response_content.peers().unwrap();
        assert_eq!(4, peers.len());
        assert_eq!(1, peers.iter().filter(|addr| addr.is_ipv4()).count());
        assert_eq!(3, peers.iter().filter(|addr| addr.is_ipv6()).count());
    }

    #[test]
    fn test_parse_compact_ipv4_and_ipv6_in_announce_response() {
        let response_data = "d8:intervali1800e5:peers6:addrpn6:peers618:addraddraddraddrpne";

        let entity = benc::Element::from_bytes(response_data.as_bytes()).unwrap();
        let response_content = AnnounceResponseContent::from_benc(entity)
            .ok_or(Error::Benc(benc::ParseError::ExternalError("Unexpected bencoding".into())))
            .unwrap();

        let peers = response_content.peers().unwrap();
        assert_eq!(2, peers.len());

        let ipv4 = *peers.iter().find(|addr| addr.is_ipv4()).expect("no ipv4 peer");
        assert_eq!(ipv4, "97.100.100.114:28782".parse().unwrap());

        let ipv6 = *peers.iter().find(|addr| addr.is_ipv6()).expect("no ipv6 peer");
        assert_eq!(ipv6, "[6164:6472:6164:6472:6164:6472:6164:6472]:28782".parse().unwrap());
    }
}
