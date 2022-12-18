use crate::tracker::utils;
use crate::utils::benc;
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str;
use url::{form_urlencoded, Url};

pub struct TrackerRequestBuilder {
    uri: Url,
    query: String,
}

impl TryFrom<&str> for TrackerRequestBuilder {
    type Error = url::ParseError;

    fn try_from(announce_url: &str) -> Result<Self, Self::Error> {
        Ok(TrackerRequestBuilder {
            uri: Url::parse(announce_url)?,
            query: String::with_capacity(128),
        })
    }
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

    pub fn bytes_left(&mut self, count: usize) -> &mut Self {
        self.append_tostring("left", count)
    }

    pub fn compact_support(&mut self) -> &mut Self {
        self.query.push_str("&compact=1");
        self
    }

    pub fn no_peer_id(&mut self) -> &mut Self {
        self.query.push_str("&no_peer_id=1");
        self
    }

    pub fn build_announce(mut self) -> Url {
        if let Some(substr) = self.query.get(1..) {
            self.uri.set_query(Some(substr));
        }
        self.uri
    }

    pub fn build_scrape(mut self) -> Option<Url> {
        if self.uri.path() != "/announce" {
            None
        } else {
            self.uri.set_path("/scrape");
            Some(self.build_announce())
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

pub struct TrackerResponseContent {
    root: BTreeMap<String, benc::Element>,
}

impl TryFrom<benc::Element> for TrackerResponseContent {
    type Error = ();

    fn try_from(e: benc::Element) -> Result<Self, Self::Error> {
        match e {
            benc::Element::Dictionary(dict) => Ok(TrackerResponseContent {
                root: benc::convert_dictionary(dict),
            }),
            _ => Err(()),
        }
    }
}

impl TrackerResponseContent {
    pub fn failure_reason(&self) -> Option<&str> {
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
        match self.root.get("peers") {
            Some(benc::Element::List(list)) => Some(dictionary_peers(list).collect()),
            Some(benc::Element::ByteString(data)) => {
                Some(utils::parse_binary_ipv4_peers(data).collect())
            }
            _ => None,
        }
    }
}

fn dictionary_peers(data: &[benc::Element]) -> impl Iterator<Item = SocketAddr> + '_ {
    fn to_addr_and_port(dict: &BTreeMap<benc::Element, benc::Element>) -> Option<SocketAddr> {
        let ip = dict.get(&benc::Element::from("ip"))?;
        let port = dict.get(&benc::Element::from("port"))?;
        match (ip, port) {
            (benc::Element::ByteString(ip), benc::Element::Integer(port)) => Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::from(u32::from_be_bytes(ip.as_slice().try_into().ok()?))),
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
        builder.info_hash(hash).bytes_left(42).bytes_uploaded(3).no_peer_id();

        let uri = builder.build_announce();

        assert_eq!(
            "http://example.com/announce?info_hash=%124Vx%9A%BC%DE%F1%23Eg%89%AB%CD%EF%124Vx%9A&left=42&uploaded=3&no_peer_id=1",
            uri.as_str());
    }

    #[test]
    fn test_scrape_uri() {
        let hash =
            b"\x12\x34\x56\x78\x9a\xbc\xde\xf1\x23\x45\x67\x89\xab\xcd\xef\x12\x34\x56\x78\x9a";
        let url_base = "http://example.com/announce";

        let mut builder = TrackerRequestBuilder::try_from(url_base).unwrap();
        builder.info_hash(hash);
        let uri = builder.build_scrape().unwrap();

        assert_eq!(
            "http://example.com/scrape?info_hash=%124Vx%9A%BC%DE%F1%23Eg%89%AB%CD%EF%124Vx%9A",
            uri.as_str()
        );
    }
}
