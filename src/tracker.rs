use crate::benc;
use crate::benc::Element;
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str;

pub struct TrackerResponse {
    root: BTreeMap<String, benc::Element>,
}

impl TryFrom<benc::Element> for TrackerResponse {
    type Error = ();

    fn try_from(e: Element) -> Result<Self, Self::Error> {
        match e {
            benc::Element::Dictionary(dict) => Ok(TrackerResponse {
                root: benc::convert_dictionary(dict),
            }),
            _ => Err(()),
        }
    }
}

impl TrackerResponse {
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
            Some(benc::Element::ByteString(data)) => Some(binary_peers(data).collect()),
            _ => None,
        }
    }
}

fn dictionary_peers(data: &Vec<benc::Element>) -> impl Iterator<Item = SocketAddr> + '_ {
    fn to_addr_and_port(dict: &BTreeMap<benc::Element, benc::Element>) -> Result<SocketAddr, ()> {
        let ip = dict.get(&benc::Element::from("ip")).ok_or(())?;
        let port = dict.get(&benc::Element::from("port")).ok_or(())?;
        match (ip, port) {
            (benc::Element::ByteString(ip), benc::Element::Integer(port)) => Ok(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::from(u32::from_be_bytes(
                    ip.as_slice().try_into().map_err(|_| ())?,
                ))),
                u16::try_from(*port).map_err(|_| ())?,
            )),
            _ => Err(()),
        }
    }
    data.iter()
        .filter_map(|e: &benc::Element| -> Option<SocketAddr> {
            match e {
                benc::Element::Dictionary(dict) => to_addr_and_port(dict).ok(),
                _ => None,
            }
        })
}

fn binary_peers(data: &[u8]) -> impl Iterator<Item = SocketAddr> + '_ {
    fn to_addr_and_port(src: &[u8]) -> Result<SocketAddr, ()> {
        let (addr_data, port_data) = src.split_at(4);
        Ok(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::from(u32::from_be_bytes(
                addr_data.try_into().map_err(|_| ())?,
            ))),
            u16::from_be_bytes(port_data.try_into().map_err(|_| ())?),
        ))
    }
    data.chunks_exact(6)
        .filter_map(|data| to_addr_and_port(data).ok())
}
