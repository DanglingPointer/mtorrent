use bitvec::prelude::*;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::{fmt, io};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};

use crate::utils::benc;

pub type Bitfield = BitVec<u8, Msb0>;

#[derive(Debug)]
pub(super) enum PeerMessage {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have {
        piece_index: u32,
    },
    Bitfield {
        bitfield: Bitfield,
    },
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
    DhtPort {
        listen_port: u16,
    },
    Extended {
        id: u8,
        data: Vec<u8>,
    },
}

const MAX_MSG_LEN: usize = 1024 * 32 + 9;

impl PeerMessage {
    pub(super) async fn read_from<S: AsyncReadExt + Unpin>(src: &mut S) -> io::Result<PeerMessage> {
        use PeerMessage::*;

        let msg_len = read_u32_from(src).await? as usize;

        if msg_len == 0 {
            return Ok(KeepAlive);
        }

        if msg_len > MAX_MSG_LEN {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("Too long message received: {msg_len} bytes"),
            ));
        }

        let id = {
            let mut id_byte = [0u8; 1];
            src.read_exact(&mut id_byte).await?;
            id_byte[0]
        };

        match id {
            ID_CHOKE => Ok(Choke),
            ID_UNCHOKE => Ok(Unchoke),
            ID_INTERESTED => Ok(Interested),
            ID_NOT_INTERESTED => Ok(NotInterested),
            ID_HAVE => Ok(Have {
                piece_index: read_u32_from(src).await?,
            }),
            ID_BITFIELD => {
                let mut bitfield_bytes = vec![0u8; msg_len - 1];
                src.read_exact(&mut bitfield_bytes).await?;
                Ok(Bitfield {
                    bitfield: BitVec::from_vec(bitfield_bytes),
                })
            }
            ID_REQUEST => {
                let index = read_u32_from(src).await?;
                let begin = read_u32_from(src).await?;
                let length = read_u32_from(src).await?;
                Ok(Request {
                    index,
                    begin,
                    length,
                })
            }
            ID_PIECE => {
                let index = read_u32_from(src).await?;
                let begin = read_u32_from(src).await?;
                let mut block = vec![0u8; msg_len - 9];
                src.read_exact(&mut block).await?;
                Ok(Piece {
                    index,
                    begin,
                    block,
                })
            }
            ID_CANCEL => {
                let index = read_u32_from(src).await?;
                let begin = read_u32_from(src).await?;
                let length = read_u32_from(src).await?;
                Ok(Cancel {
                    index,
                    begin,
                    length,
                })
            }
            ID_PORT => {
                let mut port_bytes = [0u8; 2];
                src.read_exact(&mut port_bytes).await?;
                Ok(DhtPort {
                    listen_port: u16::from_be_bytes(port_bytes),
                })
            }
            ID_EXTENDED => {
                let id = src.read_u8().await?;
                let mut data = vec![0u8; msg_len - 2];
                src.read_exact(&mut data).await?;
                Ok(Extended { id, data })
            }
            _ => Err(io::Error::new(io::ErrorKind::Other, format!("Unknown message type: {}", id))),
        }
    }

    pub(super) async fn write_to<S: AsyncWriteExt + Unpin>(
        &self,
        dest: &mut BufWriter<S>,
    ) -> io::Result<()> {
        use PeerMessage::*;

        let length = (self.get_length() as u32).to_be_bytes();
        dest.write_all(&length).await?;

        match self {
            KeepAlive => (),
            Choke => {
                dest.write_all(&[ID_CHOKE]).await?;
            }
            Unchoke => {
                dest.write_all(&[ID_UNCHOKE]).await?;
            }
            Interested => {
                dest.write_all(&[ID_INTERESTED]).await?;
            }
            NotInterested => {
                dest.write_all(&[ID_NOT_INTERESTED]).await?;
            }
            Have { piece_index } => {
                dest.write_all(&[ID_HAVE]).await?;
                dest.write_all(&piece_index.to_be_bytes()).await?;
            }
            Bitfield { bitfield } => {
                dest.write_all(&[ID_BITFIELD]).await?;
                dest.write_all(bitfield.as_raw_slice()).await?;
            }
            Request {
                index,
                begin,
                length,
            } => {
                dest.write_all(&[ID_REQUEST]).await?;
                dest.write_all(&index.to_be_bytes()).await?;
                dest.write_all(&begin.to_be_bytes()).await?;
                dest.write_all(&length.to_be_bytes()).await?;
            }
            Piece {
                index,
                begin,
                block,
            } => {
                dest.write_all(&[ID_PIECE]).await?;
                dest.write_all(&index.to_be_bytes()).await?;
                dest.write_all(&begin.to_be_bytes()).await?;
                dest.write_all(block).await?;
            }
            Cancel {
                index,
                begin,
                length,
            } => {
                dest.write_all(&[ID_CANCEL]).await?;
                dest.write_all(&index.to_be_bytes()).await?;
                dest.write_all(&begin.to_be_bytes()).await?;
                dest.write_all(&length.to_be_bytes()).await?;
            }
            DhtPort { listen_port } => {
                dest.write_all(&[ID_PORT]).await?;
                dest.write_all(&listen_port.to_be_bytes()).await?;
            }
            Extended { id, data } => {
                dest.write_all(&[ID_EXTENDED]).await?;
                dest.write_u8(*id).await?;
                dest.write_all(data).await?;
            }
        };
        dest.flush().await?;
        Ok(())
    }

    fn get_length(&self) -> usize {
        use PeerMessage::*;

        match self {
            KeepAlive => 0,
            Choke | Unchoke | Interested | NotInterested => 1,
            Have { .. } => 5,
            Bitfield { bitfield } => 1 + bitfield.as_raw_slice().len(),
            Request { .. } | Cancel { .. } => 13,
            Piece { block, .. } => 9 + block.len(),
            DhtPort { .. } => 3,
            Extended { data, .. } => 2 + data.len(),
        }
    }
}

const ID_CHOKE: u8 = 0;
const ID_UNCHOKE: u8 = 1;
const ID_INTERESTED: u8 = 2;
const ID_NOT_INTERESTED: u8 = 3;
const ID_HAVE: u8 = 4;
const ID_BITFIELD: u8 = 5;
const ID_REQUEST: u8 = 6;
const ID_PIECE: u8 = 7;
const ID_CANCEL: u8 = 8;
const ID_PORT: u8 = 9;
const ID_EXTENDED: u8 = 20;

async fn read_u32_from<S: AsyncReadExt + Unpin>(src: &mut S) -> io::Result<u32> {
    let mut bytes = [0u8; 4];
    src.read_exact(&mut bytes).await?;
    Ok(u32::from_be_bytes(bytes))
}

// ------

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct BlockInfo {
    pub piece_index: usize,
    pub in_piece_offset: usize,
    pub block_length: usize,
}

#[cfg_attr(test, derive(Debug))]
#[derive(Eq, PartialEq)]
pub enum UploaderMessage {
    Choke,
    Unchoke,
    Have {
        piece_index: usize,
    },
    Bitfield(Bitfield),
    Block(BlockInfo, Vec<u8>),
}

#[derive(Eq, PartialEq)]
pub enum DownloaderMessage {
    Interested,
    NotInterested,
    Request(BlockInfo),
    Cancel(BlockInfo),
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Extension {
    Metadata,
    PeerExchange,
}

#[derive(Default, PartialEq, Eq)]
pub struct ExtendedHandshake {
    pub extensions: HashMap<Extension, u8>,
    pub listen_port: Option<u16>,
    pub client_type: Option<String>,
    pub yourip: Option<IpAddr>,
    pub ipv4: Option<Ipv4Addr>,
    pub ipv6: Option<Ipv6Addr>,
    pub request_limit: Option<usize>,
    pub metadata_size: Option<usize>,
}

#[derive(Default, PartialEq, Eq)]
pub struct PeerExchangeData {
    pub added: HashSet<SocketAddr>,
    pub dropped: HashSet<SocketAddr>,
}

#[derive(Eq, PartialEq)]
pub enum ExtendedMessage {
    Handshake(Box<ExtendedHandshake>),
    MetadataRequest {
        piece: usize,
    },
    MetadataBlock {
        piece: usize,
        total_size: usize,
        data: Vec<u8>,
    },
    MetadataReject {
        piece: usize,
    },
    PeerExchange(Box<PeerExchangeData>),
}

impl fmt::Display for BlockInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ind={} off={} len={}", self.piece_index, self.in_piece_offset, self.block_length)
    }
}

impl fmt::Display for ExtendedHandshake {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ext=[{:?}]", self.extensions)?;
        if let Some(port) = self.listen_port {
            write!(f, " port={port}")?;
        }
        if let Some(client_type) = self.client_type.as_ref() {
            write!(f, " client={client_type}")?;
        }
        if let Some(yourip) = self.yourip {
            write!(f, " yourip={yourip}")?;
        }
        if let Some(ipv4) = self.ipv4 {
            write!(f, " ipv4={ipv4}")?;
        }
        if let Some(ipv6) = self.ipv6 {
            write!(f, " ipv6={ipv6}")?;
        }
        if let Some(reqq) = self.request_limit {
            write!(f, " reqq={reqq}")?;
        }
        if let Some(metasize) = self.metadata_size {
            write!(f, " metasize={metasize}")?;
        }
        Ok(())
    }
}

impl fmt::Display for PeerExchangeData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "added={:?} dropped={:?}", self.added, self.dropped)
    }
}

// ------

impl From<UploaderMessage> for PeerMessage {
    fn from(msg: UploaderMessage) -> Self {
        match msg {
            UploaderMessage::Choke => PeerMessage::Choke,
            UploaderMessage::Unchoke => PeerMessage::Unchoke,
            UploaderMessage::Have { piece_index } => PeerMessage::Have {
                piece_index: piece_index as u32,
            },
            UploaderMessage::Bitfield(bitfield) => PeerMessage::Bitfield { bitfield },
            UploaderMessage::Block(info, data) => PeerMessage::Piece {
                index: info.piece_index as u32,
                begin: info.in_piece_offset as u32,
                block: data,
            },
        }
    }
}

impl TryFrom<PeerMessage> for UploaderMessage {
    type Error = PeerMessage;

    fn try_from(msg: PeerMessage) -> Result<Self, Self::Error> {
        match msg {
            PeerMessage::Choke => Ok(UploaderMessage::Choke),
            PeerMessage::Unchoke => Ok(UploaderMessage::Unchoke),
            PeerMessage::Have { piece_index } => Ok(UploaderMessage::Have {
                piece_index: piece_index as usize,
            }),
            PeerMessage::Bitfield { bitfield } => Ok(UploaderMessage::Bitfield(bitfield)),
            PeerMessage::Piece {
                index,
                begin,
                block,
            } => Ok(UploaderMessage::Block(
                BlockInfo {
                    piece_index: index as usize,
                    in_piece_offset: begin as usize,
                    block_length: block.len(),
                },
                block,
            )),
            _ => Err(msg),
        }
    }
}

impl fmt::Display for UploaderMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UploaderMessage::Choke => {
                write!(f, "Choke")
            }
            UploaderMessage::Unchoke => {
                write!(f, "Unchoke")
            }
            UploaderMessage::Have { piece_index } => {
                write!(f, "Have[ind={}]", piece_index)
            }
            UploaderMessage::Bitfield(bitvec) => {
                write!(f, "Bitfield[len={}]", bitvec.len())
            }
            UploaderMessage::Block(info, _) => {
                write!(f, "Block[{info}]")
            }
        }
    }
}

// ------

impl From<DownloaderMessage> for PeerMessage {
    fn from(msg: DownloaderMessage) -> Self {
        match msg {
            DownloaderMessage::Interested => PeerMessage::Interested,
            DownloaderMessage::NotInterested => PeerMessage::NotInterested,
            DownloaderMessage::Request(info) => PeerMessage::Request {
                index: info.piece_index as u32,
                begin: info.in_piece_offset as u32,
                length: info.block_length as u32,
            },
            DownloaderMessage::Cancel(info) => PeerMessage::Cancel {
                index: info.piece_index as u32,
                begin: info.in_piece_offset as u32,
                length: info.block_length as u32,
            },
        }
    }
}

impl TryFrom<PeerMessage> for DownloaderMessage {
    type Error = PeerMessage;

    fn try_from(msg: PeerMessage) -> Result<Self, Self::Error> {
        match msg {
            PeerMessage::Interested => Ok(DownloaderMessage::Interested),
            PeerMessage::NotInterested => Ok(DownloaderMessage::NotInterested),
            PeerMessage::Request {
                index,
                begin,
                length,
            } => Ok(DownloaderMessage::Request(BlockInfo {
                piece_index: index as usize,
                in_piece_offset: begin as usize,
                block_length: length as usize,
            })),
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => Ok(DownloaderMessage::Cancel(BlockInfo {
                piece_index: index as usize,
                in_piece_offset: begin as usize,
                block_length: length as usize,
            })),
            _ => Err(msg),
        }
    }
}

impl fmt::Display for DownloaderMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DownloaderMessage::Interested => {
                write!(f, "Interested")
            }
            DownloaderMessage::NotInterested => {
                write!(f, "NotInterested")
            }
            DownloaderMessage::Request(info) => {
                write!(f, "Request[{info}]")
            }
            DownloaderMessage::Cancel(info) => {
                write!(f, "Cancel[{info}]")
            }
        }
    }
}

// ------

impl Extension {
    const NAME_METADATA: &'static str = "ut_metadata";
    const NAME_PEX: &'static str = "ut_pex";

    const ID_HANDSHAKE: u8 = 0;
    const ID_METADATA: u8 = 1;
    const ID_PEX: u8 = 2;

    fn from_name(name: &str) -> Option<Self> {
        match name {
            Self::NAME_METADATA => Some(Self::Metadata),
            Self::NAME_PEX => Some(Self::PeerExchange),
            _ => None,
        }
    }
    fn name(&self) -> &'static str {
        match self {
            Extension::Metadata => Self::NAME_METADATA,
            Extension::PeerExchange => Self::NAME_PEX,
        }
    }
    pub const fn local_id(&self) -> u8 {
        match self {
            Extension::Metadata => Self::ID_METADATA,
            Extension::PeerExchange => Self::ID_PEX,
        }
    }
}

impl ExtendedHandshake {
    const KEY_M: &'static str = "m";
    const KEY_P: &'static str = "p";
    const KEY_V: &'static str = "v";
    const KEY_YOURIP: &'static str = "yourip";
    const KEY_IPV6: &'static str = "ipv6";
    const KEY_IPV4: &'static str = "ipv4";
    const KEY_REQQ: &'static str = "reqq";
    const KEY_METADATA_SIZE: &'static str = "metadata_size";

    fn decode(payload: &[u8]) -> Option<Self> {
        use crate::utils::benc::Element::{self, *};
        if let Dictionary(d) = Element::from_bytes(payload).ok()? {
            let mut root = benc::convert_dictionary(d);
            let mut ret = Self::default();
            if let Some(Integer(port)) = root.remove(Self::KEY_P) {
                ret.listen_port = port.try_into().ok();
            }
            if let Some(ByteString(v)) = root.remove(Self::KEY_V) {
                ret.client_type = String::from_utf8(v).ok();
            }
            if let Some(ByteString(ip)) = root.remove(Self::KEY_YOURIP) {
                if let Some(ipv6_bytes) = ip.get(0..16) {
                    let mut octets = [0u8; 16];
                    octets.copy_from_slice(ipv6_bytes);
                    ret.yourip = Some(IpAddr::V6(Ipv6Addr::from(octets)));
                } else if let Some(ipv4_bytes) = ip.get(0..4) {
                    let mut octets = [0u8; 4];
                    octets.copy_from_slice(ipv4_bytes);
                    ret.yourip = Some(IpAddr::V4(Ipv4Addr::from(octets)));
                }
            }
            if let Some(ByteString(ipv4)) = root.remove(Self::KEY_IPV4) {
                ret.ipv4 = ipv4.get(0..4).map(|bytes| {
                    let mut octets = [0u8; 4];
                    octets.copy_from_slice(bytes);
                    Ipv4Addr::from(octets)
                });
            }
            if let Some(ByteString(ipv6)) = root.remove(Self::KEY_IPV6) {
                ret.ipv6 = ipv6.get(0..16).map(|bytes| {
                    let mut octets = [0u8; 16];
                    octets.copy_from_slice(bytes);
                    Ipv6Addr::from(octets)
                });
            }
            if let Some(Integer(max_requests)) = root.remove(Self::KEY_REQQ) {
                ret.request_limit = max_requests.try_into().ok();
            }
            if let Some(Integer(metasize)) = root.remove(Self::KEY_METADATA_SIZE) {
                ret.metadata_size = metasize.try_into().ok();
            }
            if let Some(Dictionary(d)) = root.remove(Self::KEY_M) {
                ret.extensions = d
                    .into_iter()
                    .filter_map(|(key, value)| match (key, value) {
                        (ByteString(key), Integer(value)) => {
                            let extension_name = String::from_utf8(key).ok()?;
                            let extension = Extension::from_name(&extension_name)?;
                            let id = u8::try_from(value).ok()?;
                            Some((extension, id))
                        }
                        _ => None,
                    })
                    .collect();
            }
            Some(ret)
        } else {
            None
        }
    }

    fn encode(&self) -> Vec<u8> {
        use crate::utils::benc::Element::{self, *};
        let root = {
            let mut tmp = BTreeMap::new();
            let mut insert = |key: &str, val| {
                tmp.insert(Element::from(key), val);
            };
            if let Some(p) = self.listen_port {
                insert(Self::KEY_P, Integer(p as i64));
            }
            if let Some(v) = self.client_type.as_ref() {
                insert(Self::KEY_V, Element::from(v.as_str()));
            }
            if let Some(ip) = self.yourip {
                let value = match ip {
                    IpAddr::V4(ip) => ByteString(ip.octets().into()),
                    IpAddr::V6(ip) => ByteString(ip.octets().into()),
                };
                insert(Self::KEY_YOURIP, value);
            }
            if let Some(v4) = self.ipv4 {
                insert(Self::KEY_IPV4, ByteString(v4.octets().into()));
            }
            if let Some(v6) = self.ipv6 {
                insert(Self::KEY_IPV6, ByteString(v6.octets().into()));
            }
            if let Some(reqq) = self.request_limit {
                insert(Self::KEY_REQQ, Element::from(reqq as i64));
            }
            if let Some(metasize) = self.metadata_size {
                insert(Self::KEY_METADATA_SIZE, Integer(metasize as i64));
            }
            if !self.extensions.is_empty() {
                let m = self
                    .extensions
                    .iter()
                    .map(|(&extension, &id)| {
                        (Element::from(extension.name()), Element::from(i64::from(id)))
                    })
                    .collect::<BTreeMap<Element, Element>>();
                insert(Self::KEY_M, Dictionary(m));
            }
            Dictionary(tmp)
        };
        root.to_bytes()
    }
}

impl PeerExchangeData {
    const KEY_ADDED_V4: &'static str = "added";
    const KEY_ADDED_V6: &'static str = "added6";
    const KEY_DROPPED_V4: &'static str = "dropped";
    const KEY_DROPPED_V6: &'static str = "dropped6";

    fn decode(payload: &[u8]) -> Option<Self> {
        use benc::Element;
        fn parse_socket_addr(src: &[u8]) -> Option<SocketAddr> {
            match src.len() {
                6 => {
                    let (addr_data, port_data) = src.split_at(4);
                    let mut octets = [0u8; 4];
                    octets.copy_from_slice(addr_data);
                    Some(SocketAddr::new(
                        IpAddr::V4(Ipv4Addr::from(octets)),
                        u16::from_be_bytes(port_data.try_into().ok()?),
                    ))
                }
                18 => {
                    let (addr_data, port_data) = src.split_at(16);
                    let mut octets = [0u8; 16];
                    octets.copy_from_slice(addr_data);
                    Some(SocketAddr::new(
                        IpAddr::V6(Ipv6Addr::from(octets)),
                        u16::from_be_bytes(port_data.try_into().ok()?),
                    ))
                }
                _ => None,
            }
        }
        let content = Element::from_bytes(payload).ok()?;
        if let Element::Dictionary(mut d) = content {
            let mut read_ips = |key: &str, dest: &mut HashSet<SocketAddr>, repr_len: usize| {
                if let Some(Element::ByteString(s)) = d.remove(&Element::from(key)) {
                    for addr in s.chunks_exact(repr_len).filter_map(parse_socket_addr) {
                        dest.insert(addr);
                    }
                }
            };
            let mut added = HashSet::new();
            let mut dropped = HashSet::new();
            read_ips(Self::KEY_ADDED_V4, &mut added, 6);
            read_ips(Self::KEY_ADDED_V6, &mut added, 18);
            read_ips(Self::KEY_DROPPED_V4, &mut dropped, 6);
            read_ips(Self::KEY_DROPPED_V6, &mut dropped, 18);
            Some(PeerExchangeData { added, dropped })
        } else {
            None
        }
    }

    fn encode(&self) -> Vec<u8> {
        use crate::utils::benc::Element::{self, *};
        let mut root = BTreeMap::new();

        let mut added_ipv4 = Vec::new();
        let mut added_ipv6 = Vec::new();
        for addr in &self.added {
            match addr {
                SocketAddr::V4(addr) => {
                    added_ipv4.extend_from_slice(&addr.ip().octets());
                    added_ipv4.extend_from_slice(&u16::to_be_bytes(addr.port()));
                }
                SocketAddr::V6(addr) => {
                    added_ipv6.extend_from_slice(&addr.ip().octets());
                    added_ipv6.extend_from_slice(&u16::to_be_bytes(addr.port()));
                }
            }
        }
        root.insert(Element::from(Self::KEY_ADDED_V4), ByteString(added_ipv4));
        root.insert(Element::from(Self::KEY_ADDED_V6), ByteString(added_ipv6));

        let mut dropped_ipv4 = Vec::new();
        let mut dropped_ipv6 = Vec::new();
        for addr in &self.dropped {
            match addr {
                SocketAddr::V4(addr) => {
                    dropped_ipv4.extend_from_slice(&addr.ip().octets());
                    dropped_ipv4.extend_from_slice(&u16::to_be_bytes(addr.port()));
                }
                SocketAddr::V6(addr) => {
                    dropped_ipv6.extend_from_slice(&addr.ip().octets());
                    dropped_ipv6.extend_from_slice(&u16::to_be_bytes(addr.port()));
                }
            }
        }
        root.insert(Element::from(Self::KEY_DROPPED_V4), ByteString(dropped_ipv4));
        root.insert(Element::from(Self::KEY_DROPPED_V6), ByteString(dropped_ipv6));

        Dictionary(root).to_bytes()
    }
}

enum MetadataMsg {
    Request {
        piece: usize,
    },
    Block {
        piece: usize,
        total_size: usize,
        data: Vec<u8>,
    },
    Reject {
        piece: usize,
    },
}

impl MetadataMsg {
    const TYPE_REQUEST: u8 = 0;
    const TYPE_BLOCK: u8 = 1;
    const TYPE_REJECT: u8 = 2;

    const KEY_TYPE: &'static str = "msg_type";
    const KEY_PIECE: &'static str = "piece";
    const KEY_TOTAL_SIZE: &'static str = "total_size";

    fn decode(mut payload: Vec<u8>) -> Result<Self, Vec<u8>> {
        use benc::Element::{self, *};
        let bencode = match Element::from_bytes(&payload) {
            Ok(b) => b,
            Err(_) => return Err(payload),
        };
        let (msg_type, piece, total_size) = match &bencode {
            Dictionary(d) => {
                let msg_type = d.get(&Element::from(Self::KEY_TYPE)).and_then(|b| match b {
                    Integer(msg_type) => u8::try_from(*msg_type).ok(),
                    _ => None,
                });
                let piece = d.get(&Element::from(Self::KEY_PIECE)).and_then(|b| match b {
                    Integer(piece) => usize::try_from(*piece).ok(),
                    _ => None,
                });
                let total_size =
                    d.get(&Element::from(Self::KEY_TOTAL_SIZE)).and_then(|e| match e {
                        Element::Integer(total_size) => usize::try_from(*total_size).ok(),
                        _ => None,
                    });
                match (msg_type, piece) {
                    (Some(msg_type), Some(piece)) => (msg_type, piece, total_size),
                    _ => return Err(payload),
                }
            }
            _ => return Err(payload),
        };
        match (msg_type, total_size) {
            (Self::TYPE_REQUEST, _) => Ok(Self::Request { piece }),
            (Self::TYPE_REJECT, _) => Ok(Self::Reject { piece }),
            (Self::TYPE_BLOCK, Some(total_size)) => {
                let header_len = bencode.to_bytes().len();
                let total_len = payload.len();
                // remove bencode from the front and retain only the data that follows
                payload.copy_within(header_len..total_len, 0);
                payload.truncate(total_len - header_len);
                Ok(Self::Block {
                    piece,
                    total_size,
                    data: payload,
                })
            }
            _ => Err(payload),
        }
    }

    fn encode(self) -> Vec<u8> {
        use benc::Element::{self, *};
        let mut root = BTreeMap::<Element, Element>::new();
        match self {
            MetadataMsg::Request { piece } => {
                root.insert(Element::from(Self::KEY_TYPE), Integer(Self::TYPE_REQUEST.into()));
                root.insert(Element::from(Self::KEY_PIECE), Integer(piece as i64));
                Dictionary(root).to_bytes()
            }
            MetadataMsg::Reject { piece } => {
                root.insert(Element::from(Self::KEY_TYPE), Integer(Self::TYPE_REJECT.into()));
                root.insert(Element::from(Self::KEY_PIECE), Integer(piece as i64));
                Dictionary(root).to_bytes()
            }
            MetadataMsg::Block {
                piece,
                total_size,
                mut data,
            } => {
                root.insert(Element::from(Self::KEY_TYPE), Integer(Self::TYPE_BLOCK.into()));
                root.insert(Element::from(Self::KEY_PIECE), Integer(piece as i64));
                root.insert(Element::from(Self::KEY_TOTAL_SIZE), Integer(total_size as i64));
                let header = Dictionary(root).to_bytes();
                let header_len = header.len();
                let data_len = data.len();
                // add header in front of data
                data.resize(data_len + header_len, 0u8);
                data.copy_within(0..data_len, header_len);
                data[0..header_len].copy_from_slice(&header);
                data
            }
        }
    }
}

impl From<(ExtendedMessage, u8)> for PeerMessage {
    fn from((extmsg, id): (ExtendedMessage, u8)) -> Self {
        match extmsg {
            ExtendedMessage::Handshake(hs) => PeerMessage::Extended {
                id: Extension::ID_HANDSHAKE,
                data: hs.encode(),
            },
            ExtendedMessage::PeerExchange(pex) => PeerMessage::Extended {
                id,
                data: pex.encode(),
            },
            ExtendedMessage::MetadataRequest { piece } => {
                let msg = MetadataMsg::Request { piece };
                PeerMessage::Extended {
                    id,
                    data: msg.encode(),
                }
            }
            ExtendedMessage::MetadataReject { piece } => {
                let msg = MetadataMsg::Reject { piece };
                PeerMessage::Extended {
                    id,
                    data: msg.encode(),
                }
            }
            ExtendedMessage::MetadataBlock {
                piece,
                total_size,
                data,
            } => {
                let msg = MetadataMsg::Block {
                    piece,
                    total_size,
                    data,
                };
                PeerMessage::Extended {
                    id,
                    data: msg.encode(),
                }
            }
        }
    }
}

impl TryFrom<PeerMessage> for ExtendedMessage {
    type Error = PeerMessage;

    fn try_from(msg: PeerMessage) -> Result<Self, Self::Error> {
        match msg {
            PeerMessage::Extended {
                id: Extension::ID_HANDSHAKE,
                ref data,
            } => {
                let handshake = ExtendedHandshake::decode(data).ok_or(msg)?;
                Ok(ExtendedMessage::Handshake(Box::new(handshake)))
            }
            PeerMessage::Extended {
                id: Extension::ID_PEX,
                ref data,
            } => {
                let peer_data = PeerExchangeData::decode(data).ok_or(msg)?;
                Ok(ExtendedMessage::PeerExchange(Box::new(peer_data)))
            }
            PeerMessage::Extended {
                id: Extension::ID_METADATA,
                data,
            } => match MetadataMsg::decode(data) {
                Ok(MetadataMsg::Request { piece }) => {
                    Ok(ExtendedMessage::MetadataRequest { piece })
                }
                Ok(MetadataMsg::Reject { piece }) => Ok(ExtendedMessage::MetadataReject { piece }),
                Ok(MetadataMsg::Block {
                    piece,
                    total_size,
                    data,
                }) => Ok(ExtendedMessage::MetadataBlock {
                    piece,
                    total_size,
                    data,
                }),
                Err(data) => Err(PeerMessage::Extended {
                    id: Extension::ID_METADATA,
                    data,
                }),
            },
            _ => Err(msg),
        }
    }
}

impl fmt::Display for ExtendedMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExtendedMessage::Handshake(hs) => {
                write!(f, "ExtendedHandshake[{hs}]")
            }
            ExtendedMessage::MetadataRequest { piece } => {
                write!(f, "MetadataRequest[piece={piece}]")
            }
            ExtendedMessage::MetadataBlock { piece, data, .. } => {
                write!(f, "MetadataBlock[piece={} len={}]", piece, data.len())
            }
            ExtendedMessage::MetadataReject { piece } => {
                write!(f, "MetadataReject[piece={piece}]")
            }
            ExtendedMessage::PeerExchange(pex) => {
                write!(f, "PEX[{pex}]")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitfield_is_parsed_correctly() {
        let bits = b"\x82";
        let msg = UploaderMessage::Bitfield(BitVec::from_slice(bits));

        let bitfield = if let UploaderMessage::Bitfield(bitfield) = msg {
            bitfield
        } else {
            panic!()
        };

        assert!(bitfield[0]);
        assert!(!bitfield[1]);
        assert!(!bitfield[2]);
        assert!(!bitfield[3]);

        assert!(!bitfield[4]);
        assert!(!bitfield[5]);
        assert!(bitfield[6]);
        assert!(!bitfield[7]);
    }

    #[test]
    fn test_handshake_payload_is_parsed_correctly() {
        let payload =
            Vec::from(b"d1:md11:ut_metadatai1e6:ut_pexi2ee1:pi6881e1:v13:\xc2\xb5Torrent 1.2e");

        let parsed = ExtendedHandshake::decode(&payload).unwrap();
        assert_eq!(
            HashMap::from([(Extension::Metadata, 1), (Extension::PeerExchange, 2)]),
            parsed.extensions
        );
        assert_eq!(6881, parsed.listen_port.unwrap());
        assert_eq!("µTorrent 1.2", parsed.client_type.as_deref().unwrap());
        assert!(parsed.ipv4.is_none());
        assert!(parsed.ipv6.is_none());
        assert!(parsed.yourip.is_none());
        assert!(parsed.request_limit.is_none());
        assert!(parsed.metadata_size.is_none());

        let data = parsed.encode();
        assert_eq!(payload, data);
    }

    #[test]
    fn test_handshake_payload_is_serialized_correctly() {
        let hs = ExtendedHandshake {
            extensions: HashMap::from([(Extension::Metadata, 1), (Extension::PeerExchange, 2)]),
            listen_port: Some(6881),
            client_type: Some("µTorrent 1.2".to_owned()),
            yourip: None,
            ipv4: None,
            ipv6: None,
            request_limit: None,
            metadata_size: None,
        };
        assert_eq!(
            Vec::from(b"d1:md11:ut_metadatai1e6:ut_pexi2ee1:pi6881e1:v13:\xc2\xb5Torrent 1.2e"),
            hs.encode()
        );
    }

    #[test]
    fn test_metadata_block_payload_is_parsed_correctly() {
        let payload = Vec::from("d8:msg_typei1e5:piecei0e10:total_sizei34256eexxxxxxxx");
        let msg = PeerMessage::Extended {
            id: Extension::ID_METADATA,
            data: payload,
        };
        let parsed = ExtendedMessage::try_from(msg).unwrap();
        assert!(
            parsed
                == ExtendedMessage::MetadataBlock {
                    piece: 0,
                    total_size: 34256,
                    data: Vec::from(b"xxxxxxxx"),
                }
        );
    }

    #[test]
    fn test_metadata_block_payload_is_serialized_correctly() {
        let msg = MetadataMsg::Block {
            piece: 0,
            total_size: 34256,
            data: Vec::from(b"xxxxxxxx"),
        };
        assert_eq!(
            Vec::from("d8:msg_typei1e5:piecei0e10:total_sizei34256eexxxxxxxx"),
            msg.encode()
        );
    }
}
