#![allow(dead_code)]

use super::u160::U160;
use super::Error;
use crate::utils::benc;
use std::collections::BTreeMap;
use std::iter;
use std::net::{Ipv4Addr, SocketAddrV4};

pub struct Message {
    pub transaction_id: Vec<u8>,
    pub version: Option<String>,
    pub data: MessageData,
}

pub enum MessageData {
    Query(QueryMsg),
    Response(ResponseMsg),
    Error(ErrorMsg),
}

// ------------------------------------------------------------------------------------------------

const KEY_TRANSACTION: &str = "t";
const KEY_VERSION: &str = "v";
const KEY_TYPE: &str = "y";
const KEY_QUERY_ARGS: &str = "a";

const TYPE_QUERY: &str = "q";
const TYPE_RESPONSE: &str = "r";
const TYPE_ERROR: &str = "e";

const QUERY_PING: &str = "ping";
const QUERY_FIND_NODE: &str = "find_node";
const QUERY_GET_PEERS: &str = "get_peers";
const QUERY_ANNOUNCE_PEER: &str = "announce_peer";

impl From<Message> for benc::Element {
    fn from(msg: Message) -> Self {
        let mut root: BTreeMap<benc::Element, benc::Element> =
            [(KEY_TRANSACTION.into(), Self::ByteString(msg.transaction_id))].into();
        if let Some(version) = msg.version {
            root.insert(KEY_VERSION.into(), version.into());
        }
        match msg.data {
            MessageData::Query(query) => {
                root.insert(KEY_TYPE.into(), TYPE_QUERY.into());
                match query {
                    QueryMsg::Ping(ping) => {
                        root.insert(TYPE_QUERY.into(), QUERY_PING.into());
                        root.insert(KEY_QUERY_ARGS.into(), ping.into());
                    }
                    QueryMsg::FindNode(find_node) => {
                        root.insert(TYPE_QUERY.into(), QUERY_FIND_NODE.into());
                        root.insert(KEY_QUERY_ARGS.into(), find_node.into());
                    }
                    QueryMsg::GetPeers(get_peers) => {
                        root.insert(TYPE_QUERY.into(), QUERY_GET_PEERS.into());
                        root.insert(KEY_QUERY_ARGS.into(), get_peers.into());
                    }
                    QueryMsg::AnnouncePeer(announce_peer) => {
                        root.insert(TYPE_QUERY.into(), QUERY_ANNOUNCE_PEER.into());
                        root.insert(KEY_QUERY_ARGS.into(), announce_peer.into());
                    }
                }
            }
            MessageData::Response(response) => {
                root.insert(KEY_TYPE.into(), TYPE_RESPONSE.into());
                root.insert(TYPE_RESPONSE.into(), response.into());
            }
            MessageData::Error(error) => {
                root.insert(KEY_TYPE.into(), TYPE_ERROR.into());
                root.insert(TYPE_ERROR.into(), error.into());
            }
        }
        Self::Dictionary(root)
    }
}

impl TryFrom<benc::Element> for Message {
    type Error = Error;

    fn try_from(root: benc::Element) -> Result<Self, Self::Error> {
        let mut root = to_text_dictionary(root)?;

        let version = root.remove(KEY_VERSION).and_then(|v| match v {
            benc::Element::ByteString(bytes) => Some(String::from_utf8(bytes).ok()?),
            _ => None,
        });

        let transaction_id = root
            .remove(KEY_TRANSACTION)
            .and_then(|v| match v {
                benc::Element::ByteString(bytes) => Some(bytes),
                _ => None,
            })
            .ok_or(Error::ParseError("no transaction id"))?;

        let msg_type = root
            .remove(KEY_TYPE)
            .and_then(|v| match v {
                benc::Element::ByteString(bytes) => Some(String::from_utf8(bytes).ok()?),
                _ => None,
            })
            .ok_or(Error::ParseError("no type"))?;

        match msg_type.as_ref() {
            TYPE_QUERY => {
                let query_type = root
                    .remove(TYPE_QUERY)
                    .and_then(|v| match v {
                        benc::Element::ByteString(bytes) => Some(String::from_utf8(bytes).ok()?),
                        _ => None,
                    })
                    .ok_or(Error::ParseError("no query type"))?;

                let query_args =
                    root.remove(KEY_QUERY_ARGS).ok_or(Error::ParseError("no query args"))?;

                let query = match query_type.as_str() {
                    QUERY_PING => QueryMsg::Ping(query_args.try_into()?),
                    QUERY_FIND_NODE => QueryMsg::FindNode(query_args.try_into()?),
                    QUERY_GET_PEERS => QueryMsg::GetPeers(query_args.try_into()?),
                    QUERY_ANNOUNCE_PEER => QueryMsg::AnnouncePeer(query_args.try_into()?),
                    _ => return Err(Error::ParseError("unknown query type")),
                };

                Ok(Self {
                    transaction_id,
                    version,
                    data: MessageData::Query(query),
                })
            }
            TYPE_RESPONSE => {
                let response_data =
                    root.remove(TYPE_RESPONSE).ok_or(Error::ParseError("no response"))?;
                Ok(Self {
                    transaction_id,
                    version,
                    data: MessageData::Response(response_data.try_into()?),
                })
            }
            TYPE_ERROR => {
                let error_data =
                    root.remove(TYPE_ERROR).ok_or(Error::ParseError("no error data"))?;
                Ok(Self {
                    transaction_id,
                    version,
                    data: MessageData::Error(error_data.try_into()?),
                })
            }
            _ => Err(Error::ParseError("unknown message type")),
        }
    }
}

// ------------------------------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
pub enum ErrorCode {
    Generic = 201,
    Server = 202,
    Protocol = 203,
    MethodUnknown = 204,
}

impl TryFrom<benc::Element> for ErrorCode {
    type Error = Error;

    fn try_from(value: benc::Element) -> Result<Self, Self::Error> {
        match value {
            benc::Element::Integer(code) => match code {
                201 => Ok(Self::Generic),
                202 => Ok(Self::Server),
                203 => Ok(Self::Protocol),
                204 => Ok(Self::MethodUnknown),
                _ => Err(Error::ParseError("unknown error code")),
            },
            _ => Err(Error::ParseError("not an integer")),
        }
    }
}

pub struct ErrorMsg {
    pub error_code: ErrorCode,
    pub error_msg: String,
}

impl From<ErrorMsg> for benc::Element {
    fn from(msg: ErrorMsg) -> Self {
        Self::List(vec![(msg.error_code as i64).into(), msg.error_msg.into()])
    }
}

impl TryFrom<benc::Element> for ErrorMsg {
    type Error = Error;

    fn try_from(value: benc::Element) -> Result<Self, Self::Error> {
        let mut list = match value {
            benc::Element::List(list) => list,
            _ => return Err(Error::ParseError("not a list")),
        };
        let error_msg = list
            .pop()
            .and_then(|msg| match msg {
                benc::Element::ByteString(bytes) => String::from_utf8(bytes).ok(),
                _ => None,
            })
            .ok_or(Error::ParseError("no error message"))?;
        let error_code = list
            .pop()
            .ok_or(Error::ParseError("no error code"))
            .and_then(ErrorCode::try_from)?;
        Ok(Self {
            error_code,
            error_msg,
        })
    }
}

// ------------------------------------------------------------------------------------------------

pub enum QueryMsg {
    Ping(PingArgs),
    FindNode(FindNodeArgs),
    GetPeers(GetPeersArgs),
    AnnouncePeer(AnnouncePeerArgs),
}

pub struct PingArgs {
    pub id: U160,
}

impl From<PingArgs> for benc::Element {
    fn from(query: PingArgs) -> Self {
        Self::Dictionary([("id".into(), query.id.into())].into())
    }
}

impl TryFrom<benc::Element> for PingArgs {
    type Error = Error;

    fn try_from(data: benc::Element) -> Result<Self, Error> {
        let mut dict = to_text_dictionary(data)?;
        Ok(Self {
            id: dict.remove("id").ok_or(Error::ParseError("no id"))?.try_into()?,
        })
    }
}

pub struct FindNodeArgs {
    pub id: U160,
    pub target: U160,
}

impl From<FindNodeArgs> for benc::Element {
    fn from(query: FindNodeArgs) -> Self {
        Self::Dictionary(
            [
                ("id".into(), query.id.into()),
                ("target".into(), query.target.into()),
            ]
            .into(),
        )
    }
}

impl TryFrom<benc::Element> for FindNodeArgs {
    type Error = Error;

    fn try_from(data: benc::Element) -> Result<Self, Error> {
        let mut dict = to_text_dictionary(data)?;
        Ok(Self {
            id: dict.remove("id").ok_or(Error::ParseError("no id"))?.try_into()?,
            target: dict.remove("target").ok_or(Error::ParseError("no target"))?.try_into()?,
        })
    }
}

pub struct GetPeersArgs {
    pub id: U160,
    pub info_hash: U160,
}

impl From<GetPeersArgs> for benc::Element {
    fn from(query: GetPeersArgs) -> Self {
        Self::Dictionary(
            [
                ("id".into(), query.id.into()),
                ("info_hash".into(), query.info_hash.into()),
            ]
            .into(),
        )
    }
}

impl TryFrom<benc::Element> for GetPeersArgs {
    type Error = Error;

    fn try_from(data: benc::Element) -> Result<Self, Error> {
        let mut dict = to_text_dictionary(data)?;
        Ok(Self {
            id: dict.remove("id").ok_or(Error::ParseError("no id"))?.try_into()?,
            info_hash: dict
                .remove("info_hash")
                .ok_or(Error::ParseError("no info_hash"))?
                .try_into()?,
        })
    }
}

pub struct AnnouncePeerArgs {
    pub id: U160,
    pub info_hash: U160,
    pub port: Option<u16>,
    pub token: Vec<u8>,
}

impl From<AnnouncePeerArgs> for benc::Element {
    fn from(query: AnnouncePeerArgs) -> Self {
        let mut args: BTreeMap<benc::Element, benc::Element> = [
            ("id".into(), query.id.into()),
            ("info_hash".into(), query.info_hash.into()),
            ("token".into(), Self::ByteString(query.token)),
            ("port".into(), i64::from(query.port.unwrap_or_default()).into()),
        ]
        .into();
        if query.port.is_none() {
            args.insert("implied_port".into(), 1i64.into());
        }
        Self::Dictionary(args)
    }
}

impl TryFrom<benc::Element> for AnnouncePeerArgs {
    type Error = Error;

    fn try_from(data: benc::Element) -> Result<Self, Error> {
        fn parse_token(token: benc::Element) -> Result<Vec<u8>, Error> {
            match token {
                benc::Element::ByteString(bytes) => Ok(bytes),
                _ => Err(Error::ParseError("token not a byte string")),
            }
        }
        fn parse_port(port: benc::Element) -> Result<u16, Error> {
            match port {
                benc::Element::Integer(port) => {
                    port.try_into().map_err(|_| Error::ParseError("port not an integer"))
                }
                _ => Err(Error::ParseError("port not an integer")),
            }
        }

        let mut dict = to_text_dictionary(data)?;

        Ok(Self {
            id: dict.remove("id").ok_or(Error::ParseError("no id"))?.try_into()?,
            info_hash: dict
                .remove("info_hash")
                .ok_or(Error::ParseError("no info_hash"))?
                .try_into()?,
            port: if let Some(benc::Element::Integer(1i64)) = dict.remove("implied_port") {
                None
            } else {
                Some(dict.remove("port").ok_or(Error::ParseError("no port")).and_then(parse_port)?)
            },
            token: dict
                .remove("token")
                .ok_or(Error::ParseError("no token"))
                .and_then(parse_token)?,
        })
    }
}

// ------------------------------------------------------------------------------------------------

pub struct ResponseMsg {
    data: BTreeMap<String, benc::Element>,
}

impl From<ResponseMsg> for benc::Element {
    fn from(msg: ResponseMsg) -> Self {
        Self::Dictionary(msg.data.into_iter().map(|(k, v)| (k.into(), v)).collect())
    }
}

impl TryFrom<benc::Element> for ResponseMsg {
    type Error = Error;

    fn try_from(value: benc::Element) -> Result<Self, Self::Error> {
        Ok(Self {
            data: to_text_dictionary(value)?,
        })
    }
}

pub struct PingResponse {
    pub id: U160,
}

impl From<PingResponse> for ResponseMsg {
    fn from(response: PingResponse) -> Self {
        Self {
            data: [("id".into(), response.id.into())].into(),
        }
    }
}

impl TryFrom<ResponseMsg> for PingResponse {
    type Error = Error;

    fn try_from(msg: ResponseMsg) -> Result<Self, Self::Error> {
        let mut data = msg.data;
        Ok(Self {
            id: data.remove("id").ok_or(Error::ParseError("no id"))?.try_into()?,
        })
    }
}

pub struct FindNodeResponse {
    pub id: U160,
    pub nodes: Vec<(U160, SocketAddrV4)>,
}

impl From<FindNodeResponse> for ResponseMsg {
    fn from(response: FindNodeResponse) -> Self {
        Self {
            data: [
                ("id".into(), response.id.into()),
                (
                    "nodes".into(),
                    benc::Element::ByteString(serialize_nodes(response.nodes.into_iter())),
                ),
            ]
            .into(),
        }
    }
}

impl TryFrom<ResponseMsg> for FindNodeResponse {
    type Error = Error;

    fn try_from(mut msg: ResponseMsg) -> Result<Self, Self::Error> {
        Ok(Self {
            id: msg.data.remove("id").ok_or(Error::ParseError("no id"))?.try_into()?,
            nodes: msg.data.remove("nodes").ok_or(Error::ParseError("no nodes")).and_then(
                |nodes| match nodes {
                    benc::Element::ByteString(bytes) => Ok(deserialize_nodes(&bytes).collect()),
                    _ => Err(Error::ParseError("nodes not a byte string")),
                },
            )?,
        })
    }
}

pub struct GetPeersResponse {
    pub id: U160,
    pub token: Vec<u8>,
    pub data: GetPeersResponseData,
}

pub enum GetPeersResponseData {
    Nodes(Vec<(U160, SocketAddrV4)>),
    Peers(Vec<SocketAddrV4>),
}

impl From<GetPeersResponse> for ResponseMsg {
    fn from(response: GetPeersResponse) -> Self {
        fn serialize_ipv4(ip: SocketAddrV4) -> benc::Element {
            let mut buffer = vec![0u8; 6];
            let (octets, port) = unsafe { buffer.split_at_mut_unchecked(4) };
            octets.copy_from_slice(&ip.ip().octets());
            port.copy_from_slice(&ip.port().to_be_bytes());
            benc::Element::ByteString(buffer)
        }
        Self {
            data: [
                ("id".into(), response.id.into()),
                ("token".into(), benc::Element::ByteString(response.token)),
                match response.data {
                    GetPeersResponseData::Nodes(nodes) => (
                        "nodes".into(),
                        benc::Element::ByteString(serialize_nodes(nodes.into_iter())),
                    ),
                    GetPeersResponseData::Peers(peers) => (
                        "values".into(),
                        benc::Element::List(peers.into_iter().map(serialize_ipv4).collect()),
                    ),
                },
            ]
            .into(),
        }
    }
}

impl TryFrom<ResponseMsg> for GetPeersResponse {
    type Error = Error;

    fn try_from(mut msg: ResponseMsg) -> Result<Self, Self::Error> {
        let id: U160 = msg.data.remove("id").ok_or(Error::ParseError("no id"))?.try_into()?;

        let token = msg
            .data
            .remove("token")
            .and_then(|token| match token {
                benc::Element::ByteString(bytes) => Some(bytes),
                _ => None,
            })
            .ok_or(Error::ParseError("no token"))?;

        let data = if let Some(nodes) = msg.data.remove("nodes") {
            GetPeersResponseData::Nodes(match nodes {
                benc::Element::ByteString(bytes) => deserialize_nodes(&bytes).collect(),
                _ => return Err(Error::ParseError("nodes not a byte string")),
            })
        } else if let Some(peers) = msg.data.remove("values") {
            GetPeersResponseData::Peers(match peers {
                benc::Element::List(peers) => peers
                    .into_iter()
                    .filter_map(|peer| match peer {
                        benc::Element::ByteString(bytes) => {
                            let octets = *bytes.first_chunk::<4>()?;
                            let port = u16::from_be_bytes(*bytes.last_chunk::<2>()?);
                            Some(SocketAddrV4::new(Ipv4Addr::from(octets), port))
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
                _ => return Err(Error::ParseError("peers not a list")),
            })
        } else {
            return Err(Error::ParseError("no nodes or values"));
        };

        Ok(Self { id, token, data })
    }
}

pub struct AnnouncePeerResponse {
    pub id: U160,
}

impl From<AnnouncePeerResponse> for ResponseMsg {
    fn from(response: AnnouncePeerResponse) -> Self {
        Self {
            data: [("id".into(), response.id.into())].into(),
        }
    }
}

impl TryFrom<ResponseMsg> for AnnouncePeerResponse {
    type Error = Error;

    fn try_from(value: ResponseMsg) -> Result<Self, Self::Error> {
        let mut data = value.data;
        Ok(Self {
            id: data.remove("id").ok_or(Error::ParseError("no id"))?.try_into()?,
        })
    }
}

// ------------------------------------------------------------------------------------------------

fn serialize_nodes(nodes: impl ExactSizeIterator<Item = (U160, SocketAddrV4)>) -> Vec<u8> {
    let mut buffer = vec![0u8; nodes.len() * 26];
    for ((id, addr), dst) in iter::zip(nodes, buffer.chunks_exact_mut(26)) {
        unsafe {
            dst.get_unchecked_mut(0..20).copy_from_slice(id.as_ref());
            dst.get_unchecked_mut(20..24).copy_from_slice(&addr.ip().octets());
            dst.get_unchecked_mut(24..26).copy_from_slice(&addr.port().to_be_bytes());
        }
    }
    buffer
}

fn deserialize_nodes(data: &[u8]) -> impl Iterator<Item = (U160, SocketAddrV4)> + '_ {
    data.chunks_exact(26).map(|chunk| unsafe {
        let id_bytes: [u8; 20] = chunk.get_unchecked(0..20).try_into().unwrap_unchecked();
        let ipv4_octets: [u8; 4] = chunk.get_unchecked(20..24).try_into().unwrap_unchecked();
        let port = u16::from_be_bytes(chunk.get_unchecked(24..26).try_into().unwrap_unchecked());
        (U160::from(id_bytes), SocketAddrV4::new(Ipv4Addr::from(ipv4_octets), port))
    })
}

fn to_text_dictionary(data: benc::Element) -> Result<BTreeMap<String, benc::Element>, Error> {
    match data {
        benc::Element::Dictionary(d) => Ok(benc::convert_dictionary(d)),
        _ => Err(Error::ParseError("not a dictionary")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_encode_error() {
        let msg = Message {
            transaction_id: Vec::from(b"aa"),
            version: None,
            data: MessageData::Error(ErrorMsg {
                error_code: ErrorCode::Generic,
                error_msg: "A Generic Error Ocurred".to_owned(),
            }),
        };
        let bencoded = benc::Element::from(msg);
        assert_eq!(bencoded.to_bytes(), b"d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee");
    }

    #[test]
    fn test_decode_error() {
        let bencoded =
            benc::Element::from_bytes(b"d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee")
                .unwrap();
        let msg = Message::try_from(bencoded).unwrap();
        assert_eq!(msg.transaction_id, Vec::from(b"aa"));
        assert_eq!(msg.version, None);
        if let MessageData::Error(error) = msg.data {
            assert_eq!(error.error_code, ErrorCode::Generic);
            assert_eq!(error.error_msg, "A Generic Error Ocurred");
        } else {
            panic!("unexpected message type");
        }
    }

    #[test]
    fn test_encode_ping_query() {
        let msg = Message {
            transaction_id: Vec::from(b"aa"),
            version: None,
            data: MessageData::Query(QueryMsg::Ping(PingArgs {
                id: U160::from(b"abcdefghij0123456789"),
            })),
        };
        let bencoded = benc::Element::from(msg);
        assert_eq!(
            bencoded.to_bytes(),
            b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe"
        );
    }

    #[test]
    fn test_decode_ping_query() {
        let bencoded =
            benc::Element::from_bytes(b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe")
                .unwrap();
        let msg = Message::try_from(bencoded).unwrap();
        assert_eq!(msg.transaction_id, Vec::from(b"aa"));
        assert_eq!(msg.version, None);
        if let MessageData::Query(QueryMsg::Ping(ping)) = msg.data {
            assert_eq!(ping.id, U160::from(b"abcdefghij0123456789"));
        } else {
            panic!("unexpected message type");
        }
    }

    #[test]
    fn test_encode_ping_response() {
        let msg = Message {
            transaction_id: Vec::from(b"aa"),
            version: None,
            data: MessageData::Response(
                PingResponse {
                    id: U160::from(b"mnopqrstuvwxyz123456"),
                }
                .into(),
            ),
        };
        let bencoded = benc::Element::from(msg);
        assert_eq!(bencoded.to_bytes(), b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re");
    }

    #[test]
    fn test_decode_ping_response() {
        let bencoded =
            benc::Element::from_bytes(b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re").unwrap();
        let msg = Message::try_from(bencoded).unwrap();
        assert_eq!(msg.transaction_id, Vec::from(b"aa"));
        assert_eq!(msg.version, None);
        if let MessageData::Response(msg) = msg.data {
            let ping_response = PingResponse::try_from(msg).unwrap();
            assert_eq!(ping_response.id, U160::from(b"mnopqrstuvwxyz123456"));
        } else {
            panic!("unexpected message type");
        }
    }

    #[test]
    fn test_encode_find_node_query() {
        let msg = Message {
            transaction_id: Vec::from(b"aa"),
            version: None,
            data: MessageData::Query(QueryMsg::FindNode(FindNodeArgs {
                id: U160::from(b"abcdefghij0123456789"),
                target: U160::from(b"mnopqrstuvwxyz123456"),
            })),
        };
        let bencoded = benc::Element::from(msg);
        assert_eq!(
            bencoded.to_bytes(),
            b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe"
        );
    }

    #[test]
    fn test_decode_find_node_query() {
        let bencoded = benc::Element::from_bytes(
            b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe",
        )
        .unwrap();
        let msg = Message::try_from(bencoded).unwrap();
        assert_eq!(msg.transaction_id, Vec::from(b"aa"));
        assert_eq!(msg.version, None);
        if let MessageData::Query(QueryMsg::FindNode(find_node)) = msg.data {
            assert_eq!(find_node.id, U160::from(b"abcdefghij0123456789"));
            assert_eq!(find_node.target, U160::from(b"mnopqrstuvwxyz123456"));
        } else {
            panic!("unexpected message type");
        }
    }

    #[test]
    fn test_encode_find_node_response() {
        let msg = Message {
            transaction_id: Vec::from(b"aa"),
            version: None,
            data: MessageData::Response(
                FindNodeResponse {
                    id: b"0123456789abcdefghij".into(),
                    nodes: [(
                        U160::from(b"mnopqrstuvwxyz123456"),
                        SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666),
                    )]
                    .into(),
                }
                .into(),
            ),
        };
        let bencoded = benc::Element::from(msg);
        assert_eq!(
            bencoded.to_bytes(),
            b"d1:rd2:id20:0123456789abcdefghij5:nodes26:mnopqrstuvwxyz123456\x7F\x00\x00\x01\x1A\x0Ae1:t2:aa1:y1:re"
        );
    }

    #[test]
    fn test_decode_find_node_response() {
        let bencoded = benc::Element::from_bytes(
            b"d1:rd2:id20:0123456789abcdefghij5:nodes26:mnopqrstuvwxyz123456\x7F\x00\x00\x01\x1A\x0Ae1:t2:aa1:y1:re",
        )
        .unwrap();
        let msg = Message::try_from(bencoded).unwrap();
        assert_eq!(msg.transaction_id, Vec::from(b"aa"));
        assert_eq!(msg.version, None);
        if let MessageData::Response(msg) = msg.data {
            let find_node_response = FindNodeResponse::try_from(msg).unwrap();
            assert_eq!(find_node_response.id, U160::from(b"0123456789abcdefghij"));
            assert_eq!(
                find_node_response.nodes,
                vec![(
                    U160::from(b"mnopqrstuvwxyz123456"),
                    SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)
                )]
            );
        } else {
            panic!("unexpected message type");
        }
    }

    #[test]
    fn test_encode_get_peers_query() {
        let msg = Message {
            transaction_id: Vec::from(b"aa"),
            version: None,
            data: MessageData::Query(QueryMsg::GetPeers(GetPeersArgs {
                id: U160::from(b"abcdefghij0123456789"),
                info_hash: U160::from(b"mnopqrstuvwxyz123456"),
            })),
        };
        let bencoded = benc::Element::from(msg);
        assert_eq!(
            bencoded.to_bytes(),
            b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe"
        );
    }

    #[test]
    fn test_decode_get_peers_query() {
        let bencoded = benc::Element::from_bytes(
            b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe",
        )
        .unwrap();
        let msg = Message::try_from(bencoded).unwrap();
        assert_eq!(msg.transaction_id, Vec::from(b"aa"));
        assert_eq!(msg.version, None);
        if let MessageData::Query(QueryMsg::GetPeers(get_peers)) = msg.data {
            assert_eq!(get_peers.id, U160::from(b"abcdefghij0123456789"));
            assert_eq!(get_peers.info_hash, U160::from(b"mnopqrstuvwxyz123456"));
        } else {
            panic!("unexpected message type");
        }
    }

    #[test]
    fn test_encode_get_peers_response() {
        let msg = Message {
            transaction_id: Vec::from(b"aa"),
            version: None,
            data: MessageData::Response(
                GetPeersResponse {
                    id: U160::from(b"abcdefghij0123456789"),
                    token: Vec::from(b"aoeusnth"),
                    data: GetPeersResponseData::Peers(
                        [
                            SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666),
                            SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7777),
                        ]
                        .into(),
                    ),
                }
                .into(),
            ),
        };
        let bencoded = benc::Element::from(msg);
        assert_eq!(
            bencoded.to_bytes(),
            b"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:\x7F\x00\x00\x01\x1A\x0A6:\x7F\x00\x00\x01\x1E\x61ee1:t2:aa1:y1:re"
        );

        let msg = Message {
            transaction_id: Vec::from(b"aa"),
            version: None,
            data: MessageData::Response(
                GetPeersResponse {
                    id: U160::from(b"abcdefghij0123456789"),
                    token: Vec::from(b"aoeusnth"),
                    data: GetPeersResponseData::Nodes(
                        [
                            (
                                b"abcdefghij0123456789".into(),
                                SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666),
                            ),
                            (
                                b"mnopqrstuvwxyz123456".into(),
                                SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7777),
                            ),
                        ]
                        .into(),
                    ),
                }
                .into(),
            ),
        };
        let bencoded = benc::Element::from(msg);
        assert_eq!(
            bencoded.to_bytes(),
            b"d1:rd2:id20:abcdefghij01234567895:nodes52:abcdefghij0123456789\x7F\x00\x00\x01\x1A\x0Amnopqrstuvwxyz123456\x7F\x00\x00\x01\x1E\x615:token8:aoeusnthe1:t2:aa1:y1:re"
        );
    }

    #[test]
    fn test_decode_get_peers_response() {
        let bencoded = benc::Element::from_bytes(b"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:\x7F\x00\x00\x01\x1A\x0A6:\x7F\x00\x00\x01\x1E\x61ee1:t2:aa1:y1:re").unwrap();
        let msg = Message::try_from(bencoded).unwrap();
        assert_eq!(msg.transaction_id, Vec::from(b"aa"));
        assert_eq!(msg.version, None);
        if let MessageData::Response(msg) = msg.data {
            let get_peers_response = GetPeersResponse::try_from(msg).unwrap();
            assert_eq!(get_peers_response.id, U160::from(b"abcdefghij0123456789"));
            assert_eq!(get_peers_response.token, Vec::from(b"aoeusnth"));
            if let GetPeersResponseData::Peers(peers) = get_peers_response.data {
                assert_eq!(
                    peers,
                    vec![
                        SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666),
                        SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7777)
                    ]
                );
            } else {
                panic!("unexpected data type");
            }
        } else {
            panic!("unexpected message type");
        }

        let bencoded = benc::Element::from_bytes(b"d1:rd2:id20:abcdefghij01234567895:nodes52:abcdefghij0123456789\x7F\x00\x00\x01\x1A\x0Amnopqrstuvwxyz123456\x7F\x00\x00\x01\x1E\x615:token8:aoeusnthe1:t2:aa1:y1:re").unwrap();
        let msg = Message::try_from(bencoded).unwrap();
        assert_eq!(msg.transaction_id, Vec::from(b"aa"));
        assert_eq!(msg.version, None);
        if let MessageData::Response(msg) = msg.data {
            let get_peers_response = GetPeersResponse::try_from(msg).unwrap();
            assert_eq!(get_peers_response.id, U160::from(b"abcdefghij0123456789"));
            assert_eq!(get_peers_response.token, Vec::from(b"aoeusnth"));
            if let GetPeersResponseData::Nodes(nodes) = get_peers_response.data {
                assert_eq!(
                    nodes,
                    vec![
                        (
                            U160::from(b"abcdefghij0123456789"),
                            SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)
                        ),
                        (
                            U160::from(b"mnopqrstuvwxyz123456"),
                            SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7777)
                        ),
                    ]
                );
            } else {
                panic!("unexpected data type");
            }
        } else {
            panic!("unexpected message type");
        }
    }

    #[test]
    fn test_encode_announce_peer_query() {
        let msg = Message {
            transaction_id: Vec::from(b"aa"),
            version: None,
            data: MessageData::Query(QueryMsg::AnnouncePeer(AnnouncePeerArgs {
                id: U160::from(b"abcdefghij0123456789"),
                info_hash: U160::from(b"mnopqrstuvwxyz123456"),
                port: Some(6881),
                token: Vec::from(b"aoeusnth"),
            })),
        };
        let bencoded = benc::Element::from(msg);
        assert_eq!(
            bencoded.to_bytes(),
            b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe"
        );

        let msg = Message {
            transaction_id: Vec::from(b"aa"),
            version: None,
            data: MessageData::Query(QueryMsg::AnnouncePeer(AnnouncePeerArgs {
                id: U160::from(b"abcdefghij0123456789"),
                info_hash: U160::from(b"mnopqrstuvwxyz123456"),
                port: None,
                token: Vec::from(b"aoeusnth"),
            })),
        };
        let bencoded = benc::Element::from(msg);
        assert_eq!(
            bencoded.to_bytes(),
            b"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti0e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe"
        );
    }

    #[test]
    fn test_decode_announce_peer_query() {
        let bencoded = benc::Element::from_bytes(
            b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe",
        )
        .unwrap();
        let msg = Message::try_from(bencoded).unwrap();
        assert_eq!(msg.transaction_id, Vec::from(b"aa"));
        assert_eq!(msg.version, None);
        if let MessageData::Query(QueryMsg::AnnouncePeer(announce_peer)) = msg.data {
            assert_eq!(announce_peer.id, U160::from(b"abcdefghij0123456789"));
            assert_eq!(announce_peer.info_hash, U160::from(b"mnopqrstuvwxyz123456"));
            assert_eq!(announce_peer.port, Some(6881));
            assert_eq!(announce_peer.token, Vec::from(b"aoeusnth"));
        } else {
            panic!("unexpected message type");
        }

        let bencoded = benc::Element::from_bytes(
            b"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti0e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe",
        )
        .unwrap();
        let msg = Message::try_from(bencoded).unwrap();
        assert_eq!(msg.transaction_id, Vec::from(b"aa"));
        assert_eq!(msg.version, None);
        if let MessageData::Query(QueryMsg::AnnouncePeer(announce_peer)) = msg.data {
            assert_eq!(announce_peer.id, U160::from(b"abcdefghij0123456789"));
            assert_eq!(announce_peer.info_hash, U160::from(b"mnopqrstuvwxyz123456"));
            assert_eq!(announce_peer.port, None);
            assert_eq!(announce_peer.token, Vec::from(b"aoeusnth"));
        } else {
            panic!("unexpected message type");
        }
    }

    #[test]
    fn test_encode_announce_peer_response() {
        let msg = Message {
            transaction_id: Vec::from(b"aa"),
            version: None,
            data: MessageData::Response(
                AnnouncePeerResponse {
                    id: U160::from(b"mnopqrstuvwxyz123456"),
                }
                .into(),
            ),
        };
        let bencoded = benc::Element::from(msg);
        assert_eq!(bencoded.to_bytes(), b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re");
    }

    #[test]
    fn test_decode_announce_peer_response() {
        let bencoded =
            benc::Element::from_bytes(b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re").unwrap();
        let msg = Message::try_from(bencoded).unwrap();
        assert_eq!(msg.transaction_id, Vec::from(b"aa"));
        assert_eq!(msg.version, None);
        if let MessageData::Response(msg) = msg.data {
            let announce_peer_response = AnnouncePeerResponse::try_from(msg).unwrap();
            assert_eq!(announce_peer_response.id, U160::from(b"mnopqrstuvwxyz123456"));
        } else {
            panic!("unexpected message type");
        }
    }
}
