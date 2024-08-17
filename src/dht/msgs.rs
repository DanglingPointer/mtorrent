#![allow(dead_code)]

use super::u160::U160;
use crate::utils::benc;
use std::{collections::BTreeMap, iter, net::SocketAddrV4};

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

// ------------------------------------------------------------------------------------------------

pub enum ErrorCode {
    Generic = 201,
    Server = 202,
    Protocol = 203,
    MethodUnknown = 204,
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

// ------------------------------------------------------------------------------------------------

pub struct ResponseMsg {
    data: BTreeMap<String, benc::Element>,
}

impl From<ResponseMsg> for benc::Element {
    fn from(msg: ResponseMsg) -> Self {
        Self::Dictionary(msg.data.into_iter().map(|(k, v)| (k.into(), v)).collect())
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
}
