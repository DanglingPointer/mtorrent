use bytes::{Buf, BufMut};
use derive_more::Display;
use local_async_utils::prelude::*;
use mtorrent_utils::ip;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::Utf8Error;
use std::{io, str};
use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::time::{Instant, timeout};

pub struct TrackerConnection {
    socket: UdpSocket,
    remote_addr: SocketAddr,
    connection_id: u64,
    connection_eof: Instant,
}

impl TrackerConnection {
    pub async fn from_connected_socket(socket: UdpSocket) -> io::Result<Self> {
        let remote_addr = socket.peer_addr()?;
        let mut conn = Self {
            socket,
            remote_addr,
            connection_id: 0,
            connection_eof: Instant::now(),
        };
        conn.renew().await?;
        Ok(conn)
    }

    #[expect(dead_code)]
    pub fn remote_addr(&self) -> &SocketAddr {
        &self.remote_addr
    }

    pub async fn do_announce_request(
        &mut self,
        request_data: AnnounceRequest,
    ) -> io::Result<AnnounceResponse> {
        if self.connection_eof <= Instant::now() {
            self.renew().await?;
        }

        let transaction_id = rand::random::<u32>();
        let request = {
            let mut buffer = Vec::with_capacity(AnnounceRequest::MIN_LENGTH);
            request_data.encode(self.connection_id, transaction_id, &mut buffer);
            buffer
        };

        log::debug!("Sending announce request to {}", self.remote_addr);

        Self::do_request(
            &self.socket,
            request,
            |data: &[u8]| -> Option<io::Result<AnnounceResponse>> {
                match parse_response(data, transaction_id, &self.remote_addr.ip()) {
                    Ok(AnyResponse::Announce(announce)) => {
                        log::debug!(
                            "Received announce response from {}: {:?}",
                            self.remote_addr,
                            announce
                        );
                        Some(Ok(announce))
                    }
                    Ok(AnyResponse::Error(error)) => Some(Err(io::Error::other(error.message))),
                    _ => None,
                }
            },
        )
        .await?
    }

    #[cfg_attr(not(test), expect(dead_code))]
    pub async fn do_scrape_request(
        &mut self,
        request_data: ScrapeRequest,
    ) -> io::Result<ScrapeResponse> {
        if self.connection_eof <= Instant::now() {
            self.renew().await?;
        }

        let transaction_id = rand::random::<u32>();
        let request = {
            let mut buffer = Vec::with_capacity(ScrapeRequest::MIN_LENGTH);
            request_data.encode(self.connection_id, transaction_id, &mut buffer);
            buffer
        };

        log::debug!("Sending scrape request to {}", self.remote_addr);

        Self::do_request(
            &self.socket,
            request,
            |data: &[u8]| -> Option<io::Result<ScrapeResponse>> {
                match parse_response(data, transaction_id, &self.remote_addr.ip()) {
                    Ok(AnyResponse::Scrape(scrape)) => {
                        log::debug!(
                            "Received scrape response from {}: {:?}",
                            self.remote_addr,
                            scrape
                        );
                        Some(Ok(scrape))
                    }
                    Ok(AnyResponse::Error(error)) => Some(Err(io::Error::other(error.message))),
                    _ => None,
                }
            },
        )
        .await?
    }

    async fn renew(&mut self) -> io::Result<()> {
        let transaction_id = rand::random::<u32>();
        let request = {
            let mut buffer = Vec::with_capacity(ConnectRequest::MIN_LENGTH);
            ConnectRequest.encode(0, transaction_id, &mut buffer);
            buffer
        };

        log::trace!("Sending connect request to {}", self.remote_addr);

        let connect_response =
            Self::do_request(&self.socket, request, |data: &[u8]| -> Option<ConnectResponse> {
                match parse_response(data, transaction_id, &self.remote_addr.ip()) {
                    Ok(AnyResponse::Connect(connect)) => Some(connect),
                    _ => None,
                }
            })
            .await?;

        log::trace!(
            "Received connect response from {}, connection_id={}",
            self.remote_addr,
            connect_response.connection_id
        );
        self.connection_id = connect_response.connection_id;
        self.connection_eof = Instant::now() + sec!(60);
        Ok(())
    }

    async fn do_request<R, F>(
        socket: &UdpSocket,
        request: Vec<u8>,
        process_response: F,
    ) -> io::Result<R>
    where
        F: Fn(&[u8]) -> Option<R>,
    {
        const MAX_RETRANSMISSIONS: usize = 3; // timeout after 225s

        let mut retransmit_n = 0usize;

        loop {
            let mut bytes_written: usize = 0;
            while bytes_written < request.len() {
                bytes_written += socket.send(&request[bytes_written..]).await?;
            }

            let timeout_sec = 15 * (1 << retransmit_n);
            let mut recv_buf = [0u8; 2048];
            match timeout(sec!(timeout_sec), socket.recv(&mut recv_buf)).await {
                Ok(read_res) => {
                    let bytes_read = read_res?;
                    if bytes_read == recv_buf.len() {
                        log::warn!("UDP tracker response buffer overflow");
                    }
                    if let Some(result) = process_response(&recv_buf[..bytes_read]) {
                        return Ok(result);
                    }
                }
                Err(_) => {
                    if retransmit_n == MAX_RETRANSMISSIONS {
                        return Err(io::Error::from(io::ErrorKind::TimedOut));
                    }
                    retransmit_n += 1;
                    log::trace!("Retrying request, retransmit_n={retransmit_n}");
                }
            }
        }
    }
}

#[derive(Debug, Display, Error, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    InvalidAction,
    InvalidLength,
    InvalidTransaction,
    NonUtf8String,
}

impl From<ParseError> for io::Error {
    fn from(e: ParseError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, Box::new(e))
    }
}

impl From<str::Utf8Error> for ParseError {
    fn from(_: Utf8Error) -> Self {
        ParseError::NonUtf8String
    }
}

enum AnyResponse {
    Connect(ConnectResponse),
    Announce(AnnounceResponse),
    Scrape(ScrapeResponse),
    Error(ErrorResponse),
}

fn parse_response(
    mut src: &[u8],
    request_transaction_id: u32,
    remote_ip: &IpAddr,
) -> Result<AnyResponse, ParseError> {
    if src.remaining() < CommonResponseHeader::LENGTH {
        return Err(ParseError::InvalidLength);
    }
    let header = CommonResponseHeader {
        action: src.get_u32(),
        transaction_id: src.get_u32(),
    };
    if header.transaction_id != request_transaction_id {
        return Err(ParseError::InvalidTransaction);
    }
    match header.action {
        ACTION_CONNECT => Ok(AnyResponse::Connect(ConnectResponse::try_from(src)?)),
        ACTION_ANNOUNCE => Ok(AnyResponse::Announce(AnnounceResponse::try_from((src, remote_ip))?)),
        ACTION_SCRAPE => Ok(AnyResponse::Scrape(ScrapeResponse::try_from(src)?)),
        ACTION_ERROR => Ok(AnyResponse::Error(ErrorResponse::try_from(src)?)),
        _ => Err(ParseError::InvalidAction),
    }
}

const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;
const ACTION_SCRAPE: u32 = 2;
const ACTION_ERROR: u32 = 3;

trait EncodableRequest {
    const MIN_LENGTH: usize;

    fn encode<B: BufMut>(&self, connection_id: u64, transaction_id: u32, dest: B);
}

// -------------------------------------------------------------------------------------------------

struct ConnectRequest;

impl EncodableRequest for ConnectRequest {
    const MIN_LENGTH: usize = 16;

    fn encode<B: BufMut>(&self, _connection_id: u64, transaction_id: u32, mut dest: B) {
        dest.put_u64(0x41727101980u64);
        dest.put_u32(ACTION_CONNECT);
        dest.put_u32(transaction_id);
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum AnnounceEvent {
    None = 0,
    Completed = 1,
    Started = 2,
    Stopped = 3,
}

#[derive(Debug, Clone)]
pub struct AnnounceRequest {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub downloaded: u64,
    pub left: u64,
    pub uploaded: u64,
    pub event: AnnounceEvent,
    pub ip: Option<Ipv4Addr>,
    pub key: u32,
    pub num_want: Option<i32>,
    pub port: u16,
}

impl EncodableRequest for AnnounceRequest {
    const MIN_LENGTH: usize = 98;

    fn encode<B: BufMut>(&self, connection_id: u64, transaction_id: u32, mut dest: B) {
        dest.put_u64(connection_id);
        dest.put_u32(ACTION_ANNOUNCE);
        dest.put_u32(transaction_id);
        dest.put_slice(&self.info_hash);
        dest.put_slice(&self.peer_id);
        dest.put_u64(self.downloaded);
        dest.put_u64(self.left);
        dest.put_u64(self.uploaded);
        dest.put_u32(self.event as u32);
        dest.put_u32(self.ip.unwrap_or(Ipv4Addr::UNSPECIFIED).to_bits());
        dest.put_u32(self.key);
        dest.put_i32(self.num_want.unwrap_or(-1));
        dest.put_u16(self.port);
    }
}

#[derive(Debug, Clone)]
pub struct ScrapeRequest {
    pub info_hashes: Vec<[u8; 20]>,
}

impl EncodableRequest for ScrapeRequest {
    const MIN_LENGTH: usize = 36;

    fn encode<B: BufMut>(&self, connection_id: u64, transaction_id: u32, mut dest: B) {
        dest.put_u64(connection_id);
        dest.put_u32(ACTION_SCRAPE);
        dest.put_u32(transaction_id);
        for info_hash in &self.info_hashes {
            dest.put_slice(info_hash);
        }
    }
}

// -------------------------------------------------------------------------------------------------

struct CommonResponseHeader {
    action: u32,
    transaction_id: u32,
}

impl CommonResponseHeader {
    const LENGTH: usize = 8;
}

struct ConnectResponse {
    connection_id: u64,
}

impl TryFrom<&[u8]> for ConnectResponse {
    type Error = ParseError;

    fn try_from(src: &[u8]) -> Result<Self, Self::Error> {
        if let Some(&data) = src.first_chunk() {
            Ok(ConnectResponse {
                connection_id: u64::from_be_bytes(data),
            })
        } else {
            Err(ParseError::InvalidLength)
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct AnnounceResponse {
    pub interval: u32,
    pub leechers: u32,
    pub seeders: u32,
    pub ips: Vec<SocketAddr>,
}

impl TryFrom<(&[u8], &IpAddr)> for AnnounceResponse {
    type Error = ParseError;

    fn try_from((mut src, tracker_ip): (&[u8], &IpAddr)) -> Result<Self, Self::Error> {
        if src.remaining() < 12 {
            Err(ParseError::InvalidLength)
        } else {
            let interval = src.get_u32();
            let leechers = src.get_u32();
            let seeders = src.get_u32();
            let ips: Vec<_> = match tracker_ip {
                IpAddr::V4(_) => ip::SocketAddrV4BytesIter(src).map(SocketAddr::V4).collect(),
                IpAddr::V6(_) => ip::SocketAddrV6BytesIter(src).map(SocketAddr::V6).collect(),
            };
            Ok(AnnounceResponse {
                interval,
                leechers,
                seeders,
                ips,
            })
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ScrapeResponseEntry {
    pub seeders: u32,
    pub completed: u32,
    pub leechers: u32,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ScrapeResponse(pub Vec<ScrapeResponseEntry>);

impl TryFrom<&[u8]> for ScrapeResponse {
    type Error = ParseError;

    fn try_from(src: &[u8]) -> Result<Self, Self::Error> {
        fn to_scrape_entry(mut src: &[u8]) -> ScrapeResponseEntry {
            assert!(src.remaining() == 12);
            let seeders = src.get_u32();
            let completed = src.get_u32();
            let leechers = src.get_u32();
            ScrapeResponseEntry {
                seeders,
                completed,
                leechers,
            }
        }
        Ok(ScrapeResponse(src.chunks_exact(12).map(to_scrape_entry).collect()))
    }
}

struct ErrorResponse {
    message: String,
}

impl TryFrom<&[u8]> for ErrorResponse {
    type Error = ParseError;

    fn try_from(src: &[u8]) -> Result<Self, Self::Error> {
        Ok(ErrorResponse {
            message: str::from_utf8(src)?.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv6Addr, SocketAddrV4};
    use std::{cell::Cell, rc::Rc};
    use tokio::join;

    #[test]
    fn test_serialize_connect_request() {
        let mut request = Vec::with_capacity(ConnectRequest::MIN_LENGTH);
        let cr = ConnectRequest {};
        cr.encode(0, 15, &mut request);

        assert_eq!([0x00, 0x00, 0x04, 0x17, 0x27, 0x10, 0x19, 0x80], request[..8]);
        assert_eq!([0u8; 4], request[8..12]);
        assert_eq!([0x00, 0x00, 0x00, 0x0f], request[12..]);
    }

    #[test]
    fn test_parse_connect_response() {
        let response: [u8; 16] = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x00, 0x00, 0x04, 0x17, 0x27, 0x10,
            0x19, 0x80,
        ];

        let parsed = ConnectResponse::try_from(&response[8..]).unwrap();
        assert_eq!(0x41727101980, parsed.connection_id);
    }

    #[tokio::test]
    async fn test_connect_request_response() {
        let server_addr = "localhost:6882";
        let client_addr = "localhost:6883";

        let server_socket = UdpSocket::bind(server_addr).await.unwrap();
        server_socket.connect(client_addr).await.unwrap();

        let client_socket = UdpSocket::bind(client_addr).await.unwrap();
        client_socket.connect(server_addr).await.unwrap();

        let client_fut = async {
            TrackerConnection::from_connected_socket(client_socket).await.unwrap();
        };

        let server_fut = async {
            let mut recv_buffer = [0u8; 1024];

            let bytes_read = server_socket.recv(&mut recv_buffer).await.unwrap();
            assert_eq!(16, bytes_read);

            let request = &recv_buffer[..16];
            assert_eq!([0x00, 0x00, 0x04, 0x17, 0x27, 0x10, 0x19, 0x80], request[..8]);
            assert_eq!([0u8; 4], request[8..12]);

            let response: [u8; 16] = {
                let transaction_id = &request[12..];
                let mut response: [u8; 16] = [
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x00, 0x00, 0x04, 0x17, 0x27,
                    0x10, 0x19, 0x80,
                ];
                response[4..8].copy_from_slice(transaction_id);
                response
            };

            server_socket.send(&response).await.unwrap();
        };

        join!(client_fut, server_fut);
    }

    #[test]
    fn test_serialize_announce_request() {
        let mut request = Vec::with_capacity(AnnounceRequest::MIN_LENGTH);
        let ar = AnnounceRequest {
            info_hash: [42u8; 20],
            peer_id: [b'm'; 20],
            downloaded: 1,
            left: 2,
            uploaded: 3,
            event: AnnounceEvent::Started,
            ip: None,
            key: 0,
            num_want: Some(256),
            port: 6889u16,
        };
        ar.encode(10, 15, &mut request);
        assert_eq!(request.len(), 98);

        assert_eq!([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a], request[..8]); // connection id
        assert_eq!([0x00, 0x00, 0x00, 0x01], request[8..12]); // announce
        assert_eq!([0x00, 0x00, 0x00, 0x0f], request[12..16]); // transaction id
        assert_eq!([42u8; 20], request[16..36]); // info hash
        assert_eq!([b'm'; 20], request[36..56]); // peer id
        assert_eq!([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01], request[56..64]); // downloaded
        assert_eq!([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02], request[64..72]); // left
        assert_eq!([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03], request[72..80]); // uploaded
        assert_eq!([0x00, 0x00, 0x00, 0x02], request[80..84]); // event
        assert_eq!([0u8; 4], request[84..88]); // ip
        assert_eq!([0u8; 4], request[88..92]); // key
        assert_eq!([0x00, 0x00, 0x01, 0x00], request[92..96]); // num want
        assert_eq!([0x1a, 0xe9], request[96..98]);
    }

    #[test]
    fn test_parse_ipv4_announce_response() {
        let response = [
            0x00, 0x00, 0x00, 0x01, // action
            0x00, 0x00, 0x00, 0x0f, // transaction id
            0x00, 0x00, 0x07, 0x08, // interval
            0x00, 0x00, 0x00, 0x01, // leechers
            0x00, 0x00, 0x00, 0x02, // seeders
            0xc0, 0xa8, 0x01, 0x01, // ip
            0x1a, 0xe9, // port
            0xc0, 0xa8, 0x00, 0x01, // ip
            0x1a, 0xe8, // port
        ];

        let parsed = parse_response(&response, 15, &Ipv4Addr::LOCALHOST.into()).unwrap();
        let parsed = match parsed {
            AnyResponse::Announce(response) => response,
            _ => panic!("Wrong message type"),
        };
        assert_eq!(1800, parsed.interval);
        assert_eq!(1, parsed.leechers);
        assert_eq!(2, parsed.seeders);
        assert_eq!(2, parsed.ips.len());
        assert_eq!("192.168.1.1:6889".parse::<SocketAddr>().unwrap(), parsed.ips[0]);
        assert_eq!("192.168.0.1:6888".parse::<SocketAddr>().unwrap(), parsed.ips[1]);
    }

    #[test]
    fn test_parse_ipv6_announce_response() {
        let response = [
            0x00, 0x00, 0x00, 0x01, // action
            0x00, 0x00, 0x00, 0x0f, // transaction id
            0x00, 0x00, 0x07, 0x08, // interval
            0x00, 0x00, 0x00, 0x01, // leechers
            0x00, 0x00, 0x00, 0x02, // seeders
            0x20, 0x01, 0x0d, 0xb8, // ip1
            0x85, 0xa3, 0x00, 0x00, // ip1
            0x8a, 0x2e, 0x03, 0x70, // ip1
            0x73, 0x34, 0x00, 0x00, // ip1
            0x1a, 0xe9, // port
            0x20, 0x02, 0x0d, 0xb8, // ip2
            0x85, 0xa3, 0x00, 0x00, // ip2
            0x8a, 0x2e, 0x03, 0x70, // ip2
            0x73, 0x35, 0x00, 0x00, // ip2
            0x1a, 0xe8, // port
        ];

        let parsed = parse_response(&response, 15, &Ipv6Addr::LOCALHOST.into()).unwrap();
        let parsed = match parsed {
            AnyResponse::Announce(response) => response,
            _ => panic!("Wrong message type"),
        };
        assert_eq!(1800, parsed.interval);
        assert_eq!(1, parsed.leechers);
        assert_eq!(2, parsed.seeders);
        assert_eq!(2, parsed.ips.len());
        assert_eq!(
            "[2001:db8:85a3:0:8a2e:370:7334:0]:6889".parse::<SocketAddr>().unwrap(),
            parsed.ips[0]
        );
        assert_eq!(
            "[2002:db8:85a3:0:8a2e:370:7335:0]:6888".parse::<SocketAddr>().unwrap(),
            parsed.ips[1]
        );
    }

    #[test]
    fn test_parse_error_response() {
        let response = [
            0x00, 0x00, 0x00, 0x03, // action
            0x00, 0x00, 0x00, 0x0f, // transaction id
            0x49, 0x6E, 0x76, 0x61, 0x6C, 0x69, 0x64,
        ];

        let parsed = parse_response(&response, 15, &Ipv6Addr::LOCALHOST.into()).unwrap();
        let parsed = match parsed {
            AnyResponse::Error(response) => response,
            _ => panic!("Wrong message type"),
        };
        assert_eq!("Invalid", parsed.message);
    }

    #[ignore]
    #[tokio::test]
    async fn test_udp_announce() {
        let udp_tracker_addrs = ["open.stealth.si:80", "tracker.opentrackr.org:1337"];

        let local_ip = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 6666));

        let announce_request = AnnounceRequest {
            info_hash: [
                30, 189, 61, 191, 187, 37, 193, 51, 63, 81, 201, 156, 126, 230, 112, 252, 42, 23,
                39, 201,
            ],
            peer_id: [b'm'; 20],
            downloaded: 0,
            left: 200,
            uploaded: 0,
            event: AnnounceEvent::None,
            ip: None,
            key: 0,
            num_want: Some(5),
            port: local_ip.port(),
        };

        for tracker_addr in udp_tracker_addrs {
            let client_socket = UdpSocket::bind(local_ip).await.unwrap();
            client_socket
                .connect(&tracker_addr)
                .await
                .unwrap_or_else(|e| panic!("Failed to connect to {}: {}", &tracker_addr, e));

            let mut client = TrackerConnection::from_connected_socket(client_socket).await.unwrap();

            let response = client
                .do_announce_request(announce_request.clone())
                .await
                .unwrap_or_else(|e| panic!("Announce error: {e}"));

            println!("Announce response: {response:?}");
            assert!(response.interval > 0);
            assert!(response.leechers > 0 || response.seeders > 0 || response.ips.is_empty());
        }
    }

    #[ignore]
    #[tokio::test]
    async fn test_udp_scrape() {
        let udp_tracker_addrs = ["open.stealth.si:80", "tracker.opentrackr.org:1337"];

        let success_count = Rc::new(Cell::new(0usize));
        let local_set = tokio::task::LocalSet::new();

        for (i, tracker_addr) in udp_tracker_addrs.iter().map(ToOwned::to_owned).enumerate() {
            let success_count = success_count.clone();
            local_set.spawn_local(async move {
                let bind_addr =
                    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 6666 + i as u16));
                let socket = UdpSocket::bind(bind_addr).await.unwrap();
                if socket.connect(&tracker_addr).await.is_err() {
                    return;
                }

                match tokio::time::timeout(
                    sec!(10),
                    TrackerConnection::from_connected_socket(socket),
                )
                .await
                {
                    Ok(Ok(mut client)) => {
                        let response = client
                            .do_scrape_request(ScrapeRequest {
                                info_hashes: vec![[
                                    30, 189, 61, 191, 187, 37, 193, 51, 63, 81, 201, 156, 126, 230,
                                    112, 252, 42, 23, 39, 201,
                                ]],
                            })
                            .await
                            .unwrap();
                        println!("Response from {tracker_addr}: {response:?}");
                        success_count.set(success_count.get() + 1);
                    }
                    _ => {
                        eprintln!("Failed to connect to {tracker_addr}");
                    }
                }
            });
        }
        local_set.await;
        assert!(success_count.get() > 0);
    }
}
