use crate::tracker::utils;
use std::net::{Ipv4Addr, SocketAddr};
use std::str::Utf8Error;
use std::time::{Duration, Instant};
use std::{io, str};
use tokio::net::UdpSocket;
use tokio::time::timeout;

pub struct UdpTrackerConnection {
    socket: UdpSocket,
    connection_id: u64,
    connection_eof: Instant,
}

impl UdpTrackerConnection {
    pub async fn from_connected_socket(socket: UdpSocket) -> io::Result<Self> {
        let transaction_id = rand::random::<u32>();
        let request = {
            let mut buffer = Vec::with_capacity(ConnectRequest::MIN_LENGTH);
            ConnectRequest.encode(0, transaction_id, &mut buffer).unwrap();
            buffer
        };

        log::trace!("Sending connect request");

        let connect_response =
            Self::do_request(&socket, request, |data: &[u8]| -> Option<ConnectResponse> {
                match parse_response(data, transaction_id) {
                    Ok(AnyResponse::Connect(connect)) => Some(connect),
                    _ => None,
                }
            })
            .await?;

        log::trace!("Received connect response, connection_id={}", connect_response.connection_id);

        Ok(UdpTrackerConnection {
            socket,
            connection_id: connect_response.connection_id,
            connection_eof: Instant::now() + Duration::from_secs(60),
        })
    }

    pub async fn from_expired_connection(old: UdpTrackerConnection) -> io::Result<Self> {
        assert!(old.expired());
        Self::from_connected_socket(old.socket).await
    }

    pub fn expired(&self) -> bool {
        self.connection_eof <= Instant::now()
    }

    pub async fn do_announce_request(
        &self,
        request_data: AnnounceRequest,
    ) -> io::Result<AnnounceResponse> {
        assert!(!self.expired());

        let transaction_id = rand::random::<u32>();
        let request = {
            let mut buffer = Vec::with_capacity(AnnounceRequest::MIN_LENGTH);
            request_data.encode(self.connection_id, transaction_id, &mut buffer).unwrap();
            buffer
        };

        log::trace!("Sending announce request");

        Self::do_request(
            &self.socket,
            request,
            |data: &[u8]| -> Option<io::Result<AnnounceResponse>> {
                if self.expired() {
                    return Some(Err(io::Error::from(io::ErrorKind::TimedOut)));
                }

                match parse_response(data, transaction_id) {
                    Ok(AnyResponse::Announce(announce)) => {
                        log::debug!("Received announce response: {:?}", announce);
                        Some(Ok(announce))
                    }
                    Ok(AnyResponse::Error(error)) => {
                        Some(Err(io::Error::new(io::ErrorKind::Other, error.message)))
                    }
                    _ => None,
                }
            },
        )
        .await?
    }

    pub async fn do_scrape_request(
        &self,
        request_data: ScrapeRequest,
    ) -> io::Result<ScrapeResponse> {
        assert!(!self.expired());

        let transaction_id = rand::random::<u32>();
        let request = {
            let mut buffer = Vec::with_capacity(ScrapeRequest::MIN_LENGTH);
            request_data.encode(self.connection_id, transaction_id, &mut buffer).unwrap();
            buffer
        };
        Self::do_request(
            &self.socket,
            request,
            |data: &[u8]| -> Option<io::Result<ScrapeResponse>> {
                if self.expired() {
                    return Some(Err(io::Error::from(io::ErrorKind::TimedOut)));
                }

                match parse_response(data, transaction_id) {
                    Ok(AnyResponse::Scrape(scrape)) => {
                        log::debug!("Received scrape response: {:?}", scrape);
                        Some(Ok(scrape))
                    }
                    Ok(AnyResponse::Error(error)) => {
                        Some(Err(io::Error::new(io::ErrorKind::Other, error.message)))
                    }
                    _ => None,
                }
            },
        )
        .await?
    }

    async fn do_request<R, F>(
        socket: &UdpSocket,
        request: Vec<u8>,
        process_response: F,
    ) -> io::Result<R>
    where
        F: Fn(&[u8]) -> Option<R>,
    {
        let mut retransmit_n = 0usize;

        loop {
            let mut bytes_written: usize = 0;
            while bytes_written < request.len() {
                bytes_written = socket.send(&request[bytes_written..]).await?;
            }

            let timeout_sec = 15 * (1 << retransmit_n);

            let mut recv_buf = [0u8; 1024];
            let receive_fut = async { socket.recv(&mut recv_buf).await };

            match timeout(Duration::from_secs(timeout_sec), receive_fut).await {
                Ok(read_res) => {
                    let bytes_read = read_res?;
                    if let Some(result) = process_response(&recv_buf[..bytes_read]) {
                        return Ok(result);
                    }
                }
                Err(_) => {
                    if retransmit_n == 8 {
                        return Err(io::Error::from(io::ErrorKind::TimedOut));
                    }
                    retransmit_n += 1;
                    log::trace!("Retrying request, retransmit_n={}", retransmit_n);
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum ParseError {
    InvalidAction,
    InvalidLength,
    InvalidTransaction,
    NonUtf8String,
}

impl From<ParseError> for io::Error {
    fn from(e: ParseError) -> Self {
        io::Error::new(io::ErrorKind::Other, format!("{:?}", e))
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

fn parse_response(src: &[u8], transaction_id: u32) -> Result<AnyResponse, ParseError> {
    let header = CommonResponseHeader::try_from(src)?;
    if header.transaction_id != transaction_id {
        return Err(ParseError::InvalidTransaction);
    }
    let src = unsafe { src.get_unchecked(CommonResponseHeader::LENGTH..) };
    match header.action {
        ACTION_CONNECT => Ok(AnyResponse::Connect(ConnectResponse::try_from(src)?)),
        ACTION_ANNOUNCE => Ok(AnyResponse::Announce(AnnounceResponse::try_from(src)?)),
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

    fn encode(
        &self,
        connection_id: u64,
        transaction_id: u32,
        dest: &mut dyn io::Write,
    ) -> io::Result<()>;
}

// -------------------------------------------------------------------------------------------------

struct ConnectRequest;

impl EncodableRequest for ConnectRequest {
    const MIN_LENGTH: usize = 16;

    fn encode(
        &self,
        _connection_id: u64,
        transaction_id: u32,
        dest: &mut dyn io::Write,
    ) -> io::Result<()> {
        dest.write_all(&0x41727101980u64.to_be_bytes())?;
        dest.write_all(&ACTION_CONNECT.to_be_bytes())?;
        dest.write_all(&transaction_id.to_be_bytes())?;
        Ok(())
    }
}

#[derive(Copy, Clone)]
pub enum AnnounceEvent {
    None = 0,
    Completed = 1,
    Started = 2,
    Stopped = 3,
}

#[derive(Clone)]
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

    fn encode(
        &self,
        connection_id: u64,
        transaction_id: u32,
        dest: &mut dyn io::Write,
    ) -> io::Result<()> {
        dest.write_all(&connection_id.to_be_bytes())?;
        dest.write_all(&ACTION_ANNOUNCE.to_be_bytes())?;
        dest.write_all(&transaction_id.to_be_bytes())?;
        dest.write_all(&self.info_hash)?;
        dest.write_all(&self.peer_id)?;
        dest.write_all(&self.downloaded.to_be_bytes())?;
        dest.write_all(&self.left.to_be_bytes())?;
        dest.write_all(&self.uploaded.to_be_bytes())?;
        dest.write_all(&(self.event as u32).to_be_bytes())?;
        dest.write_all(&self.ip.unwrap_or(Ipv4Addr::UNSPECIFIED).octets())?;
        dest.write_all(&self.key.to_be_bytes())?;
        dest.write_all(&self.num_want.unwrap_or(-1).to_be_bytes())?;
        dest.write_all(&self.port.to_be_bytes())?;
        Ok(())
    }
}

pub struct ScrapeRequest {
    pub info_hashes: Vec<[u8; 20]>,
}

impl EncodableRequest for ScrapeRequest {
    const MIN_LENGTH: usize = 36;

    fn encode(
        &self,
        connection_id: u64,
        transaction_id: u32,
        dest: &mut dyn io::Write,
    ) -> io::Result<()> {
        dest.write_all(&connection_id.to_be_bytes())?;
        dest.write_all(&ACTION_SCRAPE.to_be_bytes())?;
        dest.write_all(&transaction_id.to_be_bytes())?;
        for hash in &self.info_hashes {
            dest.write_all(&hash[..])?;
        }
        Ok(())
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

impl TryFrom<&[u8]> for CommonResponseHeader {
    type Error = ParseError;

    fn try_from(src: &[u8]) -> Result<Self, Self::Error> {
        if let Some(data) = src.get(..8) {
            let (action_bytes, transaction_id_bytes) = data.split_at(4);
            Ok(unsafe {
                CommonResponseHeader {
                    action: u32_from_be_bytes(action_bytes),
                    transaction_id: u32_from_be_bytes(transaction_id_bytes),
                }
            })
        } else {
            Err(ParseError::InvalidLength)
        }
    }
}

struct ConnectResponse {
    connection_id: u64,
}

impl TryFrom<&[u8]> for ConnectResponse {
    type Error = ParseError;

    fn try_from(src: &[u8]) -> Result<Self, Self::Error> {
        if let Some(data) = src.get(..8) {
            Ok(unsafe {
                ConnectResponse {
                    connection_id: u64_from_be_bytes(data),
                }
            })
        } else {
            Err(ParseError::InvalidLength)
        }
    }
}

#[derive(Debug)]
pub struct AnnounceResponse {
    pub interval: u32,
    pub leechers: u32,
    pub seeders: u32,
    pub ips: Vec<SocketAddr>,
}

impl TryFrom<&[u8]> for AnnounceResponse {
    type Error = ParseError;

    fn try_from(src: &[u8]) -> Result<Self, Self::Error> {
        if src.len() < 12 {
            Err(ParseError::InvalidLength)
        } else {
            let (src, addrs) = src.split_at(12);
            Ok(unsafe {
                AnnounceResponse {
                    interval: u32_from_be_bytes(src.get_unchecked(..4)),
                    leechers: u32_from_be_bytes(src.get_unchecked(4..8)),
                    seeders: u32_from_be_bytes(src.get_unchecked(8..12)),
                    ips: utils::parse_binary_ipv4_peers(addrs).collect(),
                }
            })
        }
    }
}

#[derive(Debug)]
pub struct ScrapeResponseEntry {
    pub seeders: u32,
    pub completed: u32,
    pub leechers: u32,
}

#[derive(Debug)]
pub struct ScrapeResponse(Vec<ScrapeResponseEntry>);

impl TryFrom<&[u8]> for ScrapeResponse {
    type Error = ParseError;

    fn try_from(src: &[u8]) -> Result<Self, Self::Error> {
        fn to_scrape_entry(src: &[u8]) -> ScrapeResponseEntry {
            assert!(src.len() >= 12);
            unsafe {
                ScrapeResponseEntry {
                    seeders: u32_from_be_bytes(src.get_unchecked(..4)),
                    completed: u32_from_be_bytes(src.get_unchecked(4..8)),
                    leechers: u32_from_be_bytes(src.get_unchecked(8..12)),
                }
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

unsafe fn u32_from_be_bytes(src: &[u8]) -> u32 {
    let mut dest = [0u8; 4];
    dest.copy_from_slice(src.get_unchecked(..4));
    u32::from_be_bytes(dest)
}

unsafe fn u64_from_be_bytes(src: &[u8]) -> u64 {
    let mut dest = [0u8; 8];
    dest.copy_from_slice(src.get_unchecked(..8));
    u64::from_be_bytes(dest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::join;

    #[test]
    fn test_serialize_connect_request() {
        let mut request = Vec::with_capacity(ConnectRequest::MIN_LENGTH);
        let cr = ConnectRequest {};
        cr.encode(0, 15, &mut request).unwrap();

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
            UdpTrackerConnection::from_connected_socket(client_socket).await.unwrap();
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
}
