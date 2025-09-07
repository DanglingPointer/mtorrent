mod http;
mod udp;
mod url;

use futures_util::TryFutureExt;
use local_async_utils::sec;
use mtorrent_utils::peer_id::PeerId;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::time::Duration;
use std::{io, net::SocketAddr};
use tokio::net::{self, UdpSocket};
use tokio::sync::{mpsc, oneshot};
use tokio::task;
use tokio_util::sync::CancellationToken;

pub use url::TrackerUrl;

#[derive(Debug, Clone, Copy)]
pub enum AnnounceEvent {
    Started,
    Stopped,
    Completed,
}

#[derive(Debug)]
pub struct AnnounceRequest {
    pub info_hash: [u8; 20],
    pub downloaded: usize,
    pub left: usize,
    pub uploaded: usize,
    pub local_peer_id: PeerId,
    pub listener_port: u16,
    pub event: Option<AnnounceEvent>,
    pub num_want: usize,
}

#[derive(Debug)]
pub struct AnnounceResponse {
    pub interval: Duration,
    pub peers: Vec<SocketAddr>,
}

#[derive(Debug)]
pub struct ScrapeRequest {
    pub info_hashes: Vec<[u8; 20]>,
}

#[derive(Debug)]
pub struct ScrapeResponseEntry {
    pub seeders: usize,
    pub completed: usize,
    pub leechers: usize,
}

#[derive(Debug)]
pub struct ScrapeResponse(pub Vec<ScrapeResponseEntry>);

struct Request<Request, Response> {
    url: TrackerUrl,
    data: Request,
    responder: oneshot::Sender<io::Result<Response>>,
}

enum Command {
    Announce(Request<AnnounceRequest, AnnounceResponse>),
    Scrape(Request<ScrapeRequest, ScrapeResponse>),
}

pub fn init() -> (TrackerClient, TrackerManager) {
    let (cmd_sender, cmd_receiver) = mpsc::channel(128);
    (TrackerClient { cmd_sender }, TrackerManager { cmd_receiver })
}

#[derive(Clone)]
pub struct TrackerClient {
    cmd_sender: mpsc::Sender<Command>,
}

impl TrackerClient {
    pub async fn announce(
        &self,
        url: TrackerUrl,
        data: AnnounceRequest,
    ) -> io::Result<AnnounceResponse> {
        let (tx, rx) = oneshot::channel();
        self.cmd_sender
            .send(Command::Announce(Request {
                url,
                data,
                responder: tx,
            }))
            .await
            .map_err(Self::to_broken_pipe_error)?;
        rx.await.map_err(Self::to_broken_pipe_error)?
    }

    pub async fn scrape(&self, url: TrackerUrl, data: ScrapeRequest) -> io::Result<ScrapeResponse> {
        let (tx, rx) = oneshot::channel();
        self.cmd_sender
            .send(Command::Scrape(Request {
                url,
                data,
                responder: tx,
            }))
            .await
            .map_err(Self::to_broken_pipe_error)?;
        rx.await.map_err(Self::to_broken_pipe_error)?
    }

    fn to_broken_pipe_error<T>(_: T) -> io::Error {
        io::Error::from(io::ErrorKind::BrokenPipe)
    }
}

impl From<AnnounceEvent> for http::AnnounceEvent {
    fn from(event: AnnounceEvent) -> Self {
        match event {
            AnnounceEvent::Started => http::AnnounceEvent::Started,
            AnnounceEvent::Stopped => http::AnnounceEvent::Stopped,
            AnnounceEvent::Completed => http::AnnounceEvent::Completed,
        }
    }
}

impl From<AnnounceEvent> for udp::AnnounceEvent {
    fn from(event: AnnounceEvent) -> Self {
        match event {
            AnnounceEvent::Started => udp::AnnounceEvent::Started,
            AnnounceEvent::Stopped => udp::AnnounceEvent::Stopped,
            AnnounceEvent::Completed => udp::AnnounceEvent::Completed,
        }
    }
}

pub struct TrackerManager {
    cmd_receiver: mpsc::Receiver<Command>,
}

impl TrackerManager {
    pub async fn run(mut self) {
        let canceller = CancellationToken::new();

        macro_rules! spawn_child_task {
            ($fut:expr) => {{
                task::spawn(canceller.clone().run_until_cancelled_owned($fut));
            }};
        }

        let http_client = http::TrackerClient::new()
            .inspect_err(|e| log::error!("Failed to create HTTP tracker client: {e}"))
            .ok();

        while let Some(cmd) = self.cmd_receiver.recv().await {
            match cmd {
                Command::Announce(request) => match request.url {
                    TrackerUrl::Http(url) => {
                        if let Some(client) = http_client.clone() {
                            spawn_child_task!(async move {
                                let result = do_http_announce(&client, &url, request.data).await;
                                _ = request.responder.send(result).inspect_err(|_| {
                                    log::warn!("Failed to send back http announce result")
                                });
                            });
                        } else {
                            log::debug!("Not doing HTTP announce - no client");
                        }
                    }
                    TrackerUrl::Udp(addr) => {
                        spawn_child_task!(async move {
                            let result = do_udp_announce(&addr, request.data).await;
                            _ = request.responder.send(result).inspect_err(|_| {
                                log::warn!("Failed to send back udp announce result")
                            });
                        });
                    }
                },
                Command::Scrape(request) => {
                    _ = request.responder.send(Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "scrape is not implemented",
                    )));
                }
            }
        }
        canceller.cancel();
    }
}

async fn do_http_announce(
    client: &http::TrackerClient,
    url: &str,
    data: AnnounceRequest,
) -> io::Result<AnnounceResponse> {
    let mut request = http::TrackerRequestBuilder::try_from(url)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, Box::new(e)))?;
    request
        .info_hash(&data.info_hash)
        .peer_id(data.local_peer_id.as_slice())
        .bytes_downloaded(data.downloaded)
        .bytes_left(data.left)
        .bytes_uploaded(data.uploaded)
        .numwant(data.num_want)
        .compact_support()
        .no_peer_id()
        .port(data.listener_port);
    if let Some(event) = data.event {
        request.event(event.into());
    }
    let response = client.announce(request).await?;

    let interval_sec = response
        .interval()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no interval in response"))?;
    let mut peers = response
        .peers()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no peers in response"))?;
    peers.retain(|peer_ip| !peer_ip.ip().is_unspecified());
    Ok(AnnounceResponse {
        interval: sec!(interval_sec as u64),
        peers,
    })
}

async fn do_udp_announce(
    tracker_addr: &str,
    data: AnnounceRequest,
) -> io::Result<AnnounceResponse> {
    async fn new_udp_client(tracker_addr_str: &str) -> io::Result<udp::TrackerConnection> {
        async fn bind_and_connect_socket(
            bind_addr: &SocketAddr,
            remote_addr: &SocketAddr,
        ) -> io::Result<UdpSocket> {
            let socket = UdpSocket::bind(bind_addr).await?;
            socket.connect(&remote_addr).await?;
            Ok(socket)
        }

        for tracker_addr in net::lookup_host(tracker_addr_str).await? {
            let local_ip = match &tracker_addr {
                SocketAddr::V4(_) => Ipv4Addr::UNSPECIFIED.into(),
                SocketAddr::V6(_) => Ipv6Addr::UNSPECIFIED.into(),
            };
            let local_addr = SocketAddr::new(local_ip, 0);
            if let Ok(client) = bind_and_connect_socket(&local_addr, &tracker_addr)
                .and_then(udp::TrackerConnection::from_connected_socket)
                .await
            {
                return Ok(client);
            }
        }
        Err(io::Error::new(io::ErrorKind::ConnectionRefused, "failed to connect to tracker"))
    }

    let mut client = new_udp_client(tracker_addr).await?;

    let request = udp::AnnounceRequest {
        info_hash: data.info_hash,
        peer_id: *data.local_peer_id,
        downloaded: data.downloaded as u64,
        left: data.left as u64,
        uploaded: data.uploaded as u64,
        event: data.event.map(Into::into).unwrap_or(udp::AnnounceEvent::None),
        ip: None,
        key: 0,
        num_want: Some(data.num_want as i32),
        port: data.listener_port,
    };
    let mut response = client.do_announce_request(request).await?;
    response.ips.retain(|peer_ip| !peer_ip.ip().is_unspecified());

    Ok(AnnounceResponse {
        interval: sec!(response.interval as u64),
        peers: response.ips,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time;

    fn udp_connect_response(transaction_id: &[u8]) -> [u8; 16] {
        let mut response: [u8; 16] = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x00, 0x00, 0x04, 0x17, 0x27, 0x10,
            0x19, 0x80,
        ];
        response[4..8].copy_from_slice(transaction_id);
        response
    }

    fn udp_announce_response(transaction_id: &[u8]) -> [u8; 32] {
        let mut response = [
            0x00, 0x00, 0x00, 0x01, // action
            0x00, 0x00, 0x00, 0x00, // transaction id
            0x00, 0x00, 0x07, 0x08, // interval
            0x00, 0x00, 0x00, 0x01, // leechers
            0x00, 0x00, 0x00, 0x02, // seeders
            0xc0, 0xa8, 0x01, 0x01, // ip
            0x1a, 0xe9, // port
            0xc0, 0xa8, 0x00, 0x01, // ip
            0x1a, 0xe8, // port
        ];
        response[4..8].copy_from_slice(transaction_id);
        response
    }

    #[tokio::test]
    async fn test_http_announce_success() {
        let (client, mgr) = init();
        task::spawn(mgr.run());
        let mut server = mockito::Server::new_async().await;

        let mock = server
                .mock("GET", "/announce")
                .with_status(200)
                .with_body(b"d8:intervali1800e5:peersld2:ip9:127.0.0.14:porti50000eed2:ip9:127.0.0.14:porti50049eeee")
                .match_query("info_hash=hhhhhhhhhhhhhhhhhhhh&peer_id=mmmmmmmmmmmmmmmmmmmm&downloaded=0&left=100&uploaded=0&numwant=50&compact=1&no_peer_id=1&port=123")
                .create_async()
                .await;

        let response = client
            .announce(
                format!("{}/announce", server.url()).parse().unwrap(),
                AnnounceRequest {
                    info_hash: [b'h'; 20],
                    downloaded: 0,
                    left: 100,
                    uploaded: 0,
                    local_peer_id: [b'm'; 20].into(),
                    listener_port: 123,
                    event: None,
                    num_want: 50,
                },
            )
            .await
            .unwrap();

        assert_eq!(response.interval, sec!(1800));
        assert_eq!(
            response.peers,
            vec![
                "127.0.0.1:50000".parse().unwrap(),
                "127.0.0.1:50049".parse().unwrap()
            ]
        );
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_http_announce_failure() {
        let (client, mgr) = init();
        task::spawn(mgr.run());
        let mut server = mockito::Server::new_async().await;

        let mock = server
                .mock("GET", "/announce")
                .with_status(500)
                .match_query("info_hash=gggggggggggggggggggg&peer_id=iiiiiiiiiiiiiiiiiiii&downloaded=0&left=100&uploaded=0&numwant=5&compact=1&no_peer_id=1&port=6881")
                .create_async()
                .await;

        let err_response = client
            .announce(
                format!("{}/announce", server.url()).parse().unwrap(),
                AnnounceRequest {
                    info_hash: [b'g'; 20],
                    downloaded: 0,
                    left: 100,
                    uploaded: 0,
                    local_peer_id: [b'i'; 20].into(),
                    listener_port: 6881,
                    event: None,
                    num_want: 5,
                },
            )
            .await
            .unwrap_err();

        assert_eq!(err_response.kind(), io::ErrorKind::UnexpectedEof);
        assert!(err_response.to_string().contains("500"), "{err_response}");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_udp_tracker_announce() {
        let (client, mgr) = init();
        task::spawn(mgr.run());

        let server_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let tracker_addr = server_socket.local_addr().unwrap();

        let client_task = task::spawn(async move {
            client
                .announce(
                    format!("udp://{tracker_addr}").parse().unwrap(),
                    AnnounceRequest {
                        info_hash: [b'g'; 20],
                        downloaded: 0,
                        left: 100,
                        uploaded: 0,
                        local_peer_id: [b'i'; 20].into(),
                        listener_port: 6881,
                        event: None,
                        num_want: 5,
                    },
                )
                .await
        });

        let mut recv_buffer = [0u8; 1024];
        let (bytes_read, client_addr) =
            time::timeout(sec!(5), server_socket.recv_from(&mut recv_buffer))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(bytes_read, 16);
        let connect_request = &recv_buffer[..16];

        let connect_response = udp_connect_response(&connect_request[12..]);
        server_socket.send_to(&connect_response, client_addr).await.unwrap();

        let (bytes_read, _) = time::timeout(sec!(5), server_socket.recv_from(&mut recv_buffer))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bytes_read, 98);
        let announce_request = &recv_buffer[..98];

        let announce_response = udp_announce_response(&announce_request[12..16]);
        server_socket.send_to(&announce_response, client_addr).await.unwrap();

        let result = time::timeout(sec!(5), client_task).await.unwrap().unwrap();
        let response = result.unwrap();

        assert_eq!(response.interval, sec!(1800));
        assert_eq!(
            response.peers,
            vec![
                "192.168.1.1:6889".parse().unwrap(),
                "192.168.0.1:6888".parse().unwrap()
            ]
        );
    }
}
