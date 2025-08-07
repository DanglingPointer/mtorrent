use super::error::Error as DhtError;
use super::msgs::Message;
use mtorrent_utils::benc;
use mtorrent_utils::debug_stopwatch;
use std::future::Future;
use std::io;
use std::mem::MaybeUninit;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::io::ReadBuf;
use tokio::net::UdpSocket;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

pub struct Runner {
    socket: UdpSocket,
    ingress_sender: mpsc::Sender<(Message, SocketAddr)>,
    egress_receiver: mpsc::Receiver<(Message, SocketAddr)>,
}

pub type MessageChannelSender = mpsc::Sender<(Message, SocketAddr)>;
pub type MessageChannelReceiver = mpsc::Receiver<(Message, SocketAddr)>;

pub async fn create_ipv4_socket(port: u16) -> io::Result<UdpSocket> {
    UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port)).await
}

pub(super) const MSG_QUEUE_LEN: usize = 512;

pub fn setup_udp(socket: UdpSocket) -> (MessageChannelSender, MessageChannelReceiver, Runner) {
    let (ingress_sender, ingress_receiver) = mpsc::channel(MSG_QUEUE_LEN);
    let (egress_sender, egress_receiver) = mpsc::channel(MSG_QUEUE_LEN);
    let actor = Runner {
        socket,
        ingress_sender,
        egress_receiver,
    };
    (egress_sender, ingress_receiver, actor)
}

impl Runner {
    pub async fn run(self) -> io::Result<()> {
        let _sw = debug_stopwatch!("UDP runner");
        let ingress = Ingress {
            socket: &self.socket,
            buffer: [MaybeUninit::uninit(); 1500],
            sink: self.ingress_sender,
        };
        let egress = Egress {
            socket: &self.socket,
            pending: None,
            source: self.egress_receiver,
        };
        select! {
            biased;
            egress_result = egress => egress_result,
            ingress_result = ingress => ingress_result,
        }
    }
}

struct Ingress<'s> {
    socket: &'s UdpSocket,
    buffer: [MaybeUninit<u8>; 1500],
    sink: mpsc::Sender<(Message, SocketAddr)>,
}

struct Egress<'s> {
    socket: &'s UdpSocket,
    pending: Option<(Vec<u8>, SocketAddr)>,
    source: mpsc::Receiver<(Message, SocketAddr)>,
}

impl<'s> Future for Ingress<'s> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        fn parse_msg(buffer: &[u8]) -> Result<Message, DhtError> {
            let (bencode, len) = benc::Element::from_bytes_with_len(buffer)?;
            if len < buffer.len() {
                log::warn!("Incoming UDP packet contains more than one message, ignoring the rest");
            }
            let message = Message::try_from(bencode)?;
            Ok(message)
        }

        let Ingress {
            socket,
            buffer,
            sink,
        } = self.get_mut();
        let mut buffer = ReadBuf::uninit(buffer);
        loop {
            buffer.clear();
            let src_addr = ready!(socket.poll_recv_from(cx, &mut buffer))
                .inspect_err(|e| log::error!("Failed to receive UDP packet: {e}"))?;
            let message = match parse_msg(buffer.filled()) {
                Err(e) => {
                    log::debug!("Failed to parse message from {src_addr}: {e:?}");
                    continue;
                }
                Ok(msg) => msg,
            };
            match sink.try_send((message, src_addr)) {
                Err(TrySendError::Closed(_)) => {
                    return Poll::Ready(Ok(()));
                }
                Err(TrySendError::Full(_)) => {
                    log::warn!("Dropping message from {src_addr}: channel is full");
                    continue;
                }
                Ok(()) => continue,
            }
        }
    }
}

impl<'s> Future for Egress<'s> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Egress {
            socket,
            pending,
            source,
        } = self.get_mut();

        loop {
            match pending {
                None => match ready!(source.poll_recv(cx)) {
                    None => {
                        return Poll::Ready(Ok(()));
                    }
                    Some((message, dst_addr)) => {
                        let bencode = benc::Element::from(message);
                        *pending = Some((bencode.to_bytes(), dst_addr));
                    }
                },
                Some((data, dest_addr)) => {
                    let data_len = data.len();
                    match ready!(socket.poll_send_to(cx, data, *dest_addr)) {
                        Err(e) => {
                            log::warn!("Failed to send UDP packet to {dest_addr}: {e}");
                        }
                        Ok(bytes_sent) if bytes_sent != data_len => {
                            log::error!(
                                "Could only send {bytes_sent}/{data_len} bytes to {dest_addr}"
                            );
                        }
                        Ok(_) => (),
                    }
                    *pending = None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{U160, msgs::*};
    use local_async_utils::prelude::*;
    use std::iter;
    use tokio::{task, time::timeout};

    #[tokio::test]
    async fn receive_single_message() {
        let sender_port = 6666u16;
        let receiver_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7777);

        let sender_sock = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, sender_port))
            .await
            .unwrap();

        let socket = create_ipv4_socket(receiver_addr.port()).await.unwrap();
        let (_tx_channel, mut rx_channel, runner) = setup_udp(socket);
        task::spawn(runner.run());

        let sent_msg = Message {
            transaction_id: vec![1, 2, 3, 4],
            version: None,
            data: MessageData::Query(
                PingArgs {
                    id: [12u8; 20].into(),
                }
                .into(),
            ),
        };
        sender_sock
            .send_to(&benc::Element::from(sent_msg).to_bytes(), receiver_addr)
            .await
            .unwrap();
        let (receved_msg, src_addr) = timeout(sec!(5), rx_channel.recv()).await.unwrap().unwrap();
        assert_eq!(src_addr, SocketAddr::new(Ipv4Addr::LOCALHOST.into(), sender_port));
        assert_eq!(receved_msg.transaction_id, vec![1, 2, 3, 4]);
        assert_eq!(receved_msg.version, None);
        let ping = match receved_msg.data {
            MessageData::Query(QueryMsg::Ping(ping)) => ping,
            _ => panic!("Expected a ping query"),
        };
        assert_eq!(ping.id, U160::from([12u8; 20]));
    }

    #[tokio::test]
    async fn receive_valid_message_after_malformed() {
        let _ = simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Info).init();

        let bad_sender_port = 6667u16;
        let good_sender_port = 6668u16;
        let receiver_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7778u16);

        let bad_sender = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, bad_sender_port))
            .await
            .unwrap();

        let good_sender = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, good_sender_port))
            .await
            .unwrap();

        let socket = create_ipv4_socket(receiver_addr.port()).await.unwrap();
        let (_tx_channel, mut rx_channel, runner) = setup_udp(socket);
        task::spawn(runner.run());

        bad_sender.send_to(b"malformed", receiver_addr).await.unwrap();

        let sent_msg = Message {
            transaction_id: vec![1, 2, 3, 4],
            version: None,
            data: MessageData::Query(
                PingArgs {
                    id: [12u8; 20].into(),
                }
                .into(),
            ),
        };
        good_sender
            .send_to(&benc::Element::from(sent_msg).to_bytes(), receiver_addr)
            .await
            .unwrap();

        let (receved_msg, src_addr) = timeout(sec!(5), rx_channel.recv()).await.unwrap().unwrap();
        assert_eq!(src_addr, SocketAddr::new(Ipv4Addr::LOCALHOST.into(), good_sender_port));
        assert_eq!(receved_msg.transaction_id, vec![1, 2, 3, 4]);
        assert_eq!(receved_msg.version, None);
        let ping = match receved_msg.data {
            MessageData::Query(QueryMsg::Ping(ping)) => ping,
            _ => panic!("Expected a ping query"),
        };
        assert_eq!(ping.id, U160::from([12u8; 20]));
    }

    #[tokio::test]
    async fn send_single_message() {
        let sender_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6669);
        let receiver_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7780);

        let receiver_sock = UdpSocket::bind(receiver_addr).await.unwrap();

        let socket = create_ipv4_socket(sender_addr.port()).await.unwrap();
        let (tx_channel, _rx_channel, runner) = setup_udp(socket);
        task::spawn(runner.run());

        tx_channel
            .send((
                Message {
                    transaction_id: Vec::from(b"aa"),
                    version: None,
                    data: MessageData::Error(ErrorMsg {
                        error_code: ErrorCode::Generic,
                        error_msg: "A Generic Error Ocurred".to_owned(),
                    }),
                },
                receiver_addr.into(),
            ))
            .await
            .unwrap();

        let mut buf = [0u8; 1500];
        let (len, src_addr) =
            timeout(sec!(5), receiver_sock.recv_from(&mut buf)).await.unwrap().unwrap();
        assert_eq!(src_addr, sender_addr.into());
        assert_eq!(&buf[..len], b"d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee");
    }

    #[tokio::test]
    async fn successful_send_after_failed_send() {
        let non_local_addr: SocketAddr = "212.129.33.59:6881".parse().unwrap();
        let sender_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7782);
        let receiver_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7783);

        let receiver_sock = UdpSocket::bind(receiver_addr).await.unwrap();

        let socket = UdpSocket::bind(sender_addr).await.unwrap();
        let (tx_channel, _rx_channel, runner) = setup_udp(socket);
        task::spawn(runner.run());

        // send a message to nowhere
        tx_channel
            .send((
                Message {
                    transaction_id: Vec::from(b"aa"),
                    version: None,
                    data: MessageData::Error(ErrorMsg {
                        error_code: ErrorCode::Generic,
                        error_msg: "A Generic Error Ocurred".to_owned(),
                    }),
                },
                non_local_addr,
            ))
            .await
            .unwrap();

        // send a message to somewhere
        tx_channel
            .send((
                Message {
                    transaction_id: Vec::from(b"aa"),
                    version: None,
                    data: MessageData::Error(ErrorMsg {
                        error_code: ErrorCode::Generic,
                        error_msg: "A Generic Error Ocurred".to_owned(),
                    }),
                },
                receiver_addr.into(),
            ))
            .await
            .unwrap();

        let mut buf = [0u8; 1500];
        let (len, src_addr) =
            timeout(sec!(5), receiver_sock.recv_from(&mut buf)).await.unwrap().unwrap();
        assert_eq!(src_addr, sender_addr.into());
        assert_eq!(&buf[..len], b"d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee");
    }

    #[tokio::test]
    async fn receive_multiple_messages() {
        let sender_port = 7784u16;
        let receiver_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7785);

        let sender_sock = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, sender_port))
            .await
            .unwrap();

        let socket = create_ipv4_socket(receiver_addr.port()).await.unwrap();
        let (_tx_channel, mut rx_channel, runner) = setup_udp(socket);
        task::spawn(runner.run());

        let sent_msg = Message {
            transaction_id: vec![1, 2, 3, 4],
            version: None,
            data: MessageData::Query(
                PingArgs {
                    id: [12u8; 20].into(),
                }
                .into(),
            ),
        };
        sender_sock
            .send_to(&benc::Element::from(sent_msg).to_bytes(), receiver_addr)
            .await
            .unwrap();
        let (receved_msg, src_addr) = timeout(sec!(5), rx_channel.recv()).await.unwrap().unwrap();
        assert_eq!(src_addr, SocketAddr::new(Ipv4Addr::LOCALHOST.into(), sender_port));
        assert_eq!(receved_msg.transaction_id, vec![1, 2, 3, 4]);
        assert_eq!(receved_msg.version, None);
        let ping = match receved_msg.data {
            MessageData::Query(QueryMsg::Ping(ping)) => ping,
            _ => panic!("Expected a ping query"),
        };
        assert_eq!(ping.id, U160::from([12u8; 20]));

        let sent_msg = Message {
            transaction_id: vec![5, 6, 7, 8],
            version: None,
            data: MessageData::Query(
                PingArgs {
                    id: [13u8; 20].into(),
                }
                .into(),
            ),
        };
        sender_sock
            .send_to(&benc::Element::from(sent_msg).to_bytes(), receiver_addr)
            .await
            .unwrap();
        let (receved_msg, src_addr) = timeout(sec!(5), rx_channel.recv()).await.unwrap().unwrap();
        assert_eq!(src_addr, SocketAddr::new(Ipv4Addr::LOCALHOST.into(), sender_port));
        assert_eq!(receved_msg.transaction_id, vec![5, 6, 7, 8]);
        assert_eq!(receved_msg.version, None);
        let ping = match receved_msg.data {
            MessageData::Query(QueryMsg::Ping(ping)) => ping,
            _ => panic!("Expected a ping query"),
        };
        assert_eq!(ping.id, U160::from([13u8; 20]));
    }

    #[tokio::test]
    async fn send_multiple_messages() {
        let sender_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7786);
        let receiver_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7787);

        let receiver_sock = UdpSocket::bind(receiver_addr).await.unwrap();

        let socket = create_ipv4_socket(sender_addr.port()).await.unwrap();
        let (tx_channel, _rx_channel, runner) = setup_udp(socket);
        task::spawn(runner.run());

        tx_channel
            .send((
                Message {
                    transaction_id: Vec::from(b"aa"),
                    version: None,
                    data: MessageData::Error(ErrorMsg {
                        error_code: ErrorCode::Generic,
                        error_msg: "A Generic Error Ocurred".to_owned(),
                    }),
                },
                receiver_addr.into(),
            ))
            .await
            .unwrap();

        let mut buf = [0u8; 1500];
        let (len, src_addr) =
            timeout(sec!(5), receiver_sock.recv_from(&mut buf)).await.unwrap().unwrap();
        assert_eq!(src_addr, sender_addr.into());
        assert_eq!(&buf[..len], b"d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee");

        tx_channel
            .send((
                Message {
                    transaction_id: Vec::from(b"bb"),
                    version: None,
                    data: MessageData::Error(ErrorMsg {
                        error_code: ErrorCode::Generic,
                        error_msg: "A Generic Error Ocurred".to_owned(),
                    }),
                },
                receiver_addr.into(),
            ))
            .await
            .unwrap();

        let mut buf = [0u8; 1500];
        let (len, src_addr) =
            timeout(sec!(5), receiver_sock.recv_from(&mut buf)).await.unwrap().unwrap();
        assert_eq!(src_addr, sender_addr.into());
        assert_eq!(&buf[..len], b"d1:eli201e23:A Generic Error Ocurrede1:t2:bb1:y1:ee");
    }

    #[tokio::test]
    async fn drop_incoming_message_when_channel_is_full() {
        let _ = simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Info).init();

        let first_sender_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7788);
        let second_sender_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7789);
        let receiver_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7791);

        let first_sender = UdpSocket::bind(first_sender_addr).await.unwrap();
        let second_sender = UdpSocket::bind(second_sender_addr).await.unwrap();

        let socket = create_ipv4_socket(receiver_addr.port()).await.unwrap();
        let (_tx_channel, mut rx_channel, runner) = setup_udp(socket);
        let mut _reserved = iter::repeat_with(|| runner.ingress_sender.clone().try_reserve_owned())
            .take(511)
            .collect::<Vec<_>>();
        task::spawn(runner.run());

        let first_msg = Message {
            transaction_id: vec![1, 2, 3, 4],
            version: None,
            data: MessageData::Query(
                PingArgs {
                    id: [12u8; 20].into(),
                }
                .into(),
            ),
        };
        first_sender
            .send_to(&benc::Element::from(first_msg).to_bytes(), receiver_addr)
            .await
            .unwrap();

        let dropped_msg = Message {
            transaction_id: vec![5, 6, 7, 8],
            version: None,
            data: MessageData::Query(
                PingArgs {
                    id: [13u8; 20].into(),
                }
                .into(),
            ),
        };
        second_sender
            .send_to(&benc::Element::from(dropped_msg).to_bytes(), receiver_addr)
            .await
            .unwrap();

        let (receved_msg, src_addr) = timeout(sec!(5), rx_channel.recv()).await.unwrap().unwrap();
        assert_eq!(src_addr, first_sender_addr.into());
        assert_eq!(receved_msg.transaction_id, vec![1, 2, 3, 4]);
        assert_eq!(receved_msg.version, None);
        let ping = match receved_msg.data {
            MessageData::Query(QueryMsg::Ping(ping)) => ping,
            _ => panic!("Expected a ping query"),
        };
        assert_eq!(ping.id, U160::from([12u8; 20]));

        let last_msg = Message {
            transaction_id: vec![9, 0],
            version: None,
            data: MessageData::Query(
                PingArgs {
                    id: [14u8; 20].into(),
                }
                .into(),
            ),
        };
        second_sender
            .send_to(&benc::Element::from(last_msg).to_bytes(), receiver_addr)
            .await
            .unwrap();

        let (receved_msg, src_addr) = timeout(sec!(5), rx_channel.recv()).await.unwrap().unwrap();
        assert_eq!(src_addr, second_sender_addr.into());
        assert_eq!(receved_msg.transaction_id, vec![9, 0]);
        assert_eq!(receved_msg.version, None);
        let ping = match receved_msg.data {
            MessageData::Query(QueryMsg::Ping(ping)) => ping,
            _ => panic!("Expected a ping query"),
        };
        assert_eq!(ping.id, U160::from([14u8; 20]));
    }
}
