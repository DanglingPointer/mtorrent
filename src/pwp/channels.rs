use crate::pwp::handshake::*;
use crate::pwp::message::*;
use crate::sec;
use futures::channel::mpsc;
use futures::{select, select_biased, FutureExt, SinkExt, StreamExt};
use std::future;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use std::{fmt, io};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{tcp, TcpStream};
use tokio::time::sleep;
use tokio::time::timeout;

#[derive(Debug)]
pub enum ChannelError {
    Timeout,
    ConnectionClosed,
}

pub struct PeerChannel<Q> {
    peer_info: Rc<PeerInfo>,
    inner: Q,
}

impl<Q> PeerChannel<Q> {
    pub fn remote_ip(&self) -> &SocketAddr {
        &self.peer_info.remote_addr
    }
    pub fn remote_info(&self) -> &Handshake {
        &self.peer_info.handshake_info
    }
}

type RxChannel<Msg> = PeerChannel<mpsc::Receiver<Msg>>;
type TxChannel<Msg> = PeerChannel<mpsc::Sender<Option<Msg>>>;

impl<Msg> RxChannel<Msg> {
    pub async fn receive_message(&mut self) -> Result<Msg, ChannelError> {
        self.inner.next().await.ok_or(ChannelError::ConnectionClosed)
    }

    pub async fn receive_message_timed(&mut self, deadline: Duration) -> Result<Msg, ChannelError> {
        timeout(deadline, self.receive_message()).await.or(Err(ChannelError::Timeout))?
    }
}

impl<Msg> TxChannel<Msg> {
    pub async fn send_message(&mut self, msg: Msg) -> Result<(), ChannelError> {
        self.inner.send(Some(msg)).await?;
        self.inner.send(None).await?;
        Ok(())
    }

    pub async fn send_message_timed(
        &mut self,
        msg: Msg,
        deadline: Duration,
    ) -> Result<(), ChannelError> {
        timeout(deadline, self.send_message(msg)).await.or(Err(ChannelError::Timeout))?
    }
}

pub type DownloadTxChannel = TxChannel<DownloaderMessage>;
pub type DownloadRxChannel = RxChannel<UploaderMessage>;
pub struct DownloadChannels(pub DownloadTxChannel, pub DownloadRxChannel);

pub type UploadTxChannel = TxChannel<UploaderMessage>;
pub type UploadRxChannel = RxChannel<DownloaderMessage>;
pub struct UploadChannels(pub UploadTxChannel, pub UploadRxChannel);

pub type ExtendedTxChannel = TxChannel<(ExtendedMessage, u8)>;
pub type ExtendedRxChannel = RxChannel<ExtendedMessage>;
pub struct ExtendedChannels(pub ExtendedTxChannel, pub ExtendedRxChannel);

// ------

pub struct Runner<I, E>
where
    I: AsyncReadExt,
    E: AsyncWriteExt,
{
    receiver: IngressStream<I>,
    sender: EgressStream<E>,
}

impl<I, E> Runner<I, E>
where
    I: AsyncReadExt + Unpin,
    E: AsyncWriteExt + Unpin,
{
    pub async fn run(self) -> io::Result<()> {
        let read_fut = async {
            let mut receiver = self.receiver;
            loop {
                receiver.read_one_message().await?;
            }
        };
        let write_fut = async {
            let mut sender = self.sender;
            loop {
                sender.write_one_message().await?;
            }
        };
        select! {
            read_result = read_fut.fuse() => read_result,
            write_result = write_fut.fuse() => write_result,
        }
    }
}

pub type ConnectionRunner = Runner<tcp::OwnedReadHalf, tcp::OwnedWriteHalf>;

// ------

const HANDSHAKE_TIMEOUT: Duration = sec!(30);

pub async fn channels_from_incoming(
    local_peer_id: &[u8; 20],
    info_hash: Option<&[u8; 20]>,
    extension_protocol_enabled: bool,
    remote_ip: SocketAddr,
    socket: TcpStream,
) -> io::Result<(DownloadChannels, UploadChannels, Option<ExtendedChannels>, ConnectionRunner)> {
    let local_handshake = Handshake {
        peer_id: *local_peer_id,
        info_hash: *info_hash.unwrap_or(&[0u8; 20]),
        reserved: reserved_bits(extension_protocol_enabled),
    };
    let (socket, remote_handshake) = timeout(
        HANDSHAKE_TIMEOUT,
        do_handshake_incoming(&remote_ip, socket, &local_handshake, info_hash.is_none()),
    )
    .await??;

    let (ingress, egress) = socket.into_split();

    Ok(setup_channels(ingress, egress, remote_ip, remote_handshake, extension_protocol_enabled))
}

pub async fn channels_from_outgoing(
    local_peer_id: &[u8; 20],
    info_hash: &[u8; 20],
    extension_protocol_enabled: bool,
    remote_addr: SocketAddr,
    remote_peer_id: Option<&[u8; 20]>,
) -> io::Result<(DownloadChannels, UploadChannels, Option<ExtendedChannels>, ConnectionRunner)> {
    let local_handshake = Handshake {
        peer_id: *local_peer_id,
        info_hash: *info_hash,
        reserved: reserved_bits(extension_protocol_enabled),
    };
    let socket = TcpStream::connect(remote_addr).await?;
    let (socket, remote_handshake) = timeout(
        HANDSHAKE_TIMEOUT,
        do_handshake_outgoing(&remote_addr, socket, &local_handshake, remote_peer_id),
    )
    .await??;

    let remote_ip = socket.peer_addr()?;
    let (ingress, egress) = socket.into_split();

    Ok(setup_channels(ingress, egress, remote_ip, remote_handshake, extension_protocol_enabled))
}

// ------

struct PeerInfo {
    handshake_info: Handshake,
    remote_addr: SocketAddr,
}

const MAX_INCOMING_QUEUE: usize = 20;

fn setup_channels<I, E>(
    ingress: I,
    egress: E,
    remote_ip: SocketAddr,
    remote_handshake: Handshake,
    extended_protocol_enabled: bool,
) -> (DownloadChannels, UploadChannels, Option<ExtendedChannels>, Runner<I, E>)
where
    I: AsyncReadExt,
    E: AsyncWriteExt,
{
    let extended_protocol_supported = is_extension_protocol_enabled(&remote_handshake.reserved);

    let info = Rc::new(PeerInfo {
        handshake_info: remote_handshake,
        remote_addr: remote_ip,
    });

    let (local_uploader_msg_in, local_uploader_msg_out) =
        mpsc::channel::<Option<UploaderMessage>>(0);
    let (local_downloader_msg_in, local_downloader_msg_out) =
        mpsc::channel::<Option<DownloaderMessage>>(0);

    let (remote_uploader_msg_in, remote_uploader_msg_out) =
        mpsc::channel::<UploaderMessage>(MAX_INCOMING_QUEUE);
    let (remote_downloader_msg_in, remote_downloader_msg_out) =
        mpsc::channel::<DownloaderMessage>(MAX_INCOMING_QUEUE);

    let (local_extended_msg_out, remote_extended_msg_in, extended_channels) =
        if extended_protocol_supported && extended_protocol_enabled {
            let (local_extended_msg_in, local_extended_msg_out) =
                mpsc::channel::<Option<(ExtendedMessage, u8)>>(0);
            let (remote_extended_msg_in, remote_extended_msg_out) =
                mpsc::channel::<ExtendedMessage>(MAX_INCOMING_QUEUE);

            let extended_rx = ExtendedRxChannel {
                inner: remote_extended_msg_out,
                peer_info: info.clone(),
            };
            let extended_tx = ExtendedTxChannel {
                inner: local_extended_msg_in,
                peer_info: info.clone(),
            };
            (
                Some(local_extended_msg_out),
                Some(remote_extended_msg_in),
                Some(ExtendedChannels(extended_tx, extended_rx)),
            )
        } else {
            (None, None, None)
        };

    let receiver = IngressStream {
        source: ingress,
        remote_ip,
        ul_msg_sink: remote_uploader_msg_in,
        dl_msg_sink: remote_downloader_msg_in,
        ext_msg_sink: remote_extended_msg_in,
    };
    let sender = EgressStream {
        sink: BufWriter::new(egress),
        remote_ip,
        dl_msg_source: local_downloader_msg_out,
        ul_msg_source: local_uploader_msg_out,
        ext_msg_source: local_extended_msg_out,
    };

    let download_rx = DownloadRxChannel {
        inner: remote_uploader_msg_out,
        peer_info: info.clone(),
    };
    let download_tx = DownloadTxChannel {
        inner: local_downloader_msg_in,
        peer_info: info.clone(),
    };

    let upload_rx = UploadRxChannel {
        inner: remote_downloader_msg_out,
        peer_info: info.clone(),
    };
    let upload_tx = UploadTxChannel {
        inner: local_uploader_msg_in,
        peer_info: info.clone(),
    };

    (
        DownloadChannels(download_tx, download_rx),
        UploadChannels(upload_tx, upload_rx),
        extended_channels,
        Runner { receiver, sender },
    )
}

struct IngressStream<S: AsyncReadExt> {
    source: S,
    remote_ip: SocketAddr,
    ul_msg_sink: mpsc::Sender<UploaderMessage>,
    dl_msg_sink: mpsc::Sender<DownloaderMessage>,
    ext_msg_sink: Option<mpsc::Sender<ExtendedMessage>>,
}

impl<S: AsyncReadExt + Unpin> IngressStream<S> {
    const RECV_TIMEOUT: Duration = sec!(120);

    async fn read_one_message(&mut self) -> io::Result<()> {
        async fn forward_msg<M: fmt::Display>(
            msg: M,
            sink: &mut mpsc::Sender<M>,
            source: &SocketAddr,
        ) -> io::Result<()> {
            log::trace!("{} => {}", source, msg);
            sink.send(msg)
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, Box::new(e)))?;
            Ok(())
        }

        timeout(Self::RECV_TIMEOUT, self.source.read(&mut [0u8; 0])).await??;
        let received = PeerMessage::read_from(&mut self.source).await?;

        let received = match UploaderMessage::try_from(received) {
            Ok(msg) => {
                return forward_msg(msg, &mut self.ul_msg_sink, &self.remote_ip).await;
            }
            Err(received) => received,
        };
        let received = match DownloaderMessage::try_from(received) {
            Ok(msg) => {
                return forward_msg(msg, &mut self.dl_msg_sink, &self.remote_ip).await;
            }
            Err(received) => received,
        };
        let received = if let Some(ext_msg_sink) = &mut self.ext_msg_sink {
            match ExtendedMessage::try_from(received) {
                Ok(msg) => return forward_msg(msg, ext_msg_sink, &self.remote_ip).await,
                Err(received) => received,
            }
        } else {
            received
        };
        if matches!(received, PeerMessage::KeepAlive) {
            log::trace!("{} => {:?}", self.remote_ip, received);
        } else {
            log::error!("{} => unknown message: {:?}", self.remote_ip, received)
        }
        Ok(())
    }
}

struct EgressStream<S: AsyncWriteExt> {
    sink: BufWriter<S>,
    remote_ip: SocketAddr,
    dl_msg_source: mpsc::Receiver<Option<DownloaderMessage>>,
    ul_msg_source: mpsc::Receiver<Option<UploaderMessage>>,
    ext_msg_source: Option<mpsc::Receiver<Option<(ExtendedMessage, u8)>>>,
}

impl<S: AsyncWriteExt + Unpin> EgressStream<S> {
    const PING_INTERVAL: Duration = sec!(30);

    async fn write_one_message(&mut self) -> io::Result<()> {
        fn new_channel_closed_error() -> io::Error {
            io::Error::new(io::ErrorKind::Other, "Channel closed")
        }

        async fn process_msg<M, S>(
            msg: Option<M>,
            source: &mut mpsc::Receiver<Option<M>>,
            sink: &mut BufWriter<S>,
            dest: &SocketAddr,
        ) -> io::Result<()>
        where
            M: Into<PeerMessage> + fmt::Display,
            S: AsyncWriteExt + Unpin,
        {
            let first = msg.expect("First msg must be non-None");
            log::trace!("{} <= {}", dest, first);
            first.into().write_to(sink).await?;
            let second = source.next().await.ok_or_else(new_channel_closed_error)?;
            assert!(second.is_none(), "Second msg must be None");
            Ok(())
        }

        let next_ext_msg_fut = async {
            if let Some(ext_src) = &mut self.ext_msg_source {
                ext_src.next().await
            } else {
                future::pending().await
            }
        };

        select_biased! {
            rx_msg = self.dl_msg_source.next().fuse() => {
                let msg = rx_msg.ok_or_else(new_channel_closed_error)?;
                process_msg(msg, &mut self.dl_msg_source, &mut self.sink, &self.remote_ip).await?;
            }
            tx_msg = self.ul_msg_source.next().fuse() => {
                let msg = tx_msg.ok_or_else(new_channel_closed_error)?;
                process_msg(msg, &mut self.ul_msg_source, &mut self.sink, &self.remote_ip).await?;
            }
            ext_msg = next_ext_msg_fut.fuse() => {
                let msg = ext_msg.ok_or_else(new_channel_closed_error)?;
                let first = msg.expect("First msg must be non-None");
                log::trace!("{} <= {}", self.remote_ip, first.0);
                PeerMessage::from(first).write_to(&mut self.sink).await?;
                let second = self.ext_msg_source.as_mut().unwrap().next().await.ok_or_else(new_channel_closed_error)?;
                assert!(second.is_none(), "Second msg must be None");
            }
            _ = sleep(Self::PING_INTERVAL).fuse() => {
                let ping_msg = PeerMessage::KeepAlive;
                log::trace!("{} <= {:?}", self.remote_ip, &ping_msg);
                ping_msg.write_to(&mut self.sink).await?;
            }
        };
        Ok(())
    }
}

impl From<mpsc::SendError> for ChannelError {
    fn from(_: mpsc::SendError) -> Self {
        ChannelError::ConnectionClosed
    }
}

impl From<ChannelError> for io::Error {
    fn from(ce: ChannelError) -> Self {
        match ce {
            ChannelError::Timeout => {
                io::Error::new(io::ErrorKind::TimedOut, "Peer channel timeout")
            }
            ChannelError::ConnectionClosed => io::Error::from(io::ErrorKind::UnexpectedEof),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::LocalPool;
    use futures::join;
    use futures::task::LocalSpawnExt;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncSeekExt, AsyncWrite, ReadBuf};
    use tokio::{io, task, time};

    struct ErrorStream;
    impl ErrorStream {
        const ERROR_KIND: io::ErrorKind = io::ErrorKind::OutOfMemory;
    }
    impl AsyncRead for ErrorStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::from(Self::ERROR_KIND)))
        }
    }
    impl AsyncWrite for ErrorStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::from(Self::ERROR_KIND)))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::from(Self::ERROR_KIND)))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    struct PendingStream;
    impl AsyncRead for PendingStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }
    impl AsyncWrite for PendingStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    type FakeSocket = Cursor<Vec<u8>>;

    async fn fake_socket_containing(msgs: Vec<PeerMessage>) -> FakeSocket {
        let mut socket = BufWriter::new(FakeSocket::default());
        for msg in msgs {
            msg.write_to(&mut socket).await.unwrap();
        }
        socket.rewind().await.unwrap();
        socket.into_inner()
    }

    struct FakeSink(mpsc::UnboundedSender<u8>);
    impl AsyncWrite for FakeSink {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            for byte in buf {
                self.0.unbounded_send(*byte).unwrap();
            }
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
            self.0.close_channel();
            Poll::Ready(Ok(()))
        }
    }

    const HANDSHAKE_WITH_BEP10_SUPPORT: Handshake = Handshake {
        reserved: ReservedBits {
            data: *b"\x00\x00\x00\x00\x00\x10\x00\x00",
            ..ReservedBits::ZERO
        },
        peer_id: [0u8; 20],
        info_hash: [0u8; 20],
    };

    #[tokio::test]
    async fn test_read_downloader_message() {
        let ingress = fake_socket_containing(vec![PeerMessage::Interested]).await;
        let (mut download, mut upload, extended, mut runner) = setup_channels(
            ingress,
            PendingStream {},
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
            false,
        );
        assert!(extended.is_none());

        let upload_fut = async move {
            let result = upload.1.receive_message().await;
            assert!(matches!(result, Ok(DownloaderMessage::Interested)));
        };

        let run_fut = async move {
            runner.receiver.read_one_message().await.unwrap();
        };

        let download_fut = async move {
            let result = download.1.receive_message().await;
            assert!(matches!(result, Err(ChannelError::ConnectionClosed)));
        };

        join!(upload_fut, run_fut, download_fut);
    }

    #[tokio::test]
    async fn test_read_uploader_message() {
        let ingress = fake_socket_containing(vec![PeerMessage::Unchoke]).await;
        let (mut download, mut upload, extended, mut runner) = setup_channels(
            ingress,
            PendingStream {},
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
            false,
        );
        assert!(extended.is_none());

        let download_fut = async move {
            let result = download.1.receive_message().await;
            assert!(matches!(result, Ok(UploaderMessage::Unchoke)));
        };

        let run_fut = async move {
            runner.receiver.read_one_message().await.unwrap();
        };

        let upload_fut = async move {
            let result = upload.1.receive_message().await;
            assert!(matches!(result, Err(ChannelError::ConnectionClosed)));
        };

        join!(download_fut, run_fut, upload_fut);
    }

    #[tokio::test]
    async fn test_read_extended_message() {
        let ingress = fake_socket_containing(vec![
            PeerMessage::Extended {
                id: 0,
                data: Vec::from(
                    b"d1:md11:ut_metadatai1e6:ut_pexi2ee1:pi6881e1:v13:\xc2\xb5Torrent 1.2e",
                ),
            },
            PeerMessage::Extended {
                id: Extension::Metadata.local_id(),
                data: Vec::from("d8:msg_typei2e5:piecei3ee"),
            },
        ])
        .await;
        let (mut download, mut upload, extended, mut runner) = setup_channels(
            ingress,
            PendingStream {},
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            HANDSHAKE_WITH_BEP10_SUPPORT,
            true,
        );
        assert!(extended.is_some());

        let extended_fut = async move {
            let ExtendedChannels(_tx, mut rx) = extended.unwrap();
            let result = rx.receive_message().await;
            let received = result.unwrap();
            let expected_data = HandshakeData {
                extensions: HashMap::from([(Extension::Metadata, 1), (Extension::PeerExchange, 2)]),
                listen_port: Some(6881),
                client_type: Some("µTorrent 1.2".to_owned()),
                ..Default::default()
            };
            assert!(matches!(received, ExtendedMessage::Handshake(data) if *data == expected_data));

            let result = rx.receive_message().await;
            let received = result.unwrap();
            assert!(matches!(received, ExtendedMessage::MetadataReject { piece: 3 }));
        };

        let run_fut = async move {
            runner.receiver.read_one_message().await.unwrap();
            runner.receiver.read_one_message().await.unwrap();
        };

        let upload_fut = async move {
            let result = upload.1.receive_message().await;
            assert!(matches!(result, Err(ChannelError::ConnectionClosed)));
        };

        let download_fut = async move {
            let result = download.1.receive_message().await;
            assert!(matches!(result, Err(ChannelError::ConnectionClosed)));
        };

        join!(extended_fut, run_fut, upload_fut, download_fut);
    }

    #[tokio::test]
    async fn test_read_uploader_and_downloader_and_extended_messages() {
        let ingress = fake_socket_containing(vec![
            PeerMessage::KeepAlive,
            PeerMessage::Interested,
            PeerMessage::Unchoke,
            PeerMessage::KeepAlive,
            PeerMessage::Extended {
                id: 0,
                data: Vec::from(b"d1:md11:ut_metadatai1eee"),
            },
        ])
        .await;
        let (mut download, mut upload, extended, runner) = setup_channels(
            ingress,
            PendingStream {},
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            HANDSHAKE_WITH_BEP10_SUPPORT,
            true,
        );

        let upload_fut = async {
            let result = upload.1.receive_message().await;
            assert!(matches!(result, Ok(DownloaderMessage::Interested)));
        };

        let download_fut = async {
            let result = download.1.receive_message().await;
            assert!(matches!(result, Ok(UploaderMessage::Unchoke)));
        };

        let extended_fut = async move {
            let result = extended.unwrap().1.receive_message().await;
            let received = result.unwrap();
            let expected_data = HandshakeData {
                extensions: HashMap::from([(Extension::Metadata, 1)]),
                ..Default::default()
            };
            assert!(matches!(received, ExtendedMessage::Handshake(data) if *data == expected_data));
        };

        let run_fut = async move {
            let result = runner.run().await;
            let error = result.unwrap_err();
            assert_eq!(io::ErrorKind::UnexpectedEof, error.kind());
        };

        join!(upload_fut, download_fut, extended_fut, run_fut);
    }

    #[tokio::test]
    async fn test_read_error() {
        let (mut download, mut upload, _, runner) = setup_channels(
            ErrorStream {},
            PendingStream {},
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
            false,
        );

        let download_fut = async {
            let result = download.1.receive_message().await;
            assert!(matches!(result, Err(ChannelError::ConnectionClosed)));
        };

        let upload_fut = async {
            let result = upload.1.receive_message().await;
            assert!(matches!(result, Err(ChannelError::ConnectionClosed)));
        };

        let run_fut = async {
            let result = runner.run().await;
            let error = result.unwrap_err();
            assert_eq!(ErrorStream::ERROR_KIND, error.kind(), "{}", error);
        };

        join!(download_fut, upload_fut, run_fut);
    }

    #[tokio::test]
    async fn test_write_downloader_message() {
        let (mut download, upload, _, mut runner) = setup_channels(
            PendingStream {},
            FakeSocket::default(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
            false,
        );

        let send_msg_fut = async {
            let result = download.0.send_message(DownloaderMessage::Interested).await;
            assert!(result.is_ok());
        };

        let run_fut = async {
            runner.sender.write_one_message().await.unwrap();
            assert!(!runner.sender.sink.get_ref().get_ref().is_empty());

            runner.sender.sink.rewind().await.unwrap();
            let sent_message = PeerMessage::read_from(&mut runner.sender.sink).await.unwrap();
            assert!(matches!(sent_message, PeerMessage::Interested));
        };

        join!(send_msg_fut, run_fut);
        drop(upload);
    }

    #[tokio::test]
    async fn test_write_uploader_message() {
        let (download, mut upload, _, mut runner) = setup_channels(
            PendingStream {},
            FakeSocket::default(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
            false,
        );

        let send_msg_fut = async {
            let result = upload.0.send_message(UploaderMessage::Unchoke).await;
            assert!(result.is_ok());
        };

        let run_fut = async {
            runner.sender.write_one_message().await.unwrap();
            assert!(!runner.sender.sink.get_ref().get_ref().is_empty());

            runner.sender.sink.rewind().await.unwrap();
            let sent_message = PeerMessage::read_from(&mut runner.sender.sink).await.unwrap();
            assert!(matches!(sent_message, PeerMessage::Unchoke));
        };

        join!(send_msg_fut, run_fut);
        drop(download);
    }

    #[tokio::test]
    async fn test_write_extended_messages() {
        let (_download, _upload, extended, mut runner) = setup_channels(
            PendingStream {},
            FakeSocket::default(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            HANDSHAKE_WITH_BEP10_SUPPORT,
            true,
        );
        assert!(extended.is_some());
        let ExtendedChannels(mut tx, _rx) = extended.unwrap();

        let send_msg_fut = async {
            let result = tx.send_message((ExtendedMessage::MetadataReject { piece: 3 }, 1)).await;
            assert!(result.is_ok());

            let hs_data = HandshakeData {
                extensions: HashMap::from([(Extension::Metadata, 1), (Extension::PeerExchange, 2)]),
                listen_port: Some(6881),
                client_type: Some("µTorrent 1.2".to_owned()),
                ..Default::default()
            };
            let result = tx.send_message((ExtendedMessage::Handshake(Box::new(hs_data)), 42)).await;
            assert!(result.is_ok());
        };

        let run_fut = async {
            runner.sender.write_one_message().await.unwrap();
            runner.sender.write_one_message().await.unwrap();
            assert!(!runner.sender.sink.get_ref().get_ref().is_empty());

            runner.sender.sink.rewind().await.unwrap();
            let sent_message = PeerMessage::read_from(&mut runner.sender.sink).await.unwrap();
            assert!(
                matches!(
                    sent_message,
                    PeerMessage::Extended {
                        id: 1,
                        ref data
                    } if data == &Vec::from("d8:msg_typei2e5:piecei3ee"),
                ),
                "{sent_message:?}"
            );
            let sent_message = PeerMessage::read_from(&mut runner.sender.sink).await.unwrap();
            assert!(
                matches!(
                    sent_message,
                    PeerMessage::Extended {
                        id: 0,
                        ref data
                    } if data == &Vec::from(b"d1:md11:ut_metadatai1e6:ut_pexi2ee1:pi6881e1:v13:\xc2\xb5Torrent 1.2e"),
                ),
                "{sent_message:?}"
            );
        };

        join!(send_msg_fut, run_fut);
    }

    #[tokio::test]
    async fn test_write_error() {
        let (mut download, upload, _, runner) = setup_channels(
            PendingStream {},
            ErrorStream {},
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
            false,
        );

        let send_msg_fut = async {
            let result = download.0.send_message(DownloaderMessage::Interested).await;
            assert!(matches!(result, Err(ChannelError::ConnectionClosed)));
        };

        let run_fut = async {
            let result = runner.run().await;
            let error = result.unwrap_err();
            assert_eq!(ErrorStream::ERROR_KIND, error.kind(), "{}", error);
        };

        join!(send_msg_fut, run_fut);
        drop(upload);
    }

    #[allow(clippy::await_holding_refcell_ref)]
    #[tokio::test]
    async fn test_writing_downloader_messages_takes_priority_over_uploader_messages() {
        let (mut download, mut upload, _, runner) = setup_channels(
            PendingStream {},
            FakeSocket::default(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
            false,
        );

        let runner = Rc::new(RefCell::new(runner));
        let runner_copy = runner.clone();

        let send_uploader_msg_fut = async move {
            upload
                .0
                .send_message(UploaderMessage::Block(
                    BlockInfo {
                        piece_index: 0,
                        in_piece_offset: 0,
                        block_length: 16384,
                    },
                    vec![0u8; 1024],
                ))
                .await
                .unwrap();
        };
        let send_downloader_msg_fut = async move {
            download.0.send_message(DownloaderMessage::Interested).await.unwrap();
        };
        let runner_fut = async move {
            while runner_copy.borrow_mut().sender.write_one_message().await.is_ok() {}
        };
        let mut pool = LocalPool::new();

        // given
        pool.spawner().spawn_local(send_downloader_msg_fut).unwrap();
        pool.spawner().spawn_local(send_uploader_msg_fut).unwrap();
        pool.run_until_stalled();

        // when
        pool.spawner().spawn_local(runner_fut).unwrap();
        pool.run_until_stalled();
        drop(pool);

        // then
        let mut transmitted_data = runner.borrow().sender.sink.get_ref().clone();

        transmitted_data.rewind().await.unwrap();

        let first_msg = PeerMessage::read_from(&mut transmitted_data).await.unwrap();
        assert!(matches!(first_msg, PeerMessage::Interested), "{:?}", first_msg);

        let second_msg = PeerMessage::read_from(&mut transmitted_data).await.unwrap();
        if let PeerMessage::Piece {
            index,
            begin,
            block,
        } = second_msg
        {
            assert_eq!(0, index);
            assert_eq!(0, begin);
            assert_eq!(vec![0u8; 1024], block);
        } else {
            panic!("{:?}", second_msg);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_send_keepalive_every_30s() {
        task::LocalSet::new()
            .run_until(async {
                let mut buf = Vec::<u8>::new();
                let (writer, mut reader) = mpsc::unbounded::<u8>();

                let (_download, _upload, _, runner) = setup_channels(
                    PendingStream {},
                    FakeSink(writer),
                    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
                    Default::default(),
                    false,
                );

                task::spawn_local(async move {
                    let _ = runner.run().await;
                });

                time::sleep(sec!(30)).await;
                assert!(reader.try_next().is_err());

                task::yield_now().await;
                while let Ok(Some(byte)) = reader.try_next() {
                    buf.push(byte);
                }
                assert_eq!(4, buf.len());
                assert_eq!(&[0u8; 4], &buf[..4]);

                time::sleep(sec!(30)).await;
                assert!(reader.try_next().is_err());

                task::yield_now().await;
                while let Ok(Some(byte)) = reader.try_next() {
                    buf.push(byte);
                }
                assert_eq!(8, buf.len());
                assert_eq!(&[0u8; 4], &buf[4..8]);
            })
            .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_receiver_times_out_after_2_min() {
        task::LocalSet::new()
            .run_until(async {
                let (_download, _upload, _, runner) = setup_channels(
                    PendingStream {},
                    FakeSocket::default(),
                    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
                    Default::default(),
                    false,
                );

                let (mut result_sender, mut result_receiver) = mpsc::channel::<io::Result<()>>(1);

                task::spawn_local(async move {
                    let result = runner.run().await;
                    result_sender.try_send(result).unwrap();
                });

                time::sleep(sec!(120)).await;
                assert!(result_receiver.try_next().is_err());

                task::yield_now().await;
                let error = result_receiver
                    .try_next()
                    .expect("Runner not finished")
                    .expect("channel closed");
                assert_eq!(io::ErrorKind::TimedOut, error.unwrap_err().kind());
            })
            .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_channel_send_timeout() {
        task::LocalSet::new()
            .run_until(async {
                const TIMEOUT: Duration = sec!(10);

                let (mut download, mut _upload, _, _runner) = setup_channels(
                    PendingStream {},
                    PendingStream {},
                    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
                    Default::default(),
                    false,
                );

                let (mut result_sender, mut result_receiver) =
                    mpsc::channel::<Result<(), ChannelError>>(1);

                task::spawn_local(async move {
                    let result = download
                        .0
                        .send_message_timed(DownloaderMessage::NotInterested, TIMEOUT)
                        .await;
                    result_sender.try_send(result).unwrap();
                });

                time::sleep(TIMEOUT).await;
                assert!(result_receiver.try_next().is_err());

                task::yield_now().await;
                let result =
                    result_receiver.try_next().expect("send not finished").expect("channel closed");
                assert!(matches!(result, Err(ChannelError::Timeout)));
            })
            .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_channel_receive_timeout() {
        task::LocalSet::new()
            .run_until(async {
                const TIMEOUT: Duration = sec!(10);

                let (mut _download, mut upload, _, _runner) = setup_channels(
                    PendingStream {},
                    PendingStream {},
                    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
                    Default::default(),
                    false,
                );

                let (mut result_sender, mut result_receiver) =
                    mpsc::channel::<Result<DownloaderMessage, ChannelError>>(1);

                task::spawn_local(async move {
                    let result = upload.1.receive_message_timed(TIMEOUT).await;
                    result_sender.try_send(result).unwrap();
                });

                time::sleep(TIMEOUT).await;
                assert!(result_receiver.try_next().is_err());

                task::yield_now().await;
                let result = result_receiver
                    .try_next()
                    .expect("receive not finished")
                    .expect("channel closed");
                assert!(matches!(result, Err(ChannelError::Timeout)));
            })
            .await;
    }
}
