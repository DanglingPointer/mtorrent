use super::MAX_BLOCK_SIZE;
use super::handshake::*;
use super::message::*;
use futures_channel::mpsc;
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, SinkExt, StreamExt};
use local_async_utils::prelude::*;
use std::future::{self, Future};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter,
};
use tokio::net::{TcpStream, tcp};
use tokio::time::{sleep, timeout};
use tokio::{select, try_join};

/// Error type for receiving messages from,
/// or sending them to, a [`PeerChannel`].
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum ChannelError {
    #[error("timeout")]
    Timeout,
    #[error("connection closed")]
    ConnectionClosed,
}

struct PeerInfo {
    handshake_info: Handshake,
    remote_addr: SocketAddr,
}

/// Channel for communicating with a single peer.
pub struct PeerChannel<Q> {
    peer_info: Rc<PeerInfo>,
    inner: Q,
}

impl<Q> PeerChannel<Q> {
    /// Address of the peer.
    pub fn remote_ip(&self) -> &SocketAddr {
        &self.peer_info.remote_addr
    }
    /// Handshake received from the peer upon connecting.
    pub fn remote_info(&self) -> &Handshake {
        &self.peer_info.handshake_info
    }
}

impl<Q: Clone> Clone for PeerChannel<Q> {
    fn clone(&self) -> Self {
        Self {
            peer_info: self.peer_info.clone(),
            inner: self.inner.clone(),
        }
    }
}

type RxChannel<Msg> = PeerChannel<mpsc::Receiver<Msg>>;
type TxChannel<Msg> = PeerChannel<mpsc::Sender<Option<Msg>>>;

impl<Msg> RxChannel<Msg> {
    /// Wait for the next message received from the peer.
    /// Returns [`ChannelError::ConnectionClosed`] if [`ConnectionIoDriver`] has exited.
    pub async fn receive_message(&mut self) -> Result<Msg, ChannelError> {
        self.inner.next().await.ok_or(ChannelError::ConnectionClosed)
    }

    /// Wait up to `deadline` for the next message received from the peer.
    /// Returns [`ChannelError::ConnectionClosed`] if [`ConnectionIoDriver`] has exited,
    /// or [`ChannelError::Timeout`] if the deadline has elapsed.
    pub async fn receive_message_timed(&mut self, deadline: Duration) -> Result<Msg, ChannelError> {
        timeout(deadline, self.receive_message()).await.or(Err(ChannelError::Timeout))?
    }
}

impl<Msg> TxChannel<Msg> {
    /// Send a single message to the peer. The async call will only return once
    /// the message has been successfully written to the socket.
    /// Returns [`ChannelError::ConnectionClosed`] if [`ConnectionIoDriver`] has exited.
    pub async fn send_message(&mut self, msg: Msg) -> Result<(), ChannelError> {
        self.inner.send(Some(msg)).await?;
        self.inner.send(None).await?;
        Ok(())
    }

    /// Send a single message to the peer. The async call will return either once
    /// the message has been successfully written to the socket, or when `deadline` has elapsed.
    /// Returns [`ChannelError::ConnectionClosed`] if [`ConnectionIoDriver`] has exited,
    /// or [`ChannelError::Timeout`] if the deadline has elapsed.
    pub async fn send_message_timed(
        &mut self,
        msg: Msg,
        deadline: Duration,
    ) -> Result<(), ChannelError> {
        timeout(deadline, self.send_message(msg)).await.or(Err(ChannelError::Timeout))?
    }
}

/// Channel for sending messages related to us downloading data from the peer.
pub type DownloadTxChannel = TxChannel<DownloaderMessage>;
/// Channel for receiving messages related to the peer uploading data to us.
pub type DownloadRxChannel = RxChannel<UploaderMessage>;
/// Channels for downloading data from a single peer.
pub struct DownloadChannels(pub DownloadTxChannel, pub DownloadRxChannel);

/// Channel for sending messages related to us uploading data to the peer.
pub type UploadTxChannel = TxChannel<UploaderMessage>;
/// Channel for receiving messages related to the peer downloading data from us.
pub type UploadRxChannel = RxChannel<DownloaderMessage>;
/// Channels for uploading data to a single peer.
pub struct UploadChannels(pub UploadTxChannel, pub UploadRxChannel);

/// Channel for sending extended protocol messages to the peer.
pub type ExtendedTxChannel = TxChannel<(ExtendedMessage, u8)>;
/// Channel for receiving extended protocol messages from the peer.
pub type ExtendedRxChannel = RxChannel<ExtendedMessage>;
/// Channels for exchanging extended protocol messages with a single peer.
pub struct ExtendedChannels(pub ExtendedTxChannel, pub ExtendedRxChannel);

// ------

/// Actor that handles a single TCP connection to a peer.
/// Reads inbound messages from the socket, parses them, and pushes into the appropriate channel (Upload/Download/Extended).
/// Similarly, reads outbound messages from Upload/Download/Extended channels, serializes them and writes into the socket.
/// Exits on socket error or if one or more channels have been closed.
pub struct ConnectionIoDriver(BoxFuture<'static, io::Result<()>>);

impl Future for ConnectionIoDriver {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

// ------

const HANDSHAKE_TIMEOUT: Duration = sec!(10);

/// Perform handshake on an inbound connection from a peer, and set up [`PeerChannel`]s.
pub async fn channels_for_inbound_connection(
    local_peer_id: &[u8; 20],
    info_hash: Option<&[u8; 20]>,
    extension_protocol_enabled: bool,
    remote_addr: SocketAddr,
    socket: TcpStream,
) -> io::Result<(DownloadChannels, UploadChannels, Option<ExtendedChannels>, ConnectionIoDriver)> {
    let local_handshake = Handshake {
        peer_id: *local_peer_id,
        info_hash: *info_hash.unwrap_or(&[0u8; 20]),
        reserved: reserved_bits(extension_protocol_enabled),
    };
    let (socket, remote_handshake) = timeout(
        HANDSHAKE_TIMEOUT,
        do_handshake_incoming(&remote_addr, socket, &local_handshake, info_hash.is_none()),
    )
    .await??;
    Ok(setup_channels(socket, remote_addr, remote_handshake, extension_protocol_enabled))
}

/// Perform handshake on an outbound connection to a peer, and set up [`PeerChannel`]s.
pub async fn channels_for_outbound_connection(
    local_peer_id: &[u8; 20],
    info_hash: &[u8; 20],
    extension_protocol_enabled: bool,
    remote_addr: SocketAddr,
    socket: TcpStream,
    remote_peer_id: Option<&[u8; 20]>,
) -> io::Result<(DownloadChannels, UploadChannels, Option<ExtendedChannels>, ConnectionIoDriver)> {
    let local_handshake = Handshake {
        peer_id: *local_peer_id,
        info_hash: *info_hash,
        reserved: reserved_bits(extension_protocol_enabled),
    };
    let (socket, remote_handshake) = timeout(
        HANDSHAKE_TIMEOUT,
        do_handshake_outgoing(&remote_addr, socket, &local_handshake, remote_peer_id),
    )
    .await??;
    Ok(setup_channels(socket, remote_addr, remote_handshake, extension_protocol_enabled))
}

/// Set up [`PeerChannel`]s for a fake stream without performing a handshake.
#[cfg(feature = "mocks")]
pub fn channels_from_mock<S>(
    peer_addr: SocketAddr,
    remote_handshake: Handshake,
    extension_protocol_enabled: bool,
    mock_socket: S,
) -> (DownloadChannels, UploadChannels, Option<ExtendedChannels>)
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (download, upload, extensions, runner) = setup_channels(
        StreamHolder(mock_socket),
        peer_addr,
        remote_handshake,
        extension_protocol_enabled,
    );
    tokio::task::spawn(async move {
        let _ = runner.await;
    });
    (download, upload, extensions)
}

// ------

trait SplittableStream: Send + Unpin {
    type Ingress<'i>: AsyncRead + Send + Unpin
    where
        Self: 'i;
    type Egress<'e>: AsyncWrite + Send + Unpin
    where
        Self: 'e;

    fn split(&mut self) -> (Self::Ingress<'_>, Self::Egress<'_>);
}

impl SplittableStream for TcpStream {
    type Ingress<'i> = tcp::ReadHalf<'i>;
    type Egress<'e> = tcp::WriteHalf<'e>;

    fn split(&mut self) -> (Self::Ingress<'_>, Self::Egress<'_>) {
        TcpStream::split(self)
    }
}

#[cfg(any(feature = "mocks", test))]
struct StreamHolder<S>(S)
where
    S: AsyncRead + AsyncWrite + Send + Unpin;

#[cfg(any(feature = "mocks", test))]
impl<S> SplittableStream for StreamHolder<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Ingress<'i> = tokio::io::ReadHalf<&'i mut S>;
    type Egress<'e> = tokio::io::WriteHalf<&'e mut S>;

    fn split(&mut self) -> (Self::Ingress<'_>, Self::Egress<'_>) {
        tokio::io::split(&mut self.0)
    }
}

// ------

fn setup_channels<S>(
    mut stream: S,
    remote_ip: SocketAddr,
    remote_handshake: Handshake,
    extended_protocol_enabled: bool,
) -> (DownloadChannels, UploadChannels, Option<ExtendedChannels>, ConnectionIoDriver)
where
    S: SplittableStream + 'static,
{
    const MAX_INCOMING_QUEUE: usize = 20;
    const BUFFER_SIZE: usize = MAX_BLOCK_SIZE + 512; // data + some header

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

    let actor_fut = async move {
        let (ingress, egress) = stream.split();
        let receiver = IngressProcessor {
            source: BufReader::with_capacity(BUFFER_SIZE, ingress),
            remote_ip,
            ul_msg_sink: remote_uploader_msg_in,
            dl_msg_sink: remote_downloader_msg_in,
            ext_msg_sink: remote_extended_msg_in,
        };
        let sender = EgressProcessor {
            sink: BufWriter::with_capacity(BUFFER_SIZE, egress),
            remote_ip,
            dl_msg_source: local_downloader_msg_out,
            ul_msg_source: local_uploader_msg_out,
            ext_msg_source: local_extended_msg_out,
        };
        try_join!(receiver.read_messages(), sender.write_messages()).map(|_| ())
    }
    .boxed();

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
        ConnectionIoDriver(actor_fut),
    )
}

struct IngressProcessor<S: AsyncReadExt + Unpin> {
    source: BufReader<S>,
    remote_ip: SocketAddr,
    ul_msg_sink: mpsc::Sender<UploaderMessage>,
    dl_msg_sink: mpsc::Sender<DownloaderMessage>,
    ext_msg_sink: Option<mpsc::Sender<ExtendedMessage>>,
}

impl<S: AsyncReadExt + Unpin> IngressProcessor<S> {
    const RECV_TIMEOUT: Duration = sec!(120);

    async fn read_messages(mut self) -> io::Result<()> {
        loop {
            macro_rules! forward_and_continue {
                ($msg:expr, $sink:expr) => {{
                    log::trace!("{} => {}", self.remote_ip, $msg);
                    $sink
                        .send($msg)
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, Box::new(e)))?;
                    continue;
                }};
            }

            timeout(Self::RECV_TIMEOUT, self.source.fill_buf()).await??;
            let received = PeerMessage::read_from(&mut self.source).await?;

            let received = match UploaderMessage::try_from(received) {
                Ok(msg) => forward_and_continue!(msg, self.ul_msg_sink),
                Err(received) => received,
            };
            let received = match DownloaderMessage::try_from(received) {
                Ok(msg) => forward_and_continue!(msg, self.dl_msg_sink),
                Err(received) => received,
            };
            let received = if let Some(ext_msg_sink) = &mut self.ext_msg_sink {
                match ExtendedMessage::try_from(received) {
                    Ok(msg) => forward_and_continue!(msg, ext_msg_sink),
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
        }
    }
}

struct EgressProcessor<S: AsyncWriteExt + Unpin> {
    sink: BufWriter<S>,
    remote_ip: SocketAddr,
    dl_msg_source: mpsc::Receiver<Option<DownloaderMessage>>,
    ul_msg_source: mpsc::Receiver<Option<UploaderMessage>>,
    ext_msg_source: Option<mpsc::Receiver<Option<(ExtendedMessage, u8)>>>,
}

impl<S: AsyncWriteExt + Unpin> EgressProcessor<S> {
    const PING_INTERVAL: Duration = sec!(30);

    async fn write_messages(mut self) -> io::Result<()> {
        fn new_channel_closed_error() -> io::Error {
            io::Error::new(io::ErrorKind::BrokenPipe, "Channel closed")
        }

        macro_rules! process_msg {
            ($msg:expr, $source:expr $(,$proj:tt)?) => {
                if let Some(msg) = $msg {
                    let formattable = &msg;
                    $(let formattable = &formattable.$proj;)?
                    log::trace!("{} <= {}", self.remote_ip, formattable);
                    PeerMessage::from(msg).write_to(&mut self.sink).await?;
                }
            };
        }

        loop {
            let next_ext_msg_fut = async {
                if let Some(ext_src) = &mut self.ext_msg_source {
                    ext_src.next().await
                } else {
                    future::pending().await
                }
            };

            select! {
                biased;
                dl_msg = self.dl_msg_source.next() => {
                    let msg = dl_msg.ok_or_else(new_channel_closed_error)?;
                    process_msg!(msg, &mut self.dl_msg_source);
                }
                ext_msg = next_ext_msg_fut => {
                    let msg = ext_msg.ok_or_else(new_channel_closed_error)?;
                    process_msg!(msg, self.ext_msg_source.as_mut().unwrap(), 0);
                }
                ul_msg = self.ul_msg_source.next() => {
                    let msg = ul_msg.ok_or_else(new_channel_closed_error)?;
                    process_msg!(msg, &mut self.ul_msg_source);
                }
                _ = sleep(Self::PING_INTERVAL) => {
                    let ping_msg = PeerMessage::KeepAlive;
                    log::trace!("{} <= {:?}", self.remote_ip, &ping_msg);
                    ping_msg.write_to(&mut self.sink).await?;
                }
            };
        }
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
            ChannelError::ConnectionClosed => io::Error::from(io::ErrorKind::BrokenPipe),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{FutureExt, join};
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tokio::{io, task, time};
    use tokio_test::io::Builder as MockBuilder;
    use tokio_test::task::spawn;
    use tokio_test::{assert_pending, assert_ready};

    fn buffer_with(msgs: &[PeerMessage]) -> Vec<u8> {
        let mut socket = BufWriter::new(Cursor::<Vec<u8>>::default());
        for msg in msgs {
            msg.write_to(&mut socket).now_or_never().unwrap().unwrap();
        }
        socket.into_inner().into_inner()
    }

    macro_rules! msgs {
        ($($arg:expr),+ $(,)? ) => {
            buffer_with(&[$($arg),+]).as_ref()
        };
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
    impl AsyncRead for FakeSink {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending
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

    macro_rules! setup_channels {
        ($stream:expr, $($args:expr),+ $(,)?) => {
            setup_channels(StreamHolder($stream), $($args),+)
        };
    }

    #[tokio::test]
    async fn test_read_downloader_message() {
        let socket = MockBuilder::new().read(msgs![PeerMessage::Interested]).build();
        let (mut download, mut upload, extended, runner) = setup_channels!(
            socket,
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
            let _ = runner.await;
        };

        let download_fut = async move {
            let result = download.1.receive_message().await;
            assert!(matches!(result, Err(ChannelError::ConnectionClosed)));
        };

        join!(upload_fut, run_fut, download_fut);
    }

    #[tokio::test]
    async fn test_read_uploader_message() {
        let socket = MockBuilder::new().read(msgs![PeerMessage::Unchoke]).build();
        let (mut download, mut upload, extended, runner) = setup_channels!(
            socket,
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
            let _ = runner.await;
        };

        let upload_fut = async move {
            let result = upload.1.receive_message().await;
            assert!(matches!(result, Err(ChannelError::ConnectionClosed)));
        };

        join!(download_fut, run_fut, upload_fut);
    }

    #[tokio::test]
    async fn test_read_extended_message() {
        let socket = MockBuilder::new()
            .read(msgs![
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
            .build();

        let (mut download, mut upload, extended, runner) = setup_channels!(
            socket,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            HANDSHAKE_WITH_BEP10_SUPPORT,
            true,
        );
        assert!(extended.is_some());

        let extended_fut = async move {
            let ExtendedChannels(_tx, mut rx) = extended.unwrap();
            let result = rx.receive_message().await;
            let received = result.unwrap();
            let expected_data = ExtendedHandshake {
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
            let _ = runner.await;
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
        let socket = MockBuilder::new()
            .read(msgs![
                PeerMessage::KeepAlive,
                PeerMessage::Interested,
                PeerMessage::Unchoke,
                PeerMessage::KeepAlive,
                PeerMessage::Extended {
                    id: 0,
                    data: Vec::from(b"d1:md11:ut_metadatai1eee"),
                },
            ])
            .build();
        let (mut download, mut upload, extended, runner) = setup_channels!(
            socket,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            HANDSHAKE_WITH_BEP10_SUPPORT,
            true,
        );

        let upload_fut = async move {
            let result = upload.1.receive_message().await;
            assert!(matches!(result, Ok(DownloaderMessage::Interested)));
        };

        let download_fut = async move {
            let result = download.1.receive_message().await;
            assert!(matches!(result, Ok(UploaderMessage::Unchoke)));
        };

        let extended_fut = async move {
            let result = extended.unwrap().1.receive_message().await;
            let received = result.unwrap();
            let expected_data = ExtendedHandshake {
                extensions: HashMap::from([(Extension::Metadata, 1)]),
                ..Default::default()
            };
            assert!(matches!(received, ExtendedMessage::Handshake(data) if *data == expected_data));
        };

        let run_fut = async move {
            let result = runner.await;
            let error = result.unwrap_err();
            assert_eq!(io::ErrorKind::UnexpectedEof, error.kind());
        };

        join!(upload_fut, download_fut, extended_fut, run_fut);
    }

    #[tokio::test]
    async fn test_read_error() {
        let socket = MockBuilder::new()
            .read_error(io::Error::from(io::ErrorKind::OutOfMemory))
            .build();
        let (mut download, mut upload, _, runner) = setup_channels!(
            socket,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
            false,
        );

        let download_fut = async move {
            let result = download.1.receive_message().await;
            assert!(matches!(result, Err(ChannelError::ConnectionClosed)));
        };

        let upload_fut = async move {
            let result = upload.1.receive_message().await;
            assert!(matches!(result, Err(ChannelError::ConnectionClosed)));
        };

        let run_fut = async move {
            let result = runner.await;
            let error = result.unwrap_err();
            assert_eq!(io::ErrorKind::OutOfMemory, error.kind(), "{error}");
        };

        join!(download_fut, upload_fut, run_fut);
    }

    #[tokio::test]
    async fn test_write_downloader_message() {
        let socket = MockBuilder::new().write(msgs![PeerMessage::Interested]).wait(sec!(0)).build();
        let (mut download, _upload, _, runner) = setup_channels!(
            socket,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
            false,
        );

        task::spawn(async move {
            let _ = runner.await;
        });

        let result = download.0.send_message(DownloaderMessage::Interested).await;
        assert!(result.is_ok(), "{result:?}");
    }

    #[tokio::test]
    async fn test_write_uploader_message() {
        let socket = MockBuilder::new().write(msgs![PeerMessage::Unchoke]).wait(sec!(0)).build();
        let (_download, mut upload, _, runner) = setup_channels!(
            socket,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
            false,
        );

        task::spawn(async move {
            let _ = runner.await;
        });

        let result = upload.0.send_message(UploaderMessage::Unchoke).await;
        assert!(result.is_ok(), "{result:?}");
    }

    #[tokio::test]
    async fn test_write_extended_messages() {
        let socket = MockBuilder::new()
            .write(msgs![
                PeerMessage::Extended {
                    id: 1,
                    data: Vec::from("d8:msg_typei2e5:piecei3ee"),
                },
                PeerMessage::Extended {
                    id: 0,
                    data: Vec::from(
                        b"d1:md11:ut_metadatai1e6:ut_pexi2ee1:pi6881e1:v13:\xc2\xb5Torrent 1.2e"
                    ),
                }
            ])
            .wait(sec!(0))
            .build();
        let (_download, _upload, extended, runner) = setup_channels!(
            socket,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            HANDSHAKE_WITH_BEP10_SUPPORT,
            true,
        );
        assert!(extended.is_some());
        let ExtendedChannels(mut tx, _rx) = extended.unwrap();

        task::spawn(async move {
            let _ = runner.await;
        });

        let result = tx.send_message((ExtendedMessage::MetadataReject { piece: 3 }, 1)).await;
        assert!(result.is_ok());

        let hs_data = ExtendedHandshake {
            extensions: HashMap::from([(Extension::Metadata, 1), (Extension::PeerExchange, 2)]),
            listen_port: Some(6881),
            client_type: Some("µTorrent 1.2".to_owned()),
            ..Default::default()
        };
        let result = tx.send_message((ExtendedMessage::Handshake(Box::new(hs_data)), 42)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_write_error() {
        let socket = MockBuilder::new()
            .write_error(io::Error::from(io::ErrorKind::OutOfMemory))
            .build();
        let (mut download, upload, _, runner) = setup_channels!(
            socket,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
            false,
        );

        let send_msg_fut = async move {
            let result = download.0.send_message(DownloaderMessage::Interested).await;
            assert!(matches!(result, Err(ChannelError::ConnectionClosed)));
        };

        let run_fut = async move {
            let result = runner.await;
            let error = result.unwrap_err();
            assert_eq!(io::ErrorKind::OutOfMemory, error.kind(), "{error}");
        };

        join!(send_msg_fut, run_fut);
        drop(upload);
    }

    #[tokio::test]
    async fn test_writing_downloader_message_takes_priority_over_uploader_message() {
        for _ in 0..50 {
            let socket = MockBuilder::new()
                .write(msgs![PeerMessage::Interested])
                .write(msgs![PeerMessage::Piece {
                    index: 0,
                    begin: 0,
                    block: vec![0u8; 1024]
                }])
                .wait(sec!(0))
                .build();
            let (mut download, mut upload, _, runner) = setup_channels!(
                socket,
                SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
                Default::default(),
                false,
            );

            let mut send_uploader_msg_fut = spawn(upload.0.send_message(UploaderMessage::Block(
                BlockInfo {
                    piece_index: 0,
                    in_piece_offset: 0,
                    block_length: 16384,
                },
                vec![0u8; 1024],
            )));
            let mut send_downloader_msg_fut =
                spawn(download.0.send_message(DownloaderMessage::Interested));
            let mut runner_fut = spawn(runner);

            assert_pending!(send_uploader_msg_fut.poll());
            assert_pending!(send_downloader_msg_fut.poll());

            while matches!(send_uploader_msg_fut.poll(), Poll::Pending)
                && matches!(send_downloader_msg_fut.poll(), Poll::Pending)
            {
                assert_pending!(runner_fut.poll());
            }
        }
    }

    #[tokio::test]
    async fn test_writing_extended_message_takes_priority_over_uploader_message() {
        for _ in 0..50 {
            let socket = MockBuilder::new()
                .write(msgs![PeerMessage::Extended {
                    id: 0,
                    data: Vec::from(
                        b"d1:md11:ut_metadatai1e6:ut_pexi2ee1:pi6881e1:v13:\xc2\xb5Torrent 1.2e"
                    ),
                }])
                .write(msgs![PeerMessage::Bitfield {
                    bitfield: Bitfield::repeat(true, 42),
                }])
                .wait(sec!(0))
                .build();
            let (_download, mut upload, extended, runner) = setup_channels!(
                socket,
                SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
                HANDSHAKE_WITH_BEP10_SUPPORT,
                true,
            );
            let mut extended = extended.unwrap();

            let mut send_uploader_msg_fut =
                spawn(upload.0.send_message(UploaderMessage::Bitfield(Bitfield::repeat(true, 42))));
            let mut send_extended_msg_fut = spawn(extended.0.send_message((
                ExtendedMessage::Handshake(Box::new(ExtendedHandshake {
                    extensions: HashMap::from([
                        (Extension::Metadata, 1),
                        (Extension::PeerExchange, 2),
                    ]),
                    listen_port: Some(6881),
                    client_type: Some("µTorrent 1.2".to_owned()),
                    ..Default::default()
                })),
                42,
            )));
            let mut runner_fut = spawn(runner);

            assert_pending!(send_uploader_msg_fut.poll());
            assert_pending!(send_extended_msg_fut.poll());

            while matches!(send_uploader_msg_fut.poll(), Poll::Pending)
                && matches!(send_extended_msg_fut.poll(), Poll::Pending)
            {
                assert_pending!(runner_fut.poll());
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_downloader_channel_send_backpressure() {
        let socket = MockBuilder::new()
            .wait(sec!(1))
            .write(msgs![PeerMessage::Interested])
            .wait(sec!(1))
            .write(msgs![PeerMessage::NotInterested])
            .wait(sec!(1))
            .build();

        let (mut download, _upload, _, runner) = setup_channels!(
            socket,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
            false,
        );

        let mut runner_fut = spawn(runner);
        {
            let mut send_fut = spawn(download.0.send_message(DownloaderMessage::Interested));
            assert_pending!(send_fut.poll());

            assert_pending!(runner_fut.poll());
            assert_pending!(send_fut.poll());

            time::sleep(sec!(1)).await;
            assert_pending!(runner_fut.poll());
            assert!(assert_ready!(send_fut.poll()).is_ok());
        }
        {
            let mut send_fut = spawn(download.0.send_message(DownloaderMessage::NotInterested));
            assert_pending!(send_fut.poll());

            assert_pending!(runner_fut.poll());
            assert_pending!(send_fut.poll());

            time::sleep(sec!(1)).await;
            assert_pending!(runner_fut.poll());
            assert!(assert_ready!(send_fut.poll()).is_ok());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_uploader_channel_send_backpressure() {
        let socket = MockBuilder::new()
            .wait(sec!(1))
            .write(msgs![PeerMessage::Choke])
            .wait(sec!(1))
            .write(msgs![PeerMessage::Unchoke])
            .wait(sec!(1))
            .build();

        let (_download, mut upload, _, runner) = setup_channels!(
            socket,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
            false,
        );

        let mut runner_fut = spawn(runner);
        {
            let mut send_fut = spawn(upload.0.send_message(UploaderMessage::Choke));
            assert_pending!(send_fut.poll());

            assert_pending!(runner_fut.poll());
            assert_pending!(send_fut.poll());

            time::sleep(sec!(1)).await;
            assert_pending!(runner_fut.poll());
            assert!(assert_ready!(send_fut.poll()).is_ok());
        }
        {
            let mut send_fut = spawn(upload.0.send_message(UploaderMessage::Unchoke));
            assert_pending!(send_fut.poll());

            assert_pending!(runner_fut.poll());
            assert_pending!(send_fut.poll());

            time::sleep(sec!(1)).await;
            assert_pending!(runner_fut.poll());
            assert!(assert_ready!(send_fut.poll()).is_ok());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_extended_channel_send_backpressure() {
        let socket = MockBuilder::new()
            .wait(sec!(1))
            .write(msgs![PeerMessage::Extended {
                id: 1,
                data: Vec::from("d8:msg_typei2e5:piecei3ee"),
            }])
            .wait(sec!(1))
            .write(msgs![PeerMessage::Extended {
                id: 1,
                data: Vec::from("d8:msg_typei0e5:piecei3ee"),
            }])
            .wait(sec!(1))
            .build();
        let (_download, _upload, extended, runner) = setup_channels!(
            socket,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            HANDSHAKE_WITH_BEP10_SUPPORT,
            true,
        );
        let mut extended = extended.unwrap();

        let mut runner_fut = spawn(runner);
        {
            let mut send_fut =
                spawn(extended.0.send_message((ExtendedMessage::MetadataReject { piece: 3 }, 1)));
            assert_pending!(send_fut.poll());

            assert_pending!(runner_fut.poll());
            assert_pending!(send_fut.poll());

            time::sleep(sec!(1)).await;
            assert_pending!(runner_fut.poll());
            assert!(assert_ready!(send_fut.poll()).is_ok());
        }
        {
            let mut send_fut =
                spawn(extended.0.send_message((ExtendedMessage::MetadataRequest { piece: 3 }, 1)));
            assert_pending!(send_fut.poll());

            assert_pending!(runner_fut.poll());
            assert_pending!(send_fut.poll());

            time::sleep(sec!(1)).await;
            assert_pending!(runner_fut.poll());
            assert!(assert_ready!(send_fut.poll()).is_ok());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_clone_channel_and_send_msgs_concurrently() {
        let socket = MockBuilder::new()
            .write(msgs![PeerMessage::Have { piece_index: 0 }])
            .write(msgs![PeerMessage::Unchoke])
            .wait(sec!(1))
            .build();

        let (_download, UploadChannels(mut tx, _), _, runner) = setup_channels!(
            socket,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
            false,
        );
        let mut runner_fut = spawn(runner);

        let mut tx_clone = tx.clone();

        let mut send_have_fut = spawn(tx.send_message(UploaderMessage::Have { piece_index: 0 }));
        assert_pending!(send_have_fut.poll());

        let mut send_unchoke_fut = spawn(tx_clone.send_message(UploaderMessage::Unchoke));
        assert_pending!(send_unchoke_fut.poll());

        assert_pending!(runner_fut.poll()); // this used to panic
        assert_pending!(send_have_fut.poll());
        assert_pending!(send_unchoke_fut.poll());

        assert_pending!(runner_fut.poll());
        assert_ready!(send_have_fut.poll()).expect("send_message() returned Error");
        assert_ready!(send_unchoke_fut.poll()).expect("send_message() returned Error");
    }

    #[tokio::test(start_paused = true)]
    async fn test_send_keepalive_every_30s() {
        task::LocalSet::new()
            .run_until(async {
                let mut buf = Vec::<u8>::new();
                let (writer, mut reader) = mpsc::unbounded::<u8>();

                let (_download, _upload, _, runner) = setup_channels!(
                    FakeSink(writer),
                    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
                    Default::default(),
                    false,
                );

                task::spawn_local(async move {
                    let _ = runner.await;
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
        let (sock1, _sock2) = io::duplex(0);
        let (_download, _upload, _, runner) = setup_channels!(
            sock1,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
            false,
        );

        let (mut result_sender, mut result_receiver) = mpsc::channel::<io::Result<()>>(1);

        task::spawn(async move {
            let result = runner.await;
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
    }

    #[tokio::test(start_paused = true)]
    async fn test_channel_send_timeout() {
        task::LocalSet::new()
            .run_until(async {
                const TIMEOUT: Duration = sec!(10);

                let (sock1, _sock2) = io::duplex(0);
                let (mut download, mut _upload, _, _runner) = setup_channels!(
                    sock1,
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

                let (sock1, _sock2) = io::duplex(0);
                let (mut _download, mut upload, _, _runner) = setup_channels!(
                    sock1,
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

    #[tokio::test(start_paused = true)]
    async fn test_channel_receive_zero_timeout() {
        let socket = MockBuilder::new()
            .read(msgs![
                PeerMessage::Have { piece_index: 42 },
                PeerMessage::Have { piece_index: 43 },
            ])
            .wait(sec!(0))
            .build();

        let (mut download, _upload, _, runner) = setup_channels!(
            socket,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
            false,
        );

        task::spawn(async move {
            let _ = runner.await;
        });

        task::yield_now().await;

        let res = download.1.receive_message_timed(sec!(0)).await;
        let msg = res.unwrap();
        assert!(matches!(msg, UploaderMessage::Have { piece_index: 42 }));

        let res = download.1.receive_message_timed(sec!(0)).await;
        let msg = res.unwrap();
        assert!(matches!(msg, UploaderMessage::Have { piece_index: 43 }));

        let res = download.1.receive_message_timed(sec!(0)).await;
        let err = res.unwrap_err();
        assert!(matches!(err, ChannelError::Timeout));
    }
}
