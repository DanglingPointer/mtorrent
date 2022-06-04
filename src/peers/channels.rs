use crate::peers::handshake::*;
use crate::peers::message::*;
use async_io::Async;
use bitvec::array::BitArray;
use futures::channel::mpsc;
use futures::io::BufWriter;
use futures::prelude::*;
use futures::select;
use log::debug;
use std::net::{SocketAddr, TcpStream};
use std::rc::Rc;
use std::time::Duration;
use std::{fmt, io};

pub enum ChannelError {
    Timeout,
    ConnectionClosed,
}

pub struct Channel<Rx, Tx> {
    inbound: mpsc::Receiver<Rx>,
    outbound: mpsc::Sender<Option<Tx>>,
    info: Rc<PeerInfo>,
}

impl<Rx, Tx> Channel<Rx, Tx> {
    pub async fn receive_message(&mut self) -> Result<Rx, ChannelError> {
        self.inbound.next().await.ok_or(ChannelError::ConnectionClosed)
    }
    pub async fn receive_message_timed(&mut self, timeout: Duration) -> Result<Rx, ChannelError> {
        select! {
            msg = self.receive_message().fuse() => msg,
            error = delayed(ChannelError::Timeout, timeout).fuse() => Err(error),
        }
    }
    pub async fn send_message(&mut self, msg: Tx) -> Result<(), ChannelError> {
        self.outbound.send(Some(msg)).await?;
        self.outbound.send(None).await?;
        Ok(())
    }
    pub async fn send_message_timed(
        &mut self,
        msg: Tx,
        timeout: Duration,
    ) -> Result<(), ChannelError> {
        select! {
            res = self.send_message(msg).fuse() => res,
            expired = delayed(ChannelError::Timeout, timeout).fuse() => Err(expired)
        }
    }
    pub fn remote_ip(&self) -> &SocketAddr {
        &self.info.remote_addr
    }
    pub fn remote_info(&self) -> &Handshake {
        &self.info.handshake_info
    }
}

pub type DownloadChannel = Channel<UploaderMessage, DownloaderMessage>;
pub type UploadChannel = Channel<DownloaderMessage, UploaderMessage>;

pub struct Runner<I, E>
where
    I: futures::AsyncReadExt + Unpin,
    E: futures::AsyncWriteExt + Unpin,
{
    receiver: IngressStream<I>,
    sender: EgressStream<E>,
}

impl<I, E> Runner<I, E>
where
    I: futures::AsyncReadExt + Unpin,
    E: futures::AsyncWriteExt + Unpin,
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

pub type ConnectionRunner = Runner<Async<TcpStream>, Async<TcpStream>>;

pub async fn establish_inbound(
    local_peer_id: &[u8; 20],
    info_hash: Option<&[u8; 20]>,
    socket: Async<TcpStream>,
) -> io::Result<(DownloadChannel, UploadChannel, ConnectionRunner)> {
    let local_handshake = Handshake {
        peer_id: *local_peer_id,
        info_hash: *info_hash.unwrap_or(&[0u8; 20]),
        reserved: BitArray::ZERO,
    };
    let (socket, remote_handshake) =
        do_handshake_incoming(socket, &local_handshake, info_hash.is_none()).await?;

    let remote_ip = socket.get_ref().peer_addr()?;
    let socket_copy = Async::<TcpStream>::new(socket.get_ref().try_clone()?)?;

    Ok(setup_channels(socket, socket_copy, remote_ip, remote_handshake))
}

pub async fn establish_outbound(
    local_peer_id: &[u8; 20],
    info_hash: &[u8; 20],
    remote_addr: SocketAddr,
    remote_peer_id: Option<&[u8; 20]>,
) -> io::Result<(DownloadChannel, UploadChannel, ConnectionRunner)> {
    let local_handshake = Handshake {
        peer_id: *local_peer_id,
        info_hash: *info_hash,
        reserved: BitArray::ZERO,
    };
    let socket = Async::<TcpStream>::connect(remote_addr).await?;
    let (socket, remote_handshake) =
        do_handshake_outgoing(socket, &local_handshake, remote_peer_id).await?;

    let remote_ip = socket.get_ref().peer_addr()?;
    let socket_copy = Async::<TcpStream>::new(socket.get_ref().try_clone()?)?;

    Ok(setup_channels(socket, socket_copy, remote_ip, remote_handshake))
}

// ------

struct PeerInfo {
    handshake_info: Handshake,
    remote_addr: SocketAddr,
}

fn setup_channels<I, E>(
    ingress: I,
    egress: E,
    remote_ip: SocketAddr,
    remote_handshake: Handshake,
) -> (DownloadChannel, UploadChannel, Runner<I, E>)
where
    I: futures::AsyncReadExt + Unpin,
    E: futures::AsyncWriteExt + Unpin,
{
    let info = Rc::new(PeerInfo {
        handshake_info: remote_handshake,
        remote_addr: remote_ip,
    });

    let (local_uploader_msg_in, local_uploader_msg_out) =
        mpsc::channel::<Option<UploaderMessage>>(0);
    let (local_downloader_msg_in, local_downloader_msg_out) =
        mpsc::channel::<Option<DownloaderMessage>>(0);

    let (remote_uploader_msg_in, remote_uploader_msg_out) = mpsc::channel::<UploaderMessage>(0);
    let (remote_downloader_msg_in, remote_downloader_msg_out) =
        mpsc::channel::<DownloaderMessage>(0);

    let receiver = IngressStream {
        source: ingress,
        remote_ip,
        rx_inbound: remote_uploader_msg_in,
        tx_inbound: remote_downloader_msg_in,
    };
    let sender = EgressStream {
        sink: BufWriter::new(egress),
        remote_ip,
        rx_outbound: local_downloader_msg_out,
        tx_outbound: local_uploader_msg_out,
    };

    let rx_channel = DownloadChannel {
        inbound: remote_uploader_msg_out,
        outbound: local_downloader_msg_in,
        info: info.clone(),
    };
    let tx_channel = UploadChannel {
        inbound: remote_downloader_msg_out,
        outbound: local_uploader_msg_in,
        info: info.clone(),
    };

    (rx_channel, tx_channel, Runner { receiver, sender })
}

struct IngressStream<S: futures::AsyncReadExt + Unpin> {
    source: S,
    remote_ip: SocketAddr,
    rx_inbound: mpsc::Sender<UploaderMessage>,
    tx_inbound: mpsc::Sender<DownloaderMessage>,
}

impl<S: futures::AsyncReadExt + Unpin> IngressStream<S> {
    async fn read_one_message(&mut self) -> io::Result<()> {
        let timeout = Duration::from_secs(120);

        async fn forward_msg<M: fmt::Display>(
            msg: M,
            sink: &mut mpsc::Sender<M>,
            source: &SocketAddr,
        ) -> io::Result<()> {
            debug!("{} => {}", source, msg);
            sink.send(msg).await.map_err(|_| io::Error::from(io::ErrorKind::Other))?;
            Ok(())
        }

        let received: PeerMessage = select! {
            msg_res = PeerMessage::read_from(&mut self.source).fuse() => {
                msg_res
            }
            e = delayed(io::Error::from(io::ErrorKind::TimedOut), timeout).fuse() => {
                Err(e)
            }
        }?;
        let received = match UploaderMessage::try_from(received) {
            Ok(msg) => {
                return forward_msg(msg, &mut self.rx_inbound, &self.remote_ip).await;
            }
            Err(received) => received,
        };
        let received = match DownloaderMessage::try_from(received) {
            Ok(msg) => {
                return forward_msg(msg, &mut self.tx_inbound, &self.remote_ip).await;
            }
            Err(received) => received,
        };
        debug!("{} => IGNORED {:?}", self.remote_ip, received);
        Ok(())
    }
}

struct EgressStream<S: futures::AsyncWriteExt + Unpin> {
    sink: BufWriter<S>,
    remote_ip: SocketAddr,
    rx_outbound: mpsc::Receiver<Option<DownloaderMessage>>,
    tx_outbound: mpsc::Receiver<Option<UploaderMessage>>,
}

impl<S: futures::AsyncWriteExt + Unpin> EgressStream<S> {
    async fn write_one_message(&mut self) -> io::Result<()> {
        let ping_period = Duration::from_secs(30);

        async fn process_msg<M, S>(
            msg: Option<M>,
            source: &mut mpsc::Receiver<Option<M>>,
            sink: &mut BufWriter<S>,
            dest: &SocketAddr,
        ) -> io::Result<()>
        where
            M: Into<PeerMessage> + fmt::Display,
            S: futures::AsyncWriteExt + Unpin,
        {
            let first = msg.expect("First msg must be non-None");
            debug!("{} <= {}", dest, first);
            first.into().write_to(sink).await?;
            let second = source.next().await.ok_or(io::Error::from(io::ErrorKind::Other))?;
            assert!(second.is_none(), "Second msg must be None");
            Ok(())
        }

        select! {
            rx_msg = self.rx_outbound.next().fuse() => {
                let msg = rx_msg.ok_or(io::Error::from(io::ErrorKind::Other))?;
                process_msg(msg, &mut self.rx_outbound, &mut self.sink, &self.remote_ip).await?;
            }
            tx_msg = self.tx_outbound.next().fuse() => {
                let msg = tx_msg.ok_or(io::Error::from(io::ErrorKind::Other))?;
                process_msg(msg, &mut self.tx_outbound, &mut self.sink, &self.remote_ip).await?;
            }
            ping_msg = delayed(PeerMessage::KeepAlive, ping_period).fuse() => {
                debug!("{} <= KeepAlive", self.remote_ip);
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

#[cfg(not(test))]
async fn delayed<R>(what: R, by: Duration) -> R {
    async_io::Timer::after(by).await;
    what
}

#[cfg(test)]
async fn delayed<R>(what: R, by: Duration) -> R {
    let should_fire = tests::TIME_BUDGET.with(|time| {
        let prev_time = time.get();
        if prev_time >= by {
            time.set(prev_time - by);
            true
        } else {
            false
        }
    });
    if should_fire {
        std::future::ready(what).await
    } else {
        std::future::pending::<R>().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::LocalPool;
    use futures::io::Cursor;
    use futures::join;
    use futures::task::LocalSpawnExt;
    use std::cell::{Cell, RefCell};
    use std::io::SeekFrom;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    thread_local! {
        pub static TIME_BUDGET: Cell<Duration> = Cell::new(Duration::default());
    }

    struct ErrorStream;
    impl ErrorStream {
        const ERROR_KIND: io::ErrorKind = io::ErrorKind::OutOfMemory;
    }
    impl futures::AsyncRead for ErrorStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::from(Self::ERROR_KIND)))
        }
    }
    impl futures::AsyncWrite for ErrorStream {
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
        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    struct PendingStream;
    impl futures::AsyncRead for PendingStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
    }
    impl futures::AsyncWrite for PendingStream {
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
        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    type FakeSocket = Cursor<Vec<u8>>;

    fn fake_socket_containing(msgs: Vec<PeerMessage>) -> FakeSocket {
        async_io::block_on(async {
            let mut socket = BufWriter::new(FakeSocket::default());
            for msg in msgs {
                msg.write_to(&mut socket).await.unwrap();
            }
            socket.seek(SeekFrom::Start(0)).await.unwrap();
            socket.into_inner()
        })
    }

    #[test]
    fn test_read_downloader_message() {
        let ingress = fake_socket_containing(vec![PeerMessage::Interested]);
        let (mut download, mut upload, mut runner) = setup_channels(
            ingress,
            PendingStream {},
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
        );

        let upload_fut = async move {
            let result = upload.receive_message().await;
            assert!(matches!(result, Ok(DownloaderMessage::Interested)));
        };

        let run_fut = async move {
            runner.receiver.read_one_message().await.unwrap();
        };

        let download_fut = async move {
            let result = download.receive_message().await;
            assert!(matches!(result, Err(ChannelError::ConnectionClosed)));
        };

        async_io::block_on(async { join!(upload_fut, run_fut, download_fut) });
    }

    #[test]
    fn test_read_uploader_message() {
        let ingress = fake_socket_containing(vec![PeerMessage::Unchoke]);
        let (mut download, mut upload, mut runner) = setup_channels(
            ingress,
            PendingStream {},
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
        );

        let download_fut = async move {
            let result = download.receive_message().await;
            assert!(matches!(result, Ok(UploaderMessage::Unchoke)));
        };

        let run_fut = async move {
            runner.receiver.read_one_message().await.unwrap();
        };

        let upload_fut = async move {
            let result = upload.receive_message().await;
            assert!(matches!(result, Err(ChannelError::ConnectionClosed)));
        };

        async_io::block_on(async { join!(download_fut, run_fut, upload_fut) });
    }

    #[test]
    fn test_read_uploader_and_downloader_messages() {
        let ingress = fake_socket_containing(vec![
            PeerMessage::KeepAlive,
            PeerMessage::Interested,
            PeerMessage::Unchoke,
            PeerMessage::KeepAlive,
        ]);
        let (mut download, mut upload, runner) = setup_channels(
            ingress,
            PendingStream {},
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
        );

        let upload_fut = async {
            let result = upload.receive_message().await;
            assert!(matches!(result, Ok(DownloaderMessage::Interested)));
        };

        let download_fut = async {
            let result = download.receive_message().await;
            assert!(matches!(result, Ok(UploaderMessage::Unchoke)));
        };

        let run_fut = async move {
            let result = runner.run().await;
            let error = result.unwrap_err();
            assert_eq!(io::ErrorKind::UnexpectedEof, error.kind());
        };

        async_io::block_on(async { join!(upload_fut, download_fut, run_fut) });
    }

    #[test]
    fn test_read_error() {
        let (mut download, mut upload, runner) = setup_channels(
            ErrorStream {},
            PendingStream {},
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
        );

        let download_fut = async {
            let result = download.receive_message().await;
            assert!(matches!(result, Err(ChannelError::ConnectionClosed)));
        };

        let upload_fut = async {
            let result = upload.receive_message().await;
            assert!(matches!(result, Err(ChannelError::ConnectionClosed)));
        };

        let run_fut = async {
            let result = runner.run().await;
            let error = result.unwrap_err();
            assert_eq!(ErrorStream::ERROR_KIND, error.kind(), "{}", error);
        };

        async_io::block_on(async { join!(download_fut, upload_fut, run_fut) });
    }

    #[test]
    fn test_write_downloader_message() {
        let (mut download, upload, mut runner) = setup_channels(
            PendingStream {},
            FakeSocket::default(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
        );

        let send_msg_fut = async {
            let result = download.send_message(DownloaderMessage::Interested).await;
            assert!(result.is_ok());
        };

        let run_fut = async {
            runner.sender.write_one_message().await.unwrap();
            assert!(!runner.sender.sink.get_ref().get_ref().is_empty());

            runner.sender.sink.seek(SeekFrom::Start(0)).await.unwrap();
            let sent_message = PeerMessage::read_from(&mut runner.sender.sink).await.unwrap();
            assert!(matches!(sent_message, PeerMessage::Interested));
        };

        async_io::block_on(async { join!(send_msg_fut, run_fut) });
        drop(upload);
    }

    #[test]
    fn test_write_uploader_message() {
        let (download, mut upload, mut runner) = setup_channels(
            PendingStream {},
            FakeSocket::default(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
        );

        let send_msg_fut = async {
            let result = upload.send_message(UploaderMessage::Unchoke).await;
            assert!(result.is_ok());
        };

        let run_fut = async {
            runner.sender.write_one_message().await.unwrap();
            assert!(!runner.sender.sink.get_ref().get_ref().is_empty());

            runner.sender.sink.seek(SeekFrom::Start(0)).await.unwrap();
            let sent_message = PeerMessage::read_from(&mut runner.sender.sink).await.unwrap();
            assert!(matches!(sent_message, PeerMessage::Unchoke));
        };

        async_io::block_on(async { join!(send_msg_fut, run_fut) });
        drop(download);
    }

    #[test]
    fn test_write_error() {
        let (mut download, upload, runner) = setup_channels(
            PendingStream {},
            ErrorStream {},
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
        );

        let send_msg_fut = async {
            let result = download.send_message(DownloaderMessage::Interested).await;
            assert!(matches!(result, Err(ChannelError::ConnectionClosed)));
        };

        let run_fut = async {
            let result = runner.run().await;
            let error = result.unwrap_err();
            assert_eq!(ErrorStream::ERROR_KIND, error.kind(), "{}", error);
        };

        async_io::block_on(async { join!(send_msg_fut, run_fut) });
        drop(upload);
    }

    #[test]
    fn test_send_keepalive_every_30s() {
        let (_download, _upload, runner) = setup_channels(
            PendingStream {},
            FakeSocket::default(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
        );

        let runner = Rc::new(RefCell::new(runner));
        let runner_copy = runner.clone();

        TIME_BUDGET.with(|time| time.set(Duration::from_secs(60)));

        let mut pool = LocalPool::new();
        pool.spawner()
            .spawn_local(async move {
                loop {
                    runner_copy.borrow_mut().sender.write_one_message().await.unwrap();
                }
            })
            .unwrap();
        pool.run_until_stalled();
        drop(pool);

        let two_keepalives =
            fake_socket_containing(vec![PeerMessage::KeepAlive, PeerMessage::KeepAlive])
                .into_inner();
        assert_eq!(&two_keepalives, runner.borrow().sender.sink.get_ref().get_ref());
    }

    #[test]
    fn test_receiver_times_out_after_2_min() {
        let (_download, _upload, mut runner) = setup_channels(
            PendingStream {},
            FakeSocket::default(),
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
        );

        TIME_BUDGET.with(|time| time.set(Duration::from_secs(120)));

        async_io::block_on(async move {
            let result = runner.receiver.read_one_message().await;
            let error = result.unwrap_err();
            assert_eq!(io::ErrorKind::TimedOut, error.kind());
        });
    }

    #[test]
    fn test_channel_send_receive_timeout() {
        let (mut download, mut upload, _runner) = setup_channels(
            PendingStream {},
            PendingStream {},
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
            Default::default(),
        );

        TIME_BUDGET.with(|time| time.set(Duration::from_secs(20)));

        let fut_download_send = async {
            let result = download
                .send_message_timed(DownloaderMessage::NotInterested, Duration::from_secs(10))
                .await;
            assert!(matches!(result, Err(ChannelError::Timeout)));
        };

        let fut_upload_receive = async {
            let result = upload.receive_message_timed(Duration::from_secs(10)).await;
            assert!(matches!(result, Err(ChannelError::Timeout)));
        };

        async_io::block_on(async { join!(fut_download_send, fut_upload_receive) });
    }
}
