mod connection;
mod driver;
mod protocol;
mod utils;

use futures_util::{FutureExt, Stream, StreamExt};
use local_async_utils::prelude::local_bounded;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;
use tokio::net::UdpSocket;

pub use local_async_utils::prelude::local_pipe;

type Connection = connection::Connection<local_pipe::DuplexEnd>;
type InboundConnect = driver::InboundConnect<Connection>;

/// Initialize the uTP subsystem.
pub fn init(socket: UdpSocket) -> (ConnectReporter, ConnectionCreator, IoDriver) {
    let (inbound_tx, inbound_rx) = local_bounded::channel::<InboundConnect>(64);
    let (command_tx, command_rx) = local_bounded::channel::<driver::Command<Connection>>(1);
    let driver = driver::IoDriver::new(command_rx, inbound_tx, socket);
    (ConnectReporter(inbound_rx), ConnectionCreator(command_tx), IoDriver(driver))
}

/// Opaque data for an inbound connection attempt.
#[derive(Debug, Clone)]
pub struct InboundConnectData(<Connection as driver::PeerConnection>::InboundInitData);

/// Stream of inbound connection attempts.
pub struct ConnectReporter(local_bounded::Receiver<InboundConnect>);

impl Stream for ConnectReporter {
    type Item = (SocketAddr, InboundConnectData);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let inbound = ready!(self.get_mut().0.poll_next_unpin(cx));
        Poll::Ready(inbound.map(|inbound| (inbound.source_addr, InboundConnectData(inbound.data))))
    }
}

/// Handle for creating new uTP connections.
pub struct ConnectionCreator(local_bounded::Sender<driver::Command<Connection>>);

impl ConnectionCreator {
    const PIPE_CAPACITY: usize = crate::pwp::MAX_BLOCK_SIZE;

    /// Create a new outbound connection.
    /// # Returns
    /// `None` if `IoDriver` has been shut down, otherwise a stream for the new connection.
    pub async fn new_outbound(
        &mut self,
        remote_addr: SocketAddr,
        timeout: Duration,
    ) -> Option<local_pipe::DuplexEnd> {
        let (left, right) = local_pipe::duplex_pipe(Self::PIPE_CAPACITY);
        self.0
            .send(driver::Command::AddConnection(Box::pin(Connection::new_outbound(
                left,
                remote_addr,
                timeout,
            ))))
            .await
            .ok()?;
        Some(right)
    }

    /// Create a new inbound connection.
    /// # Returns
    /// `None` if `IoDriver` has been shut down, otherwise a stream for the new connection.
    pub async fn new_inbound(
        &mut self,
        remote_addr: SocketAddr,
        data: InboundConnectData,
        timeout: Duration,
    ) -> Option<local_pipe::DuplexEnd> {
        let (left, right) = local_pipe::duplex_pipe(Self::PIPE_CAPACITY);
        self.0
            .send(driver::Command::AddConnection(Box::pin(Connection::new_inbound(
                left,
                remote_addr,
                data.0,
                timeout,
            ))))
            .await
            .ok()?;
        Some(right)
    }
}

/// Actor driving the uTP protocol.
pub struct IoDriver(driver::IoDriver<Connection>);

impl Future for IoDriver {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.poll_unpin(cx)
    }
}
