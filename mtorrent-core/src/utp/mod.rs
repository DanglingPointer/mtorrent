mod connection;
mod driver;
mod packets;
mod protocol;
mod retransmitter;

use bytes::Bytes;
use connection::Connection;
use futures_util::{Stream, StreamExt, TryFutureExt};
use local_async_utils::prelude::*;
use protocol::Header;
use protocol::TypeVer;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::{task, time};

pub use driver::IoDriver;

/// Initialize the uTP subsystem.
pub fn init(socket: UdpSocket) -> (ConnectionSpawner, ConnectReporter, IoDriver) {
    let (cmd_sender, cmd_receiver) = local_bounded::channel(1);
    let (connect_sender, connect_receiver) = local_bounded::channel(64);

    (
        ConnectionSpawner(cmd_sender),
        ConnectReporter(connect_receiver),
        IoDriver::new(cmd_receiver, socket, connect_sender),
    )
}

/// Opaque data for an inbound connection attempt.
#[derive(Debug, Clone)]
pub struct InboundConnectData(Header);

/// Stream of inbound connection attempts (i.e. received SYN packets that don't belong to an existing connection).
pub struct ConnectReporter(local_bounded::Receiver<(SocketAddr, Bytes)>);

impl Stream for ConnectReporter {
    type Item = (SocketAddr, InboundConnectData);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Self(receiver) = self.get_mut();
        loop {
            let inbound = ready!(receiver.poll_next_unpin(cx));
            let Some((source, mut packet)) = inbound else {
                return Poll::Ready(None);
            };
            if let Ok(header) = Header::decode_from(&mut packet)
                && let TypeVer::Syn = header.type_ver()
            {
                return Poll::Ready(Some((source, InboundConnectData(header))));
            }
        }
    }
}

/// Handle for creating new uTP connections.
pub struct ConnectionSpawner(local_bounded::Sender<driver::Command>);

impl ConnectionSpawner {
    const PIPE_CAPACITY: usize = crate::pwp::MAX_BLOCK_SIZE;
    const INGRESS_QUEUE: usize = 64;

    /// Create a new outbound connection.
    /// # Returns
    /// `None` if `IoDriver` has been shut down, otherwise a stream for the new connection.
    pub async fn spawn_outbound(
        &mut self,
        remote_addr: SocketAddr,
        timeout: Duration,
    ) -> Option<local_pipe::DuplexEnd> {
        let (left, right) = local_pipe::duplex_pipe(Self::PIPE_CAPACITY);
        let (egress_sender, egress_receiver) = local_bounded::channel(1);
        let (ingress_sender, ingress_receiver) = local_bounded::channel(Self::INGRESS_QUEUE);
        let handle = driver::ConnectionHandle {
            egress: egress_receiver,
            ingress: ingress_sender,
        };
        self.0.send(driver::Command::AddConnection((remote_addr, handle))).await.ok()?;
        task::spawn_local(
            async move {
                let connection = time::timeout(
                    timeout,
                    Connection::outbound(right, ingress_receiver, egress_sender),
                )
                .await??;
                connection.run().await
            }
            .inspect_err(move |e| {
                log::error!("Outbound connection to {remote_addr} exited with error: {e}")
            }),
        );
        Some(left)
    }

    /// Create a new inbound connection.
    /// # Returns
    /// `None` if `IoDriver` has been shut down, otherwise a stream for the new connection.
    pub async fn spawn_inbound(
        &mut self,
        remote_addr: SocketAddr,
        data: InboundConnectData,
        timeout: Duration,
    ) -> Option<local_pipe::DuplexEnd> {
        let InboundConnectData(syn) = data;
        let (left, right) = local_pipe::duplex_pipe(Self::PIPE_CAPACITY);
        let (egress_sender, egress_receiver) = local_bounded::channel(1);
        let (ingress_sender, ingress_receiver) = local_bounded::channel(Self::INGRESS_QUEUE);
        let handle = driver::ConnectionHandle {
            egress: egress_receiver,
            ingress: ingress_sender,
        };
        self.0.send(driver::Command::AddConnection((remote_addr, handle))).await.ok()?;
        task::spawn_local(
            async move {
                let connection = time::timeout(
                    timeout,
                    Connection::inbound(right, ingress_receiver, egress_sender, syn),
                )
                .await??;
                connection.run().await
            }
            .inspect_err(move |e| {
                log::error!("Inbound connection from {remote_addr} exited with error: {e}")
            }),
        );
        Some(left)
    }
}
