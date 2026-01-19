mod connection;
mod driver;
mod protocol;
mod retransmitter;
mod seq;

#[cfg(test)]
mod tests;

use bytes::Bytes;
use connection::Connection;
use futures_util::{FutureExt, Stream, StreamExt};
use local_async_utils::prelude::*;
use protocol::Header;
use protocol::TypeVer;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task;

pub use driver::IoDriver;

/// Initialize the uTP subsystem.
pub fn init(socket: UdpSocket) -> (ConnectionSpawner, ConnectReporter, IoDriver) {
    let (cmd_sender, cmd_receiver) = mpsc::channel(1);
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
#[derive(Clone)]
pub struct ConnectionSpawner(mpsc::Sender<driver::Command>);

impl ConnectionSpawner {
    const PIPE_CAPACITY: usize = crate::pwp::MAX_BLOCK_SIZE;
    const INGRESS_QUEUE: usize = 64;

    /// Create a new outbound connection.
    /// # Returns
    /// `None` if `IoDriver` has been shut down, otherwise a stream for the new connection.
    pub async fn outbound_connection(
        &self,
        remote_addr: SocketAddr,
    ) -> io::Result<local_pipe::DuplexEnd> {
        let (left, right) = local_pipe::duplex_pipe(Self::PIPE_CAPACITY);
        let (egress_sender, egress_receiver) = local_bounded::channel(1);
        let (ingress_sender, ingress_receiver) = local_bounded::channel(Self::INGRESS_QUEUE);
        let handle = driver::ConnectionHandle {
            egress: egress_receiver,
            ingress: ingress_sender,
        };
        self.0
            .send(driver::Command::AddConnection((remote_addr, handle)))
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;

        let connection = Connection::outbound(right, ingress_receiver, egress_sender).await?;
        log::debug!("Outbound connection to {remote_addr} established");

        task::spawn_local(connection.run().inspect(move |result| match result {
            Ok(()) => {
                log::debug!("Outbound connection to {remote_addr} closed");
            }
            Err(e) => {
                log::error!("Outbound connection to {remote_addr} exited with error: {e}");
            }
        }));
        Ok(left)
    }

    /// Create a new inbound connection.
    /// # Returns
    /// `None` if `IoDriver` has been shut down, otherwise a stream for the new connection.
    pub async fn inbound_connection(
        &self,
        remote_addr: SocketAddr,
        data: InboundConnectData,
    ) -> io::Result<local_pipe::DuplexEnd> {
        let InboundConnectData(syn) = data;
        let (left, right) = local_pipe::duplex_pipe(Self::PIPE_CAPACITY);
        let (egress_sender, egress_receiver) = local_bounded::channel(1);
        let (ingress_sender, ingress_receiver) = local_bounded::channel(Self::INGRESS_QUEUE);
        let handle = driver::ConnectionHandle {
            egress: egress_receiver,
            ingress: ingress_sender,
        };
        self.0
            .send(driver::Command::AddConnection((remote_addr, handle)))
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;

        let connection = Connection::inbound(right, ingress_receiver, egress_sender, syn).await?;
        log::debug!("Inbound connection from {remote_addr} established");

        task::spawn_local(connection.run().inspect(move |result| match result {
            Ok(()) => {
                log::debug!("Inbound connection from {remote_addr} closed");
            }
            Err(e) => {
                log::error!("Inbound connection from {remote_addr} exited with error: {e}");
            }
        }));
        Ok(left)
    }

    /// Close all connections by sending RST packets.
    pub async fn reset_connections(&self) {
        let _ = self.0.send(driver::Command::ResetConnections).await;
    }
}
