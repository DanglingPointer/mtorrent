mod connection;
mod protocol;
mod retransmitter;
mod seq;
mod udp;

#[cfg(test)]
mod tests;

use bytes::Bytes;
use connection::Connection;
use futures_util::{FutureExt, Stream, StreamExt};
use local_async_utils::prelude::*;
use log::log_enabled;
use protocol::{Header, TypeVer, dbg_hdr_and_ext};
use std::hash::RandomState;
use std::io;
use std::net::SocketAddr;
use std::ops::Deref;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, ready};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task;

pub use udp::UdpDemux;

/// Initialize the uTP subsystem.
pub fn init(socket: UdpSocket) -> (ConnectionSpawner, ConnectReporter, UdpDemux) {
    let (cmd_sender, cmd_receiver) = mpsc::channel(1);
    let (connect_sender, connect_receiver) = local_bounded::channel(64);

    (
        ConnectionSpawner {
            cmds: cmd_sender,
            hasher_state: Rc::new(RandomState::new()),
        },
        ConnectReporter(connect_receiver),
        UdpDemux::new(cmd_receiver, socket, connect_sender),
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
            if let Ok(header) = Header::decode_from(&mut packet) {
                if log_enabled!(log::Level::Trace) {
                    log::trace!(
                        "ConnectReporter got {:?} from {source}",
                        dbg_hdr_and_ext(&header, &packet)
                    );
                }
                if let TypeVer::Syn = header.type_ver() {
                    return Poll::Ready(Some((source, InboundConnectData(header))));
                }
            }
        }
    }
}

/// Handle for creating new uTP connections.
#[derive(Clone)]
pub struct ConnectionSpawner {
    cmds: mpsc::Sender<udp::Command>,
    hasher_state: Rc<RandomState>,
}

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
        let handle = udp::ConnectionHandle {
            egress: egress_receiver,
            ingress: ingress_sender,
        };
        self.cmds
            .send(udp::Command::AddConnection((remote_addr, handle)))
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;

        let connection = Connection::outbound(
            remote_addr,
            right,
            ingress_receiver,
            egress_sender,
            self.hasher_state.deref(),
        )
        .await?;
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
        let handle = udp::ConnectionHandle {
            egress: egress_receiver,
            ingress: ingress_sender,
        };
        self.cmds
            .send(udp::Command::AddConnection((remote_addr, handle)))
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;

        let connection =
            Connection::inbound(remote_addr, right, ingress_receiver, egress_sender, syn).await?;
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
        let _ = self.cmds.send(udp::Command::ResetConnections).await;
    }
}
