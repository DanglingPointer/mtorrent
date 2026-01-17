use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use local_async_utils::prelude::*;
use mtorrent_utils::loop_select::loop_select;
use std::collections::hash_map::{Entry, HashMap};
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::task::{Context, Poll};
use tokio::net::UdpSocket;

pub(super) enum Command {
    AddConnection((SocketAddr, ConnectionHandle)),
    ResetAll,
}

pub(super) struct ConnectionHandle {
    pub(super) egress: local_bounded::Receiver<Bytes>,
    pub(super) ingress: local_bounded::Sender<Bytes>,
}

/// [`IoDriver`] is responsible for sending and receiving UDP packets
/// and dispatching them to the appropriate connections.
pub struct IoDriver {
    commands: local_bounded::Receiver<Command>,
    socket: UdpSocket,
    connections: HashMap<SocketAddr, ConnectionHandle>,
    pending_tx: Vec<(SocketAddr, Bytes)>,
    unknown_source_rx: local_bounded::Sender<(SocketAddr, Bytes)>,
    reset_in_progress: bool,
}

impl IoDriver {
    pub(super) fn new(
        command_receiver: local_bounded::Receiver<Command>,
        socket: UdpSocket,
        unknown_source_reporter: local_bounded::Sender<(SocketAddr, Bytes)>,
    ) -> Self {
        Self {
            commands: command_receiver,
            socket,
            connections: HashMap::new(),
            pending_tx: Vec::new(),
            unknown_source_rx: unknown_source_reporter,
            reset_in_progress: false,
        }
    }

    /// Run the I/O driver.
    pub async fn run(mut self) {
        log::info!("uTP I/O driver started");
        loop_select(&mut self, [Self::poll_commands, Self::poll_egress, Self::poll_ingress]).await;
        log::info!("uTP I/O driver exited");
    }

    fn poll_commands(&mut self, cx: &mut Context<'_>) -> Poll<ControlFlow<()>> {
        if self.reset_in_progress {
            if self.connections.is_empty() {
                self.reset_in_progress = false;
                log::info!("uTP reset complete");
            } else {
                // wait for all connections to close
                return Poll::Pending;
            }
        }

        if let Poll::Ready(result) = self.commands.poll_next_unpin(cx) {
            // handle command
            match result {
                Some(Command::AddConnection((peer_addr, connection))) => {
                    match self.connections.entry(peer_addr) {
                        Entry::Occupied(mut entry) => {
                            if entry.get().ingress.is_closed() || entry.get().egress.is_closed() {
                                // replace closed connection
                                entry.insert(connection);
                            } else {
                                log::error!("Not adding connection to {peer_addr}: already exists");
                            }
                        }
                        Entry::Vacant(e) => {
                            e.insert(connection);
                        }
                    }
                    Poll::Ready(ControlFlow::Continue(()))
                }
                Some(Command::ResetAll) => {
                    self.pending_tx.clear();
                    for connection in self.connections.values_mut() {
                        // fake an empty inbound packet which will fail parsing and trigger an outbound RST and connection exit
                        connection.ingress.queue().clear();
                        _ = connection.ingress.try_send(Bytes::new());
                        connection.egress.queue().clear();
                    }
                    self.reset_in_progress = true;
                    Poll::Pending // yield to allow connections to process the reset
                }
                None => {
                    // application exiting
                    Poll::Ready(ControlFlow::Break(()))
                }
            }
        } else {
            Poll::Pending
        }
    }

    fn poll_egress(&mut self, cx: &mut Context<'_>) -> Poll<ControlFlow<()>> {
        let mut made_progress = false;

        if let Some((remote_addr, packet)) = self.pending_tx.last() {
            // has pending egress, try sending it
            if let Poll::Ready(Ok(())) = self.socket.poll_send_ready(cx) {
                match self.socket.try_send_to(packet, *remote_addr) {
                    Ok(bytes_sent) if bytes_sent != packet.len() => {
                        log::error!(
                            "Incomplete send to {remote_addr}: {bytes_sent}/{}",
                            packet.len()
                        );
                        self.connections.remove(remote_addr);
                    }
                    Err(e) => {
                        log::error!("Send failed to {remote_addr}: {e}");
                        self.connections.remove(remote_addr);
                    }
                    Ok(_) => {}
                }
                self.pending_tx.pop();
                made_progress = true;
            }
        } else {
            // iterate through connections and fill the egress queue with at most 1 packet from each connection
            self.connections.retain(|remote_addr, connection| {
                match connection.egress.poll_next_unpin(cx) {
                    Poll::Ready(None) => {
                        made_progress = true;
                        false
                    }
                    Poll::Ready(Some(packet)) => {
                        made_progress = true;
                        self.pending_tx.push((*remote_addr, packet));
                        true
                    }
                    Poll::Pending => true,
                }
            });
        }

        if made_progress {
            Poll::Ready(ControlFlow::Continue(()))
        } else {
            Poll::Pending
        }
    }

    fn poll_ingress(&mut self, cx: &mut Context<'_>) -> Poll<ControlFlow<()>> {
        if let Poll::Ready(Ok(())) = self.socket.poll_recv_ready(cx) {
            // handle ingress
            let mut buf = BytesMut::new();
            if let Ok((_bytes_read, source_addr)) = self.socket.try_recv_buf_from(&mut buf) {
                match self.connections.entry(source_addr) {
                    Entry::Occupied(mut connection) => {
                        match connection.get_mut().ingress.try_send(buf.freeze()) {
                            Ok(_) => {}
                            Err(local_sync_error::TrySendError::Full(_buf)) => {
                                log::warn!("Dropping received packet from {source_addr}");
                            }
                            Err(local_sync_error::TrySendError::Closed(_buf)) => {
                                connection.remove();
                            }
                        }
                    }
                    Entry::Vacant(_) => {
                        match self.unknown_source_rx.try_send((source_addr, buf.freeze())) {
                            Ok(_) => {}
                            Err(local_sync_error::TrySendError::Full(_)) => {
                                log::warn!(
                                    "Dropping received packet from unknown source {source_addr}"
                                );
                            }
                            Err(local_sync_error::TrySendError::Closed(_)) => {
                                return Poll::Ready(ControlFlow::Break(()));
                            }
                        }
                    }
                }
                return Poll::Ready(ControlFlow::Continue(()));
            }
        }
        Poll::Pending
    }
}
