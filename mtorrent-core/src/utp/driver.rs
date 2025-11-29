use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use local_async_utils::prelude::*;
use log::log_enabled;
use mtorrent_utils::info_stopwatch;
use mtorrent_utils::loop_select::loop_select;
use std::collections::hash_map::{Entry, HashMap};
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::task::{Context, Poll, ready};
use tokio::io::ReadBuf;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

pub(super) enum Command {
    AddConnection((SocketAddr, ConnectionHandle)),
    ResetConnections,
}

pub(super) struct ConnectionHandle {
    pub(super) egress: local_bounded::Receiver<Bytes>,
    pub(super) ingress: local_bounded::Sender<Bytes>,
}

const MAX_UDP_PACKET_SIZE: usize = 65536;

/// [`IoDriver`] is responsible for sending and receiving UDP packets
/// and dispatching them to the appropriate connections.
pub struct IoDriver {
    commands: mpsc::Receiver<Command>,
    socket: UdpSocket,
    connections: HashMap<SocketAddr, ConnectionHandle>,
    pending_tx: Vec<(SocketAddr, Bytes)>,
    rx_buffer: Box<[MaybeUninit<u8>]>,
    new_source_reporter: local_bounded::Sender<(SocketAddr, Bytes)>,
    reset_in_progress: bool,
}

impl IoDriver {
    pub(super) fn new(
        command_receiver: mpsc::Receiver<Command>,
        socket: UdpSocket,
        new_source_reporter: local_bounded::Sender<(SocketAddr, Bytes)>,
    ) -> Self {
        Self {
            commands: command_receiver,
            socket,
            connections: HashMap::new(),
            pending_tx: Vec::new(),
            rx_buffer: Box::new_uninit_slice(MAX_UDP_PACKET_SIZE),
            new_source_reporter,
            reset_in_progress: false,
        }
    }

    /// Run the I/O driver.
    pub async fn run(mut self) {
        log::info!("uTP I/O driver started");
        let _sw = info_stopwatch!("uTP I/O driver");
        loop_select(&mut self, [Self::poll_commands, Self::poll_egress, Self::poll_ingress]).await;
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

        macro_rules! trigger_reset {
            ($reason:expr) => {{
                if !self.reset_in_progress {
                    log::info!("uTP I/O driver {} initiated", $reason);
                    self.pending_tx.clear();
                    for connection in self.connections.values_mut() {
                        // fake an empty inbound packet which will fail parsing and trigger an outbound RST and connection exit
                        connection.ingress.queue().clear();
                        _ = connection.ingress.try_send(Bytes::new());
                    }
                    self.reset_in_progress = true;
                }
            }};
        }

        match ready!(self.commands.poll_recv(cx)) {
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
            Some(Command::ResetConnections) => {
                if self.connections.is_empty() {
                    Poll::Ready(ControlFlow::Continue(()))
                } else {
                    trigger_reset!("reset");
                    Poll::Pending // yield to allow connections to process the reset
                }
            }
            None => {
                // application exiting
                if self.connections.is_empty() {
                    Poll::Ready(ControlFlow::Break(()))
                } else {
                    trigger_reset!("shutdown");
                    Poll::Pending // yield to allow connections to process the reset
                }
            }
        }
    }

    fn poll_egress(&mut self, cx: &mut Context<'_>) -> Poll<ControlFlow<()>> {
        let mut made_progress = false;

        if let Some((remote_addr, packet)) = self.pending_tx.last() {
            // has pending egress, try sending it
            if let Poll::Ready(ret) = self.socket.poll_send_to(cx, packet, *remote_addr) {
                match ret {
                    Ok(bytes_sent) if bytes_sent != packet.len() => {
                        log::error!(
                            "Incomplete send to {remote_addr}: {bytes_sent}/{}",
                            packet.len()
                        );
                        self.connections.remove(remote_addr);
                    }
                    Err(e) => {
                        // `e` is guaranteed to never be WouldBlock here
                        log::error!("Send failed to {remote_addr}: {e}");
                        self.connections.remove(remote_addr);
                    }
                    Ok(_) => {
                        if log_enabled!(log::Level::Trace) {
                            log::trace!("TX-{remote_addr}: {}", String::from_utf8_lossy(packet));
                        }
                    }
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
        let mut buf = ReadBuf::uninit(&mut self.rx_buffer);

        let (source_addr, packet) = match ready!(self.socket.poll_recv_from(cx, &mut buf)) {
            Ok(addr) => {
                let packet = BytesMut::from(buf.filled());
                (addr, packet.freeze())
            }
            Err(e) => {
                log::error!("Receive failed: {e}");
                return Poll::Ready(ControlFlow::Break(()));
            }
        };
        if log_enabled!(log::Level::Trace) {
            log::trace!("RX-{source_addr}: {}", String::from_utf8_lossy(&packet));
        }

        match self.connections.entry(source_addr) {
            Entry::Occupied(mut connection) => {
                match connection.get_mut().ingress.try_send(packet) {
                    Ok(_) => {}
                    Err(local_sync_error::TrySendError::Full(_buf)) => {
                        log::warn!("Dropping received packet from {source_addr}");
                    }
                    Err(local_sync_error::TrySendError::Closed(_buf)) => {
                        connection.remove();
                        // TODO: should we report this as unknown source?
                    }
                }
            }
            Entry::Vacant(_) => match self.new_source_reporter.try_send((source_addr, packet)) {
                Ok(_) => {}
                Err(e) => {
                    // don't exit even if the channel is closed
                    log::warn!("Dropping received packet from new source {source_addr}: {e}");
                }
            },
        }
        Poll::Ready(ControlFlow::Continue(()))
    }
}
