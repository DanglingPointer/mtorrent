use std::{
    collections::{HashMap, hash_map::Entry},
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll, ready},
};

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use local_async_utils::prelude::*;
use tokio::{net::UdpSocket, task};

pub enum Command {
    AddConnection((SocketAddr, ConnectionHandle)),
}

pub struct ConnectionHandle {
    pub egress: local_bounded::Receiver<Bytes>,
    pub ingress: local_bounded::Sender<Bytes>,
}

pub struct IoDriver {
    commands: local_bounded::Receiver<Command>,
    socket: UdpSocket,
    connections: HashMap<SocketAddr, ConnectionHandle>,
    pending_tx: Vec<(SocketAddr, Bytes)>,
    unknown_source_rx: local_bounded::Sender<(SocketAddr, Bytes)>,
}

impl IoDriver {
    pub fn new(
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
        }
    }

    fn poll_run(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            let coop = ready!(task::coop::poll_proceed(cx));
            let mut made_progress = false;

            if let Poll::Ready(result) = self.commands.poll_next_unpin(cx) {
                // handle command
                match result {
                    Some(Command::AddConnection((peer_addr, connection))) => {
                        match self.connections.entry(peer_addr) {
                            Entry::Occupied(_) => {
                                log::error!("Not adding connection to {peer_addr}: already exists");
                            }
                            Entry::Vacant(e) => {
                                e.insert(connection);
                            }
                        }
                        made_progress = true;
                    }
                    None => return Poll::Ready(()),
                }
            }

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
                                    return Poll::Ready(());
                                }
                            }
                        }
                    }
                    made_progress = true;
                }
            }

            if !made_progress {
                return Poll::Pending;
            }

            coop.made_progress();
        }
    }
}

impl Future for IoDriver {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().poll_run(cx)
    }
}
