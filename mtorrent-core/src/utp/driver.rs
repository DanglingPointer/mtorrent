use bytes::BufMut;
use futures_util::StreamExt;
use local_async_utils::prelude::*;
use mtorrent_utils::fifo_set::UnboundedFifoSet;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::{fmt, io};
use tokio::net::UdpSocket;
use tokio::task;

#[derive(Debug, Clone, Copy)]
pub enum Action {
    None,
    FlushEgress,
    RemoveConnection,
}

pub trait DataReceiver {
    fn try_recv_buf<B: BufMut>(self, buf: &mut B) -> io::Result<()>;
}

pub trait DataSender {
    fn try_send(self, buf: &[u8]) -> io::Result<()>;
}

pub trait PeerConnection {
    type InboundInitData;

    fn peer_addr(&self) -> SocketAddr;

    fn current_stage(&self) -> impl fmt::Display;

    /// # Returns
    /// [`Poll::Pending`] if no progress was made.
    fn poll_next_action(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Action>;

    /// Read from socket and process the received data. This method MUST invoke [`DataReceiver::try_recv_buf`] to empty the socket.
    /// Returned error means connections should be dropped immediately
    fn process_ingress<R: DataReceiver>(self: Pin<&mut Self>, receiver: R) -> io::Result<()>;

    /// Write pending tx data to the socket. Called if [`PeerConnection`] has earlier indicated readiness via [`Action::FlushEgress`].
    /// Returned error means connections should be dropped immediately.
    fn produce_egress<S: DataSender>(self: Pin<&mut Self>, sender: S) -> io::Result<()>;

    /// Read packet not belonging to an existing connection, and see if it's an inbound connect
    fn process_unknown_source<R: DataReceiver>(receiver: R) -> io::Result<Self::InboundInitData>;
}

impl DataReceiver for (&UdpSocket, SocketAddr) {
    fn try_recv_buf<B: BufMut>(self, buf: &mut B) -> io::Result<()> {
        let (socket, peer_addr) = self;
        let (_bytes_read, remote_addr) = socket.try_recv_buf_from(buf)?;
        if remote_addr != peer_addr {
            Err(io::Error::other(format!(
                "unexpected peer addr (expected {peer_addr}, got {remote_addr})"
            )))
        } else {
            Ok(())
        }
    }
}

impl DataSender for (&UdpSocket, SocketAddr) {
    fn try_send(self, buf: &[u8]) -> io::Result<()> {
        let (socket, peer_addr) = self;
        let bytes_written = socket.try_send_to(buf, peer_addr)?;
        if bytes_written != buf.len() {
            Err(io::Error::other(format!("incomplete write ({bytes_written}/{} bytes)", buf.len())))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
pub enum Command<C: PeerConnection> {
    AddConnection(Pin<Box<C>>),
}

#[derive(Debug, Clone)]
pub struct InboundConnect<C: PeerConnection> {
    pub source_addr: SocketAddr,
    pub data: C::InboundInitData,
}

pub struct IoDriver<C: PeerConnection> {
    commands: local_bounded::Receiver<Command<C>>,
    reporter: local_bounded::Sender<InboundConnect<C>>,
    socket: UdpSocket,
    connections: HashMap<SocketAddr, Pin<Box<C>>>,
    egress_queue: UnboundedFifoSet<SocketAddr>,
}

impl<C: PeerConnection> IoDriver<C> {
    pub fn new(
        commands: local_bounded::Receiver<Command<C>>,
        reporter: local_bounded::Sender<InboundConnect<C>>,
        socket: UdpSocket,
    ) -> Self {
        Self {
            commands,
            reporter,
            socket,
            connections: HashMap::with_capacity(128),
            egress_queue: UnboundedFifoSet::with_capacity(1024),
        }
    }
}

impl<C: PeerConnection> IoDriver<C> {
    fn handle_command(&mut self, cmd: Command<C>) {
        match cmd {
            Command::AddConnection(connection) => {
                let peer_addr = connection.peer_addr();
                match self.connections.entry(peer_addr) {
                    Entry::Occupied(_) => {
                        log::error!("Not adding connection to {peer_addr}: already exists");
                    }
                    Entry::Vacant(e) => {
                        e.insert(connection);
                    }
                }
            }
        }
    }

    fn handle_ingress(&mut self, source_addr: SocketAddr) {
        let receiver = (&self.socket, source_addr);
        match self.connections.entry(source_addr) {
            Entry::Occupied(mut connection) => {
                if let Err(e) = connection.get_mut().as_mut().process_ingress(receiver) {
                    log::error!(
                        "Removing connection {source_addr}-{stage}: ingress failed ({e})",
                        stage = connection.get().current_stage()
                    );
                    connection.remove();
                }
            }
            Entry::Vacant(_) => {
                if let Ok(data) = C::process_unknown_source(receiver) {
                    _ = self.reporter.try_send(InboundConnect { source_addr, data }).inspect_err(
                        |e| log::error!("Failed to report inbound connect from {source_addr}: {e}"),
                    );
                }
            }
        }
    }

    fn handle_egress(&mut self, dest_addr: SocketAddr) {
        if let Entry::Occupied(mut connection) = self.connections.entry(dest_addr) {
            let sender = (&self.socket, dest_addr);
            if let Err(e) = connection.get_mut().as_mut().produce_egress(sender) {
                log::error!(
                    "Removing connection {dest_addr}-{stage}: egress failed ({e})",
                    stage = connection.get().current_stage()
                );
                connection.remove();
            }
        }
    }
}

impl<C: PeerConnection> Future for IoDriver<C> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            let coop = ready!(task::coop::poll_proceed(cx));
            let mut made_progress = false;

            // receive commands
            if let Poll::Ready(result) = this.commands.poll_next_unpin(cx) {
                match result {
                    Some(cmd) => this.handle_command(cmd),
                    None => return Poll::Ready(Ok(())),
                }
                made_progress = true;
            }

            // read data from socket
            if let Poll::Ready(result) = this.socket.poll_peek_sender(cx) {
                let source_addr = result?;
                this.handle_ingress(source_addr);
                made_progress = true;
            }

            // poll connections
            this.connections.retain(|peer_addr, connection| {
                match connection.as_mut().poll_next_action(cx) {
                    Poll::Ready(action) => match action {
                        Action::None => {
                            made_progress = true;
                            true
                        }
                        Action::FlushEgress => {
                            made_progress |= this.egress_queue.insert(*peer_addr);
                            true
                        }
                        Action::RemoveConnection => {
                            made_progress = true;
                            false
                        }
                    },
                    Poll::Pending => true,
                }
            });

            // write data to socket
            if !this.egress_queue.is_empty()
                && let Poll::Ready(result) = this.socket.poll_send_ready(cx)
            {
                result?;
                let dest_addr = this.egress_queue.remove_first().unwrap_or_else(|| unreachable!());
                this.handle_egress(dest_addr);
                made_progress = true;
            }

            if !made_progress {
                return Poll::Pending;
            }
            coop.made_progress();
        }
    }
}
