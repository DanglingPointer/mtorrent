use super::connection::Connection;
use super::protocol::{Header, TypeVer, dbg_header_extensions};
use super::udp;
use bytes::Bytes;
use futures_util::{FutureExt, Stream, StreamExt};
use local_async_utils::prelude::*;
use log::log_enabled;
use mtorrent_utils::split_stream::SplitStream;
use std::hash::RandomState;
use std::io;
use std::net::SocketAddr;
use std::ops::Deref;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::task;

/// Opaque data for an inbound connection attempt, returned by [`InboundListener`] and consumed by
/// [`EndpointHandle::add_inbound_connection`].
#[derive(Debug, Clone)]
pub struct InboundConnectData(Header);

/// Stream of incoming connection attempts (i.e. received SYN packets that don't belong to an
/// existing connection).
pub struct InboundListener(local_bounded::Receiver<(SocketAddr, Bytes)>);

impl InboundListener {
    pub(super) fn new(receiver: local_bounded::Receiver<(SocketAddr, Bytes)>) -> Self {
        Self(receiver)
    }
}

impl Stream for InboundListener {
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
                        "InboundListener got {:?} from {source}",
                        dbg_header_extensions(&header, &packet)
                    );
                }
                if let TypeVer::Syn = header.type_ver {
                    return Poll::Ready(Some((source, InboundConnectData(header))));
                }
            }
        }
    }
}

/// Handle for creating new uTP connections.
#[derive(Clone)]
pub struct EndpointHandle {
    cmds: mpsc::Sender<udp::Command>,
    hasher_state: Rc<RandomState>,
}

impl EndpointHandle {
    const PIPE_CAPACITY: usize = crate::pwp::MAX_BLOCK_SIZE;
    const INGRESS_QUEUE: usize = 64;

    pub(super) fn new(cmds: mpsc::Sender<udp::Command>) -> Self {
        Self {
            cmds,
            hasher_state: Rc::new(RandomState::new()),
        }
    }

    /// Establish a new outbound connection to `remote_addr`. Waits for the uTP handshake to
    /// complete and returns a [`DataStream`] for the new connection.
    /// # Error
    /// If [`IoDriver`](super::IoDriver) has been shut down or if uTP handshake failed or if the
    /// connection already exists.
    pub async fn add_outbound_connection(&self, remote_addr: SocketAddr) -> io::Result<DataStream> {
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

        let (notifier, receiver) = local_condvar::condvar();
        task::spawn_local(connection.run(receiver).inspect(move |result| match result {
            Ok(()) => {
                log::debug!("Outbound connection to {remote_addr} closed");
            }
            Err(e) => {
                log::error!("Outbound connection to {remote_addr} exited with error: {e}");
            }
        }));
        Ok(DataStream {
            pipe: left,
            _canceller: notifier,
        })
    }

    /// Accept a new inbound connection from `remote_addr`. Waits for the uTP handshake to
    /// complete and returns a [`DataStream`] for the new connection.
    /// # Error
    /// If [`IoDriver`](super::IoDriver) has been shut down or if uTP handshake failed or if the
    /// connection already exists.
    pub async fn add_inbound_connection(
        &self,
        remote_addr: SocketAddr,
        data: InboundConnectData,
    ) -> io::Result<DataStream> {
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

        let (notifier, receiver) = local_condvar::condvar();
        task::spawn_local(connection.run(receiver).inspect(move |result| match result {
            Ok(()) => {
                log::debug!("Inbound connection from {remote_addr} closed");
            }
            Err(e) => {
                log::error!("Inbound connection from {remote_addr} exited with error: {e}");
            }
        }));
        Ok(DataStream {
            pipe: left,
            _canceller: notifier,
        })
    }

    /// Close all connections by sending RESET packets.
    pub async fn reset_connections(&self) {
        let _ = self.cmds.send(udp::Command::ResetConnections).await;
    }
}

/// Readable and writable channel returned by [`EndpointHandle`] after a successful connection.
/// Dropping the stream will close the underlying uTP connection by sending a RESET packet.
#[derive(Debug)]
pub struct DataStream {
    pipe: local_pipe::DuplexEnd,
    _canceller: local_condvar::Sender,
}

impl AsyncRead for DataStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().pipe).poll_read(cx, buf)
    }
}

impl AsyncWrite for DataStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().pipe).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().pipe).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().pipe).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().pipe).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.pipe.is_write_vectored()
    }
}

impl SplitStream for DataStream {
    type Ingress<'i> = <local_pipe::DuplexEnd as SplitStream>::Ingress<'i>;

    type Egress<'e> = <local_pipe::DuplexEnd as SplitStream>::Egress<'e>;

    fn split(&mut self) -> (Self::Ingress<'_>, Self::Egress<'_>) {
        self.pipe.split()
    }
}
