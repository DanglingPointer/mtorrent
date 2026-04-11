use local_async_utils::prelude::*;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpStream, tcp};

/// Helper trait for efficiently splitting a stream into read and write halves.
pub trait SplitStream: Unpin {
    /// Associated type for the read half of the stream. These can be a non-owning
    /// reference to avoid unnecessary heap allocations and reference counting.
    type Ingress<'i>: AsyncRead + Unpin
    where
        Self: 'i;

    /// Associated type for the write half of the stream. These can be a non-owning
    /// reference to avoid unnecessary heap allocations and reference counting.
    type Egress<'e>: AsyncWrite + Unpin
    where
        Self: 'e;

    /// Split the stream into non-owning read and write halves.
    fn split(&mut self) -> (Self::Ingress<'_>, Self::Egress<'_>);
}

impl SplitStream for TcpStream {
    type Ingress<'i> = tcp::ReadHalf<'i>;
    type Egress<'e> = tcp::WriteHalf<'e>;

    fn split(&mut self) -> (Self::Ingress<'_>, Self::Egress<'_>) {
        TcpStream::split(self)
    }
}

impl SplitStream for local_pipe::DuplexEnd {
    type Ingress<'i> = &'i mut local_pipe::ReadEnd;
    type Egress<'e> = &'e mut local_pipe::WriteEnd;

    fn split(&mut self) -> (Self::Ingress<'_>, Self::Egress<'_>) {
        self.split()
    }
}
