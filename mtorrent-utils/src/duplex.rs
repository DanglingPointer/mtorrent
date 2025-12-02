use local_async_utils::prelude::*;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, ReadHalf, SimplexStream, WriteHalf, simplex};

/// Utility to avoid double nexted Arc<Mutex<..>> when calling [`tokio::io::split()`] on [`tokio::io::DuplexStream`].
pub struct DuplexPipe(pub ReadHalf<SimplexStream>, pub WriteHalf<SimplexStream>);

impl AsyncRead for DuplexPipe {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let DuplexPipe(read, _write) = self.get_mut();
        Pin::new(read).poll_read(cx, buf)
    }
}

impl AsyncWrite for DuplexPipe {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let DuplexPipe(_read, write) = self.get_mut();
        Pin::new(write).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let DuplexPipe(_read, write) = self.get_mut();
        Pin::new(write).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let DuplexPipe(_read, write) = self.get_mut();
        Pin::new(write).poll_shutdown(cx)
    }
}

/// Create a pair of connected [`DuplexPipe`]s with the given capacity.
/// Similar to [`tokio::io::duplex()`], but more efficient.
pub fn duplex_pipe(capacity: usize) -> (DuplexPipe, DuplexPipe) {
    let (read1, write1) = simplex(capacity);
    let (read2, write2) = simplex(capacity);
    (DuplexPipe(read1, write2), DuplexPipe(read2, write1))
}

/// Two-way in-memory pipe using `local_pipe`.
pub struct LocalDuplexPipe(pub local_pipe::Reader, pub local_pipe::Writer);

impl AsyncRead for LocalDuplexPipe {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let LocalDuplexPipe(read, _write) = self.get_mut();
        Pin::new(read).poll_read(cx, buf)
    }
}

impl AsyncWrite for LocalDuplexPipe {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let LocalDuplexPipe(_read, write) = self.get_mut();
        Pin::new(write).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let LocalDuplexPipe(_read, write) = self.get_mut();
        Pin::new(write).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let LocalDuplexPipe(_read, write) = self.get_mut();
        Pin::new(write).poll_shutdown(cx)
    }
}

/// Create a pair of connected [`LocalDuplexPipe`]s with the given capacity.
pub fn local_duplex_pipe(capacity: usize) -> (LocalDuplexPipe, LocalDuplexPipe) {
    let (read1, write1) = local_pipe::pipe(capacity);
    let (read2, write2) = local_pipe::pipe(capacity);
    (LocalDuplexPipe(read1, write2), LocalDuplexPipe(read2, write1))
}
