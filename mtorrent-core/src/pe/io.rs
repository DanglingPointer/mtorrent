use super::{Decryptor, Encryptor};
use bytes::{Buf, BufMut, BytesMut};
use mtorrent_utils::split_stream::SplitStream;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::{cmp, io, mem};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWrite, Chain, ReadBuf};

pin_project! {
    /// A wrapper around an [`AsyncRead`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html) that decrypts the data read from it using a [`Decryptor`].
    #[derive(Debug)]
    pub struct DecryptingReader<R: AsyncRead> {
        #[pin]
        inner: R,
        crypto: Decryptor,
    }
}

impl<R: AsyncRead> DecryptingReader<R> {
    /// Creates a new `DecryptingReader` that wraps the given [`AsyncRead`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html) and [`Decryptor`].
    pub fn new(inner: R, crypto: Decryptor) -> Self {
        Self { inner, crypto }
    }

    /// Consumes the `DecryptingReader`, returning the wrapped [`AsyncRead`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html) and [`Decryptor`].
    pub fn into_parts(self) -> (R, Decryptor) {
        (self.inner, self.crypto)
    }
}

impl<R: AsyncRead> AsyncRead for DecryptingReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.project();

        let old_len = buf.filled().len();
        ready!(this.inner.poll_read(cx, buf))?;
        let new_len = buf.filled().len();

        if new_len > old_len {
            this.crypto.decrypt(&mut buf.filled_mut()[old_len..new_len]);
        }
        Poll::Ready(Ok(()))
    }
}

const BUFFER_SIZE: usize = 33 * 1024;

pin_project! {
    /// A buffering reader that wraps an [`AsyncRead`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html) and decrypts the data read from it using a [`Decryptor`].
    ///
    /// Implementation of `DecryptingBufReader` is similar to [`tokio::io::BufReader`](https://docs.rs/tokio/latest/tokio/io/struct.BufReader.html), slightly
    /// simplified thanks to [`BytesMut`](https://docs.rs/bytes/latest/bytes/struct.BytesMut.html), and with added calls to `Decryptor::decrypt()`.
    #[derive(Debug)]
    pub struct DecryptingBufReader<R: AsyncRead> {
        #[pin]
        inner: R,
        crypto: Decryptor,
        buffer: BytesMut,
    }
}

impl<R: AsyncRead> DecryptingBufReader<R> {
    /// Creates a new `DecryptingBufReader` that wraps the given [`AsyncRead`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html) and [`Decryptor`].
    pub fn new(inner: R, crypto: Decryptor) -> Self {
        Self {
            inner,
            crypto,
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
        }
    }

    /// Consumes the `DecryptingBufReader`, returning the wrapped [`AsyncRead`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html) and [`Decryptor`].
    pub fn into_parts(self) -> (R, Decryptor) {
        (self.inner, self.crypto)
    }
}

impl<R: AsyncRead> AsyncRead for DecryptingBufReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let data = ready!(self.as_mut().poll_fill_buf(cx))?;
        let bytes_to_copy = cmp::min(data.len(), buf.remaining());
        buf.put_slice(&data[..bytes_to_copy]);
        self.consume(bytes_to_copy);
        Poll::Ready(Ok(()))
    }
}

impl<R: AsyncRead> AsyncBufRead for DecryptingBufReader<R> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.project();

        if this.buffer.is_empty() {
            assert!(this.buffer.try_reclaim(BUFFER_SIZE));
            let mut rd = ReadBuf::uninit(this.buffer.spare_capacity_mut());
            ready!(this.inner.poll_read(cx, &mut rd))?;

            this.crypto.decrypt(rd.filled_mut());

            let bytes_read = rd.filled().len();
            unsafe { this.buffer.advance_mut(bytes_read) }
        }
        Poll::Ready(Ok(this.buffer))
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.project();
        let amt = cmp::min(amt, this.buffer.remaining());
        this.buffer.advance(amt);
    }
}

pin_project! {
    /// A buffering writer that wraps an [`AsyncWrite`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncWrite.html) and encrypts the data written to it using an [`Encryptor`].
    ///
    /// Implementation of `EncryptingWriter` is similar to [`tokio::io::BufWriter`](https://docs.rs/tokio/latest/tokio/io/struct.BufWriter.html), slightly
    /// simplified thanks to [`BytesMut`](https://docs.rs/bytes/latest/bytes/struct.BytesMut.html), and with added calls to `Encryptor::encrypt()`.
    #[derive(Debug)]
    pub struct EncryptingWriter<W: AsyncWrite> {
        #[pin]
        inner: W,
        crypto: Encryptor,
        buffer: BytesMut,
    }
}

impl<W: AsyncWrite> EncryptingWriter<W> {
    /// Creates a new `EncryptingWriter` that wraps the given [`AsyncWrite`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncWrite.html) and [`Encryptor`].
    pub fn new(inner: W, crypto: Encryptor) -> Self {
        Self {
            inner,
            crypto,
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
        }
    }

    /// Consumes the `EncryptingWriter`, returning the wrapped [`AsyncWrite`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncWrite.html) and [`Encryptor`].
    pub fn into_parts(self) -> (W, Encryptor) {
        (self.inner, self.crypto)
    }

    fn flush_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();

        while !this.buffer.is_empty() {
            let written = ready!(this.inner.as_mut().poll_write(cx, this.buffer))?;
            if written == 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write the buffered data",
                )));
            }
            this.buffer.advance(written);
        }
        Poll::Ready(Ok(()))
    }

    fn fill_buf(self: Pin<&mut Self>, data: &[u8]) -> io::Result<usize> {
        let this = self.project();
        assert!(this.buffer.capacity() <= BUFFER_SIZE);

        let filled_len = this.buffer.len();
        let available_cap = {
            let curr_available = this.buffer.capacity() - filled_len;
            if data.len() > curr_available {
                let max_available = BUFFER_SIZE - filled_len;
                assert!(this.buffer.try_reclaim(max_available));
                max_available
            } else {
                curr_available
            }
        };

        let bytes_to_write = cmp::min(data.len(), available_cap);

        if bytes_to_write == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "can't write data to buffer"));
        }

        this.buffer.extend_from_slice(&data[..bytes_to_write]);
        this.crypto.encrypt(&mut this.buffer[filled_len..]);

        Ok(bytes_to_write)
    }
}

impl<W: AsyncWrite> AsyncWrite for EncryptingWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        ready!(self.as_mut().flush_buf(cx))?;
        Poll::Ready(self.fill_buf(buf))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.as_mut().flush_buf(cx))?;
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.as_mut().flush_buf(cx))?;
        self.project().inner.poll_shutdown(cx)
    }
}

pin_project! {
    /// A wrapper around an [`AsyncRead`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html) that first returns the bytes from an initial buffer, and then
    /// delegates to the wrapped stream.
    pub struct PrefixedStream<T: Buf, S> {
        prefix: T,
        #[pin]
        stream: S,
    }
}

impl<T: Buf, S> PrefixedStream<T, S> {
    /// Creates a new `PrefixedStream` that wraps the given initial buffer and stream.
    pub fn new(prefix: T, stream: S) -> Self {
        Self { prefix, stream }
    }

    /// Replaces the prefix buffer with a new one.
    pub fn replace_prefix(&mut self, new_prefix: T) -> T {
        mem::replace(&mut self.prefix, new_prefix)
    }
}

impl<T: Buf, S: AsyncRead> AsyncRead for PrefixedStream<T, S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.prefix.has_remaining() {
            let bytes_to_copy = cmp::min(buf.remaining(), self.prefix.chunk().len());
            buf.put_slice(&self.prefix.chunk()[..bytes_to_copy]);
            self.project().prefix.advance(bytes_to_copy);
            Poll::Ready(Ok(()))
        } else {
            self.project().stream.poll_read(cx, buf)
        }
    }
}

impl<T: Buf, S: AsyncWrite> AsyncWrite for PrefixedStream<T, S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.project().stream.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().stream.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().stream.poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.project().stream.poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }
}

impl<T: Buf, S: SplitStream> SplitStream for PrefixedStream<T, S> {
    type Ingress<'i>
        = Chain<io::Cursor<&'i [u8]>, <S as SplitStream>::Ingress<'i>>
    where
        Self: 'i;

    type Egress<'e>
        = S::Egress<'e>
    where
        Self: 'e;

    fn split(&mut self) -> (Self::Ingress<'_>, Self::Egress<'_>) {
        let (ingress, egress) = self.stream.split();
        let ingress = AsyncReadExt::chain(io::Cursor::new(self.prefix.chunk()), ingress);
        (ingress, egress)
    }
}

#[cfg(test)]
mod tests {
    use super::super::cipher::crypto_pair;
    use super::*;
    use local_async_utils::prelude::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::join;

    #[tokio::test]
    async fn test_pipe_big_data_over_encrypted_stream() {
        let (enc, dec) = crypto_pair(&rand::random());

        let (reader, writer) = local_pipe::Pipe::new(1024).into_split();
        let mut reader = DecryptingReader::new(reader, dec);
        let mut writer = EncryptingWriter::new(writer, enc);

        for _ in 0..3 {
            let src_data: [u8; BUFFER_SIZE * 2] = rand::random();
            let mut dest_data = [0u8; BUFFER_SIZE * 2];

            let write_fut = async {
                writer.write_all(&src_data).await.unwrap();
                writer.flush().await.unwrap();
            };
            let read_fut = async {
                reader.read_exact(&mut dest_data).await.unwrap();
            };
            join!(write_fut, read_fut);

            assert_eq!(src_data, dest_data);
        }
    }

    #[tokio::test]
    async fn test_pipe_small_data_over_encrypted_stream() {
        let (enc, dec) = crypto_pair(&rand::random());

        let (reader, writer) = local_pipe::Pipe::new(1024).into_split();
        let mut reader = DecryptingReader::new(reader, dec);
        let mut writer = EncryptingWriter::new(writer, enc);

        for _ in 0..3 {
            let src_data: [u8; BUFFER_SIZE / 2] = rand::random();
            let mut dest_data = [0u8; BUFFER_SIZE / 2];

            let write_fut = async {
                writer.write_all(&src_data).await.unwrap();
                writer.flush().await.unwrap();
            };
            let read_fut = async {
                reader.read_exact(&mut dest_data).await.unwrap();
            };
            join!(write_fut, read_fut);

            assert_eq!(src_data, dest_data);
        }
    }

    #[tokio::test]
    async fn test_pipe_big_data_over_encrypted_buffered_stream() {
        let (enc, dec) = crypto_pair(&rand::random());

        let (reader, writer) = local_pipe::Pipe::new(1024).into_split();
        let mut reader = DecryptingBufReader::new(reader, dec);
        let mut writer = EncryptingWriter::new(writer, enc);

        for _ in 0..3 {
            let src_data: [u8; BUFFER_SIZE * 2] = rand::random();
            let mut dest_data = [0u8; BUFFER_SIZE * 2];

            let write_fut = async {
                writer.write_all(&src_data).await.unwrap();
                writer.flush().await.unwrap();
            };
            let read_fut = async {
                reader.read_exact(&mut dest_data).await.unwrap();
            };
            join!(write_fut, read_fut);

            assert_eq!(src_data, dest_data);
        }
    }

    #[tokio::test]
    async fn test_pipe_small_data_over_encrypted_buffered_stream() {
        let (enc, dec) = crypto_pair(&rand::random());

        let (reader, writer) = local_pipe::Pipe::new(1024).into_split();
        let mut reader = DecryptingBufReader::new(reader, dec);
        let mut writer = EncryptingWriter::new(writer, enc);

        for _ in 0..3 {
            let src_data: [u8; BUFFER_SIZE / 2] = rand::random();
            let mut dest_data = [0u8; BUFFER_SIZE / 2];

            let write_fut = async {
                writer.write_all(&src_data).await.unwrap();
                writer.flush().await.unwrap();
            };
            let read_fut = async {
                reader.read_exact(&mut dest_data).await.unwrap();
            };
            join!(write_fut, read_fut);

            assert_eq!(src_data, dest_data);
        }
    }

    #[tokio::test]
    async fn test_prefixed_stream() {
        let prefix: [u8; 10] = rand::random();
        let data: [u8; 20] = rand::random();
        let mut stream = PrefixedStream::new(BytesMut::from(&prefix[..]), &data[..]);

        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();

        assert_eq!(&buf[..10], &prefix);
        assert_eq!(&buf[10..], &data);
    }
}
