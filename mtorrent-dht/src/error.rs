use super::msgs;
use local_async_utils::prelude::*;
use mtorrent_utils::benc;
use std::io;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

#[expect(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("parsing failed ({0})")]
    ParseError(&'static str),
    #[error("malformed bencode ({0})")]
    BencodeError(#[from] benc::ParseError),
    #[error("error response ({0:?})")]
    ErrorResponse(msgs::ErrorMsg),
    #[error("channel closed")]
    ChannelClosed,
    #[error("timeout")]
    Timeout,
    #[error("no nodes")]
    NoNodes,
}

impl From<msgs::ErrorMsg> for Error {
    fn from(error: msgs::ErrorMsg) -> Self {
        Error::ErrorResponse(error)
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        Error::ChannelClosed
    }
}

impl From<oneshot::error::RecvError> for Error {
    fn from(_: oneshot::error::RecvError) -> Self {
        Error::ChannelClosed
    }
}

impl<T> From<local_sync_error::SendError<T>> for Error {
    fn from(_: local_sync_error::SendError<T>) -> Self {
        Error::ChannelClosed
    }
}

impl From<Error> for io::Error {
    fn from(e: Error) -> Self {
        let kind = match e {
            Error::ErrorResponse(_) => io::ErrorKind::Other,
            Error::ChannelClosed => io::ErrorKind::BrokenPipe,
            Error::Timeout => io::ErrorKind::TimedOut,
            Error::NoNodes => io::ErrorKind::InvalidInput,
            Error::BencodeError(_) | Error::ParseError(_) => io::ErrorKind::InvalidData,
        };
        Self::new(kind, e)
    }
}
