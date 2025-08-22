use super::msgs;
use mtorrent_utils::benc;
use std::io;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

#[expect(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("parsing failed ({0})")]
    ParseError(&'static str),
    #[error("malformed bencode ({0})")]
    BencodeError(#[from] benc::ParseError),
    #[error("error response ({0:?})")]
    ErrorResponse(msgs::ErrorMsg),
    #[error("unexpected response type")]
    UnexpectedResponseType,
    #[error("channel closed")]
    ChannelClosed,
    #[error("channel full")]
    ChannelFull,
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

impl From<Error> for io::Error {
    fn from(e: Error) -> Self {
        let kind = match e {
            Error::ErrorResponse(_) => io::ErrorKind::Other,
            Error::ChannelClosed => io::ErrorKind::BrokenPipe,
            Error::ChannelFull => io::ErrorKind::WouldBlock,
            Error::Timeout => io::ErrorKind::TimedOut,
            Error::NoNodes => io::ErrorKind::InvalidInput,
            Error::BencodeError(_) | Error::ParseError(_) | Error::UnexpectedResponseType => {
                io::ErrorKind::InvalidData
            }
        };
        Self::new(kind, e)
    }
}
