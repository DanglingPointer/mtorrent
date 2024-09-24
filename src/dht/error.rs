use super::msgs;
use crate::utils::benc;
use std::io;
use tokio::sync::{mpsc, oneshot};

#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub enum Error {
    ParseError(&'static str),
    ErrorResponse(msgs::ErrorMsg),
    UnexpectedResponseType,
    ChannelClosed,
    ChannelFull,
    Timeout,
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
        match e {
            Error::ParseError(msg) => io::Error::new(io::ErrorKind::InvalidData, msg),
            Error::ErrorResponse(response) => {
                io::Error::new(io::ErrorKind::Other, format!("{response:?}"))
            }
            Error::UnexpectedResponseType => {
                io::Error::new(io::ErrorKind::InvalidData, "unexpected response type")
            }
            Error::ChannelClosed => io::Error::new(io::ErrorKind::BrokenPipe, "channel closed"),
            Error::ChannelFull => io::Error::new(io::ErrorKind::WouldBlock, "channel full"),
            Error::Timeout => io::Error::from(io::ErrorKind::TimedOut),
        }
    }
}

impl From<benc::ParseError> for Error {
    fn from(_e: benc::ParseError) -> Self {
        Error::ParseError("malformed bencode")
    }
}
