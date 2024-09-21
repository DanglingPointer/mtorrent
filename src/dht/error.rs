use super::msgs;
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

impl<T> From<mpsc::error::TrySendError<T>> for Error {
    fn from(e: mpsc::error::TrySendError<T>) -> Self {
        match e {
            mpsc::error::TrySendError::Full(_) => Error::ChannelFull,
            mpsc::error::TrySendError::Closed(_) => Error::ChannelClosed,
        }
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
