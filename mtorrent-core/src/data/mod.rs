use std::io;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

mod block_accountant;
mod piece_info;
mod piece_requests;
mod piece_tracker;
mod storage;

pub use block_accountant::BlockAccountant;
pub use piece_info::PieceInfo;
pub use piece_requests::PendingRequests;
pub use piece_tracker::PieceTracker;
pub use storage::{StorageClient, StorageServer, new_async_storage};

#[cfg(feature = "mocks")]
pub use storage::new_mock_storage;

/// Common error type.
#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    IOError(#[from] io::Error),
    #[error("invalid Location")]
    InvalidLocation,
    #[error("channel closed")]
    ChannelClosed,
}

impl From<Error> for io::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::IOError(e) => e,
            Error::InvalidLocation => io::Error::new(io::ErrorKind::NotFound, "invalid location"),
            Error::ChannelClosed => io::Error::new(io::ErrorKind::BrokenPipe, "channel closed"),
        }
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(_: mpsc::error::SendError<T>) -> Self {
        Self::ChannelClosed
    }
}

impl From<oneshot::error::RecvError> for Error {
    fn from(_: oneshot::error::RecvError) -> Self {
        Self::ChannelClosed
    }
}
