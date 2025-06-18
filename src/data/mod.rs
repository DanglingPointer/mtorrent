use std::io;
use thiserror::Error;

mod block_accountant;
mod piece_info;
mod piece_requests;
mod piece_tracker;
mod storage;

pub(crate) use block_accountant::BlockAccountant;
pub use piece_info::PieceInfo;
pub(crate) use piece_requests::PendingRequests;
pub(crate) use piece_tracker::PieceTracker;
pub use storage::{new_async_storage, Storage, StorageClient, StorageServer};

#[cfg(test)]
pub(crate) use storage::new_mock_storage;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    IOError(#[from] io::Error),
    #[error("invalid Location")]
    InvalidLocation,
}

impl From<Error> for io::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::IOError(e) => e,
            Error::InvalidLocation => io::Error::new(io::ErrorKind::NotFound, "invalid location"),
        }
    }
}
