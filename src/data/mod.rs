use std::{error, fmt, io};

mod block_accountant;
mod piece_info;
mod piece_tracker;
mod storage;

pub(crate) use block_accountant::BlockAccountant;
pub use piece_info::PieceInfo;
pub(crate) use piece_tracker::PieceTracker;
pub use storage::{new_async_storage, Storage, StorageClient, StorageServer};

#[cfg(test)]
pub(crate) use storage::new_mock_storage;

#[derive(Debug)]
pub enum Error {
    IOError(io::Error),
    InvalidLocation,
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::IOError(e)
    }
}

impl From<Error> for io::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::IOError(e) => e,
            Error::InvalidLocation => io::Error::new(io::ErrorKind::NotFound, "invalid location"),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::IOError(e) => write!(f, "{e}"),
            Error::InvalidLocation => write!(f, "Invalid Location"),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::IOError(e) => Some(e),
            Error::InvalidLocation => None,
        }
    }
}
