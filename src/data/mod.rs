use std::io;

mod block_accountant;
mod piece_info;
mod piece_tracker;
mod storage;

pub use block_accountant::BlockAccountant;
pub use piece_info::PieceInfo;
pub use piece_tracker::PieceTracker;
pub use storage::*;

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
            Error::InvalidLocation => io::Error::from(io::ErrorKind::NotFound),
        }
    }
}
