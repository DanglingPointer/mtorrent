use std::io;

pub mod files;
pub mod meta;
pub mod pieces;

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
