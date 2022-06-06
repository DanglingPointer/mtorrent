use std::io;

pub mod files;
pub mod meta;
pub mod pieces;

#[derive(Debug)]
pub enum Error {
    IOError(String),
    InvalidLocation,
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::IOError(format!("{}", e))
    }
}
