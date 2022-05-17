use std::io;

mod files;
mod pieces;

pub use files::FileKeeper;

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
