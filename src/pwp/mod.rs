mod channels;
mod handshake;
mod message;
mod states;

#[cfg(test)]
pub(crate) mod testutils;

pub use channels::*;
pub use handshake::{reserved_bits, Handshake};
pub use message::{
    Bitfield, BlockInfo, DownloaderMessage, ExtendedHandshake, ExtendedMessage, Extension,
    PeerExchangeData, UploaderMessage,
};
pub(crate) use states::*;

pub(crate) const MAX_BLOCK_SIZE: usize = 16 * 1024;
