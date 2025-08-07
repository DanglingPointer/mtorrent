mod channels;
mod handshake;
mod message;
mod states;

#[cfg(feature = "mocks")]
pub mod testutils;

pub use channels::*;
pub use handshake::{Handshake, reserved_bits};
pub use message::{
    Bitfield, BlockInfo, DownloaderMessage, ExtendedHandshake, ExtendedMessage, Extension,
    PeerExchangeData, UploaderMessage,
};
pub use states::*;

pub const MAX_BLOCK_SIZE: usize = 16 * 1024;
