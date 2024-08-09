mod channels;
mod handshake;
mod message;
mod peer_states;
mod requests;

#[cfg(test)]
pub(crate) mod testutils;

pub use channels::*;
pub use handshake::{reserved_bits, Handshake};
pub use message::{
    Bitfield, BlockInfo, DownloaderMessage, ExtendedHandshake, ExtendedMessage, Extension,
    PeerExchangeData, UploaderMessage,
};
pub(crate) use peer_states::*;
pub(crate) use requests::PendingRequests;

pub(crate) const MAX_BLOCK_SIZE: usize = 16 * 1024;
