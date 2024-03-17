mod channels;
mod handshake;
mod message;
mod peer_states;
mod requests;

pub use channels::*;
pub use handshake::Handshake;
pub use message::{
    Bitfield, BlockInfo, DownloaderMessage, ExtendedMessage, Extension, HandshakeData,
    PeerExchangeData, UploaderMessage,
};
pub(crate) use peer_states::*;
pub(crate) use requests::PendingRequests;

pub(crate) const MAX_BLOCK_SIZE: usize = 16 * 1024;
