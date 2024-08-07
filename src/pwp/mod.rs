mod channels;
mod handshake;
mod message;
mod peer_states;
mod requests;
mod tcp;

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
pub(crate) use tcp::{
    channels_for_incoming_connection, channels_for_outgoing_connection, run_listener,
};

pub(crate) const MAX_BLOCK_SIZE: usize = 16 * 1024;
