mod cipher;
mod handshake;
mod io;
mod key_exchange;
mod utils;

pub use cipher::{Crypto, Decryptor, Encryptor};
pub use handshake::{inbound_handshake, outbound_handshake};
pub use io::{DecryptingBufReader, DecryptingReader, EncryptingWriter, PrefixedStream};
pub use utils::{MaybeEncrypted, is_stream_unencrypted};

#[cfg(test)]
pub(crate) use cipher::crypto_pair;
