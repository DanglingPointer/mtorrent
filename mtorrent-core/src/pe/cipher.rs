use super::utils::sha1_of;
use derive_more::Debug;
use rc4::consts::U20;
use rc4::{KeyInit, Rc4, StreamCipher};

/// RC4 stream cipher used for encryption in PE.
#[derive(Debug)]
#[debug("Encryptor")]
pub struct Encryptor(Rc4<U20>);

impl Encryptor {
    /// Encrypts the given buffer in-place.
    pub fn encrypt(&mut self, buf: &mut [u8]) {
        self.0.apply_keystream(buf)
    }
}

/// RC4 stream cipher used for decryption in PE.
#[derive(Debug)]
#[debug("Decryptor")]
pub struct Decryptor(Rc4<U20>);

impl Decryptor {
    /// Decrypts the given buffer in-place.
    pub fn decrypt(&mut self, buf: &mut [u8]) {
        self.0.apply_keystream(buf)
    }
}

/// A pair of RC4 encryptor and decryptor for a peer connection.
#[derive(Debug)]
pub struct Crypto {
    pub encryptor: Encryptor,
    pub decryptor: Decryptor,
}

/// Derives the RC4 encryption and decryption keys from the shared secret and info hash for an
/// outbound connection.
pub(super) fn crypto_for_outbound_connection(
    secret: &crypto_bigint::U768,
    info_hash: &[u8; 20],
) -> (Encryptor, Decryptor) {
    let encryption_key = sha1_of![b"keyA", &secret.to_be_bytes(), info_hash];
    let decryption_key = sha1_of![b"keyB", &secret.to_be_bytes(), info_hash];
    let mut encryptor = Encryptor(Rc4::new(&encryption_key.into()));
    let mut decryptor = Decryptor(Rc4::new(&decryption_key.into()));

    let mut buf = [0u8; 1024];
    encryptor.encrypt(&mut buf);
    decryptor.decrypt(&mut buf);
    (encryptor, decryptor)
}

/// Derives the RC4 encryption and decryption keys from the shared secret and info hash for an
/// inbound connection.
pub(super) fn crypto_for_inbound_connection(
    secret: &crypto_bigint::U768,
    info_hash: &[u8; 20],
) -> (Encryptor, Decryptor) {
    let encryption_key = sha1_of![b"keyB", &secret.to_be_bytes(), info_hash];
    let decryption_key = sha1_of![b"keyA", &secret.to_be_bytes(), info_hash];
    let mut encryptor = Encryptor(Rc4::new(&encryption_key.into()));
    let mut decryptor = Decryptor(Rc4::new(&decryption_key.into()));

    let mut buf = [0u8; 1024];
    encryptor.encrypt(&mut buf);
    decryptor.decrypt(&mut buf);
    (encryptor, decryptor)
}

#[cfg(test)]
pub(crate) fn crypto_pair(info_hash: &[u8; 20]) -> (Encryptor, Decryptor) {
    let secret: [u8; 96] = rand::random();
    let key = sha1_of![b"keyA", &secret, info_hash];
    (Encryptor(Rc4::new(&key.into())), Decryptor(Rc4::new(&key.into())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crypto_bigint::{Random, U768};
    use rand::rng;

    #[test]
    fn test_inbound_outbound_compatibility() {
        let info_hash: [u8; 20] = rand::random();
        let secret = U768::random_from_rng(&mut rng());
        let (mut out_enc, mut out_dec) = crypto_for_outbound_connection(&secret, &info_hash);
        let (mut in_enc, mut in_dec) = crypto_for_inbound_connection(&secret, &info_hash);

        let data: [u8; 100] = rand::random();

        let mut encrypted = data;
        out_enc.encrypt(&mut encrypted);
        in_dec.decrypt(&mut encrypted);
        assert_eq!(data, encrypted);

        let mut encrypted2 = data;
        in_enc.encrypt(&mut encrypted2);
        out_dec.decrypt(&mut encrypted2);
        assert_eq!(data, encrypted2);
    }

    #[test]
    fn test_decrypt_in_chunks() {
        let (mut enc, mut dec) = crypto_pair(&rand::random());
        let plaintext: [u8; 100] = rand::random();

        for chunk_size in 1..=100 {
            let encrypted = {
                let mut buf = plaintext;
                enc.encrypt(&mut buf);
                buf
            };

            let mut chunked_data = encrypted;
            for chunk in chunked_data.chunks_mut(chunk_size) {
                dec.decrypt(chunk);
            }

            assert_eq!(plaintext, chunked_data, "decryption failed for chunk size {chunk_size}");
        }
    }

    #[test]
    fn test_encrypt_in_chunks() {
        let (mut enc, mut dec) = crypto_pair(&rand::random());
        let plaintext: [u8; 100] = rand::random();

        for chunk_size in 1..=100 {
            let mut chunked_data = plaintext;
            for chunk in chunked_data.chunks_mut(chunk_size) {
                enc.encrypt(chunk);
            }

            dec.decrypt(&mut chunked_data);

            assert_eq!(
                plaintext, chunked_data,
                "encryption/decryption failed for chunk size {chunk_size}"
            );
        }
    }
}
