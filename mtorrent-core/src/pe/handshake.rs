use super::cipher::{Crypto, crypto_for_inbound_connection, crypto_for_outbound_connection};
use super::key_exchange::DhKeyExchange;
use super::utils::{consume_encrypted, consume_through, sha1_of, xor_arrays};
use bytes::{Buf, BufMut};
use crypto_bigint::{Encoding, U768};
use mtorrent_utils::split_stream::SplitStream;
use std::mem::MaybeUninit;
use std::{cmp, io};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::try_join;

const MODE_PLAINTEXT: u32 = 1;
const MODE_RC4: u32 = 2;
const MODE_ANY: u32 = 3;

const MAX_PADDING_LEN: usize = 512;
const VC_LEN: usize = 8;

const fn max_pe3_len() -> usize {
    20 + 20 + VC_LEN + 4 /* crypto_provide */ + 2/* padding_len */+  MAX_PADDING_LEN + 2
}

const fn max_pe4_len() -> usize {
    VC_LEN + 4 /* crypto_select */ + 2 /* padding_len */ + MAX_PADDING_LEN
}

async fn read_remote_pubkey<S: AsyncReadExt + Unpin>(stream: &mut S) -> io::Result<U768> {
    let mut buf = [0u8; DhKeyExchange::KEY_SIZE];
    stream.read_exact(&mut buf).await?;
    Ok(U768::from_be_bytes(buf.into()))
}

async fn write_padding<S: AsyncWriteExt + Unpin>(stream: &mut S) -> io::Result<()> {
    let padding: [u8; MAX_PADDING_LEN] = rand::random();
    let len = rand::random::<u16>() as usize % MAX_PADDING_LEN;
    stream.write_all(&padding[..len]).await
}

/// Performs the outbound PE handshake over the given stream, returning the established encryptor
/// and decryptor if encryption was selected by the remote peer.
pub async fn outbound_handshake<S: AsyncRead + AsyncWrite + SplitStream>(
    stream: &mut S,
    info_hash: &[u8; 20],
    mut ia_data: impl Buf,
) -> io::Result<Option<Crypto>> {
    assert!(
        ia_data.remaining() <= u16::MAX as usize,
        "IA data too large: {} bytes",
        ia_data.remaining()
    );
    let dh = DhKeyExchange::default();

    // send pubkey
    stream.write_all(&dh.local_pubkey().to_be_bytes()).await?;

    // receive remote pubkey
    let remote_pubkey = read_remote_pubkey(stream).await?;

    // set up rc4
    let secret = dh.into_shared_secret(&remote_pubkey);
    let (mut encryptor, mut decryptor) = crypto_for_outbound_connection(&secret, info_hash);
    let secret = secret.to_be_bytes();

    let (mut ingress, mut egress) = stream.split();

    let write_pe3 = async {
        // write padding A
        write_padding(&mut egress).await?;

        // prepare pe3
        let mut buf: [u8; max_pe3_len()] = rand::random();
        let mut pe3_writer = &mut buf[..];

        let hash_req1_s = sha1_of![b"req1", &secret];
        let hash_req2_req3_xored =
            xor_arrays(sha1_of![b"req2", info_hash], sha1_of![b"req3", &secret]);
        let crypto_provide = MODE_ANY;
        let padding_c_len = rand::random::<u16>() as usize % MAX_PADDING_LEN;

        pe3_writer.put_slice(&hash_req1_s);
        pe3_writer.put_slice(&hash_req2_req3_xored);
        pe3_writer.put_bytes(0, VC_LEN);
        pe3_writer.put_u32(crypto_provide);
        pe3_writer.put_u16(padding_c_len as u16);
        pe3_writer = &mut pe3_writer[padding_c_len..];
        pe3_writer.put_u16(ia_data.remaining() as u16);

        // encrypt and send pe3
        let total_pe3_len = buf.len() - MAX_PADDING_LEN + padding_c_len;
        encryptor.encrypt(&mut buf[40..total_pe3_len]);
        egress.write_all(&buf[..total_pe3_len]).await?;

        // encrypt and send IA if any
        let mut buf = [MaybeUninit::<u8>::uninit(); 512];
        let mut rd = ReadBuf::uninit(&mut buf);
        while ia_data.has_remaining() {
            let bytes_to_write = cmp::min(ia_data.remaining(), rd.capacity());
            rd.put_slice(&ia_data.chunk()[..bytes_to_write]);
            ia_data.advance(bytes_to_write);

            encryptor.encrypt(rd.filled_mut());
            egress.write_all(rd.filled()).await?;
            rd.clear();
        }
        Ok(())
    };

    let mut crypto_select = 0;

    let read_pad_b_pe4_pad_d = async {
        // read and discard remote padding B
        let expected_remote_vc = {
            let mut vc = [0u8; VC_LEN];
            decryptor.decrypt(&mut vc); // encrypt with inbound key
            vc
        };
        consume_through(
            (&mut ingress).take((MAX_PADDING_LEN + VC_LEN) as u64),
            &expected_remote_vc,
        )
        .await?;

        // read and decrypt pe4 up until padding D
        let mut buf = [0u8; 6];
        ingress.read_exact(&mut buf).await?;
        decryptor.decrypt(&mut buf);
        let mut src = &buf[..];

        crypto_select = src.get_u32();
        if !matches!(crypto_select, MODE_RC4 | MODE_PLAINTEXT) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("unexpected crypto select ({crypto_select:#x})"),
            ));
        }
        let padding_d_len = src.get_u16() as usize;
        if padding_d_len > MAX_PADDING_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("padding len ({padding_d_len}) exceeds {MAX_PADDING_LEN}"),
            ));
        }

        // decrypt and discard padding D
        consume_encrypted::<MAX_PADDING_LEN>(ingress, padding_d_len, &mut decryptor, "padding D")
            .await?;
        Ok(())
    };

    try_join!(biased; write_pe3, read_pad_b_pe4_pad_d)?;

    match crypto_select {
        MODE_RC4 => Ok(Some(Crypto {
            encryptor,
            decryptor,
        })),
        MODE_PLAINTEXT => Ok(None),
        _ => unreachable!(),
    }
}

/// Performs the inbound PE handshake over the given stream, returning the established encryptor and
/// decryptor if encryption is supported by the remote peer.
pub async fn inbound_handshake<S: AsyncRead + AsyncWrite + SplitStream>(
    stream: &mut S,
    info_hash: &[u8; 20],
    mut ia_buffer: impl BufMut,
) -> io::Result<Option<Crypto>> {
    let dh = DhKeyExchange::default();

    // receive remote pubkey
    let remote_pubkey = read_remote_pubkey(stream).await?;

    // send pubkey + padding B
    stream.write_all(&dh.local_pubkey().to_be_bytes()).await?;
    write_padding(stream).await?;

    // set up rc4
    let secret = dh.into_shared_secret(&remote_pubkey);
    let (mut encryptor, mut decryptor) = crypto_for_inbound_connection(&secret, info_hash);
    let secret = secret.to_be_bytes();

    // read and discard remote padding A
    let expected_hash_req1_s = sha1_of![b"req1", &secret];
    consume_through(
        stream.take((MAX_PADDING_LEN + expected_hash_req1_s.len()) as u64),
        &expected_hash_req1_s,
    )
    .await?;

    // read and validate xored hash of req2 and req3
    let expected_hash_req2_req3_xored =
        xor_arrays(sha1_of![b"req2", info_hash], sha1_of![b"req3", &secret]);
    let mut buf = [0u8; 20];
    stream.read_exact(&mut buf).await?;
    if buf != expected_hash_req2_req3_xored {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected hash of req2 and req3"));
    }

    // read and decrypt the rest of pe3 until padding
    let mut buf = [0u8; VC_LEN + 4 + 2];
    stream.read_exact(&mut buf).await?;
    decryptor.decrypt(&mut buf);
    let mut src = &buf[..];

    let remote_vc = src.get_u64();
    if remote_vc != 0u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected non-zero VC from remote peer",
        ));
    }
    let crypto_provide = src.get_u32();
    if crypto_provide & MODE_ANY == 0 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("unexpected crypto provide ({crypto_provide:#x})"),
        ));
    }
    let padding_c_len = src.get_u16() as usize;
    if padding_c_len > MAX_PADDING_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("padding C len ({padding_c_len}) exceeds {MAX_PADDING_LEN}"),
        ));
    }

    let crypto_select = if crypto_provide & MODE_RC4 != 0 {
        MODE_RC4
    } else {
        MODE_PLAINTEXT
    };

    let (mut ingress, mut egress) = stream.split();

    let read_pad_c_ia = async {
        // decrypt and discard padding C
        consume_encrypted::<MAX_PADDING_LEN>(
            &mut ingress,
            padding_c_len,
            &mut decryptor,
            "padding C",
        )
        .await?;

        // read and decrypt IA len
        let mut buf = [0u8; 2];
        ingress.read_exact(&mut buf).await?;
        decryptor.decrypt(&mut buf);
        let mut ia_len = u16::from_be_bytes(buf) as usize;

        // read and decrypt IA if any
        let mut buf = [MaybeUninit::<u8>::uninit(); 512];
        let mut rd = ReadBuf::uninit(&mut buf);
        while ia_len != 0 {
            let mut dest = rd.take(ia_len);
            let bytes_read = ingress.read_buf(&mut dest).await?;
            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "stream exhausted before IA fully read",
                ));
            }
            ia_len -= bytes_read;
            decryptor.decrypt(dest.filled_mut());
            ia_buffer.put_slice(dest.filled());
        }
        Ok(())
    };

    let write_pe4 = async {
        // prepare pe4
        let mut buf: [u8; max_pe4_len()] = rand::random();
        let mut pe4_writer = &mut buf[..];

        let padding_d_len = rand::random::<u16>() as usize % MAX_PADDING_LEN;

        pe4_writer.put_bytes(0, VC_LEN);
        pe4_writer.put_u32(crypto_select);
        pe4_writer.put_u16(padding_d_len as u16);

        // encrypt and send pe4
        let total_pe4_len = buf.len() - MAX_PADDING_LEN + padding_d_len;
        encryptor.encrypt(&mut buf[..total_pe4_len]);
        egress.write_all(&buf[..total_pe4_len]).await?;
        Ok(())
    };

    try_join!(biased; write_pe4, read_pad_c_ia)?;

    match crypto_select {
        MODE_RC4 => Ok(Some(Crypto {
            encryptor,
            decryptor,
        })),
        MODE_PLAINTEXT => Ok(None),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use local_async_utils::prelude::*;
    use std::iter;
    use tokio::join;

    #[tokio::test]
    async fn test_outbound_inbound_handshake_no_ia() {
        let (mut outbound_io, mut inbound_io) = local_pipe::duplex_pipe(128);
        let info_hash: [u8; 20] = rand::random();

        let mut ia_buf = Vec::new();

        for _ in 0..100 {
            let outbound_fut = async {
                outbound_handshake(&mut outbound_io, &info_hash, &[0u8; 0][..]).await.unwrap()
            };
            let inbound_fut = async {
                inbound_handshake(&mut inbound_io, &info_hash, &mut ia_buf).await.unwrap()
            };
            let (outbound_ret, inbound_ret) = join!(outbound_fut, inbound_fut);
            assert!(outbound_ret.is_some(), "outbound peer did not select encryption");
            assert!(inbound_ret.is_some(), "inbound peer did not select encryption");
            assert!(ia_buf.is_empty(), "unexpected IA sent by outbound peer");
        }
    }

    #[tokio::test]
    async fn test_outbound_inbound_handshake_with_ia() {
        let (mut outbound_io, mut inbound_io) = local_pipe::duplex_pipe(128);
        let info_hash: [u8; 20] = rand::random();

        for _ in 0..100 {
            let sent_ia: Vec<u8> =
                iter::repeat_with(rand::random).take(rand::random::<u16>() as usize).collect();

            let mut received_ia = Vec::new();

            let outbound_fut = async {
                outbound_handshake(&mut outbound_io, &info_hash, &sent_ia[..]).await.unwrap()
            };
            let inbound_fut = async {
                inbound_handshake(&mut inbound_io, &info_hash, &mut received_ia).await.unwrap()
            };
            let (outbound_ret, inbound_ret) = join!(outbound_fut, inbound_fut);
            assert!(outbound_ret.is_some(), "outbound peer did not select encryption");
            assert!(inbound_ret.is_some(), "inbound peer did not select encryption");
            assert_eq!(received_ia, sent_ia);
        }
    }
}
