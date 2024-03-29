use bitvec::prelude::*;
use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};

pub type ReservedBits = BitArray<[u8; 8], Lsb0>;

pub fn reserved_bits(extended_protocol: bool) -> ReservedBits {
    let mut bits = ReservedBits::ZERO;
    bits.set(44, extended_protocol);
    bits
}

pub fn is_extension_protocol_enabled(reserved: &ReservedBits) -> bool {
    reserved[44]
}

#[derive(Clone, PartialEq, Debug)]
pub struct Handshake {
    pub peer_id: [u8; 20],
    pub info_hash: [u8; 20],
    pub reserved: ReservedBits,
}

impl Default for Handshake {
    fn default() -> Self {
        Handshake {
            peer_id: [0u8; 20],
            info_hash: [0u8; 20],
            reserved: BitArray::ZERO,
        }
    }
}

pub(super) async fn do_handshake_incoming<S>(
    remote_ip: &SocketAddr,
    mut socket: S,
    local_handshake: &Handshake,
    use_remote_info_hash: bool,
) -> io::Result<(S, Handshake)>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    // Read remote handshake up until peer id,
    // then send entire local handshake (with either local info_hash or remote one),
    // then read remote peer id.
    log::debug!("Receiving incoming handshake from {}", remote_ip);

    let mut remote_handshake = Handshake::default();

    socket = read_pstr_and_reserved(socket, &mut remote_handshake.reserved).await?;
    socket.read_exact(&mut remote_handshake.info_hash).await?;
    if !use_remote_info_hash && local_handshake.info_hash != remote_handshake.info_hash {
        return Err(io::Error::new(io::ErrorKind::Other, "info_hash doesn't match"));
    }

    let mut writer = BufWriter::new(socket);
    writer = write_pstr_and_reserved(writer, &local_handshake.reserved).await?;
    if use_remote_info_hash {
        writer.write_all(&remote_handshake.info_hash).await?;
    } else {
        writer.write_all(&local_handshake.info_hash).await?;
    }
    writer.write_all(&local_handshake.peer_id).await?;
    writer.flush().await?;

    let mut socket = writer.into_inner();
    socket.read_exact(&mut remote_handshake.peer_id).await?;

    if remote_handshake.peer_id == local_handshake.peer_id {
        // possible because some trackers include our own external ip
        Err(io::Error::new(io::ErrorKind::Other, "incoming connect from ourselves"))
    } else {
        log::debug!(
            "Incoming handshake with {} DONE. Peer id: {}",
            remote_ip,
            String::from_utf8_lossy(&remote_handshake.peer_id[0..8])
        );
        Ok((socket, remote_handshake))
    }
}

pub(super) async fn do_handshake_outgoing<S>(
    remote_ip: &SocketAddr,
    socket: S,
    local_handshake: &Handshake,
    expected_remote_peer_id: Option<&[u8; 20]>,
) -> io::Result<(S, Handshake)>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    // Send local hanshake up until peer id,
    // then wait for the entire remote handshake,
    // then send local peer id.
    log::debug!("Starting outgoing handshake with {}", remote_ip);

    let mut writer = BufWriter::new(socket);
    writer = write_pstr_and_reserved(writer, &local_handshake.reserved).await?;
    writer.write_all(&local_handshake.info_hash).await?;
    writer.flush().await?;

    let mut remote_handshake = Handshake::default();

    let mut socket = writer.into_inner();
    socket = read_pstr_and_reserved(socket, &mut remote_handshake.reserved).await?;

    socket.read_exact(&mut remote_handshake.info_hash).await?;
    if local_handshake.info_hash != remote_handshake.info_hash {
        return Err(io::Error::new(io::ErrorKind::Other, "info_hash doesn't match"));
    }

    socket.read_exact(&mut remote_handshake.peer_id).await?;
    if matches!(expected_remote_peer_id, Some(id) if id != &remote_handshake.peer_id) {
        return Err(io::Error::new(io::ErrorKind::Other, "remote peer_id doesn't match"));
    }

    socket.write_all(&local_handshake.peer_id).await?;

    log::debug!(
        "Outgoing handshake with {} DONE. Peer id: {}",
        remote_ip,
        String::from_utf8_lossy(&remote_handshake.peer_id[0..8])
    );
    Ok((socket, remote_handshake))
}

async fn write_pstr_and_reserved<S: AsyncWriteExt + Unpin>(
    mut sink: S,
    reserved: &ReservedBits,
) -> io::Result<S> {
    sink.write_all(&[19u8]).await?;
    sink.write_all("BitTorrent protocol".as_bytes()).await?;
    sink.write_all(&reserved.data).await?;
    Ok(sink)
}

async fn read_pstr_and_reserved<S: AsyncReadExt + Unpin>(
    mut source: S,
    reserved: &mut ReservedBits,
) -> io::Result<S> {
    let pstr_len = {
        let mut pstr_len_byte = [0u8; 1];
        source.read_exact(&mut pstr_len_byte).await?;
        pstr_len_byte[0] as usize
    };
    let pstr = {
        let mut pstr_bytes = vec![0u8; pstr_len];
        source.read_exact(&mut pstr_bytes).await?;
        String::from_utf8_lossy(&pstr_bytes).to_string()
    };
    if pstr != "BitTorrent protocol" {
        return Err(io::Error::new(io::ErrorKind::Other, format!("Unknown protocol: '{}'", pstr)));
    }
    source.read_exact(&mut reserved.data).await?;
    Ok(source)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::join;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use tokio::io::duplex;

    const IP: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));

    #[tokio::test]
    async fn test_handshake_specified_server_info_hash() {
        let (server_stream, client_stream) = duplex(1024);

        let client_hs_data = Handshake {
            peer_id: [1u8; 20],
            info_hash: [7u8; 20],
            reserved: BitArray::from([0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x01]),
        };
        let server_hs_data = Handshake {
            peer_id: [2u8; 20],
            info_hash: [7u8; 20],
            reserved: BitArray::ZERO,
        };

        let client_hs_fut = async {
            do_handshake_outgoing(
                &IP,
                client_stream,
                &client_hs_data,
                Some(&server_hs_data.peer_id),
            )
            .await
            .unwrap()
            .1
        };
        let server_hs_fut = async {
            do_handshake_incoming(&IP, server_stream, &server_hs_data, false)
                .await
                .unwrap()
                .1
        };

        let (received_server_hs, received_client_hs): (Handshake, Handshake) =
            join!(client_hs_fut, server_hs_fut);

        assert_eq!(server_hs_data, received_server_hs);
        assert_eq!(client_hs_data, received_client_hs);
        assert!(received_client_hs.reserved[44]);
        assert!(received_client_hs.reserved[56]);
    }

    #[tokio::test]
    async fn test_handshake_any_server_info_hash() {
        let (server_stream, client_stream) = duplex(1024);

        let client_hs_data = Handshake {
            peer_id: [1u8; 20],
            info_hash: [7u8; 20],
            reserved: BitArray::ZERO,
        };
        let server_hs_data = Handshake {
            peer_id: [2u8; 20],
            info_hash: [7u8; 20],
            reserved: BitArray::from([0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x04]),
        };

        let client_hs_fut = async {
            do_handshake_outgoing(
                &IP,
                client_stream,
                &client_hs_data,
                Some(&server_hs_data.peer_id),
            )
            .await
            .unwrap()
            .1
        };
        let server_hs_fut = async {
            do_handshake_incoming(&IP, server_stream, &server_hs_data, true)
                .await
                .unwrap()
                .1
        };

        let (received_server_hs, received_client_hs): (Handshake, Handshake) =
            join!(client_hs_fut, server_hs_fut);

        assert_eq!(server_hs_data, received_server_hs);
        assert_eq!(client_hs_data, received_client_hs);
        assert!(received_server_hs.reserved[44]);
        assert!(received_server_hs.reserved[58]);
    }

    #[tokio::test]
    async fn test_handshake_peer_id_doesnt_match() {
        let (server_stream, client_stream) = duplex(1024);

        let client_hs_data = Handshake {
            peer_id: [1u8; 20],
            info_hash: [7u8; 20],
            reserved: BitArray::ZERO,
        };
        let server_hs_data = Handshake {
            peer_id: [2u8; 20],
            info_hash: [7u8; 20],
            reserved: BitArray::ZERO,
        };

        let client_hs_fut = async {
            let result =
                do_handshake_outgoing(&IP, client_stream, &client_hs_data, Some(&[0u8; 20])).await;
            let error: io::Error = result.err().unwrap();
            assert_eq!("remote peer_id doesn't match", error.to_string(),)
        };
        let server_hs_fut = async {
            let result = do_handshake_incoming(&IP, server_stream, &server_hs_data, true).await;
            assert!(result.is_err());
        };
        join!(client_hs_fut, server_hs_fut);
    }

    #[tokio::test]
    async fn test_handshake_info_hash_doesnt_match() {
        let (server_stream, client_stream) = duplex(1024);

        let client_hs_data = Handshake {
            peer_id: [1u8; 20],
            info_hash: [7u8; 20],
            reserved: BitArray::ZERO,
        };
        let server_hs_data = Handshake {
            peer_id: [2u8; 20],
            info_hash: [8u8; 20],
            reserved: BitArray::ZERO,
        };

        let client_hs_fut = async {
            let result = do_handshake_outgoing(&IP, client_stream, &client_hs_data, None).await;
            assert!(result.is_err());
        };
        let server_hs_fut = async {
            let result = do_handshake_incoming(&IP, server_stream, &server_hs_data, false).await;
            let error: io::Error = result.err().unwrap();
            assert_eq!("info_hash doesn't match", error.to_string())
        };
        join!(client_hs_fut, server_hs_fut);
    }

    #[tokio::test]
    async fn test_handshake_parse_entire_real_hanshake_message() {
        let server_hs_msg = b"\x13\x42\x69\x74\x54\x6f\x72\x72\x65\x6e\x74\x20\x70\x72\x6f\x74\
            \x6f\x63\x6f\x6c\x00\x00\x00\x00\x00\x10\x00\x05\x74\x4f\x27\x27\
            \xce\x5d\x3c\x4d\x6b\xa4\xcf\x5b\xa7\xac\x08\x78\x46\x0a\x9e\xed\
            \x2d\x42\x54\x37\x61\x35\x57\x2d\x11\xb4\x8d\x05\x19\x2c\x3e\x33\
            \x88\x7c\x4b\xca";

        let (mut server_stream, client_stream) = duplex(1024);

        let client_hs_data = Handshake {
            peer_id: [1u8; 20],
            info_hash:
                *b"\x74\x4f\x27\x27\xce\x5d\x3c\x4d\x6b\xa4\xcf\x5b\xa7\xac\x08\x78\x46\x0a\x9e\xed",
            reserved: BitArray::ZERO,
        };

        let client_hs_fut = async {
            do_handshake_outgoing(&IP, client_stream, &client_hs_data, None)
                .await
                .unwrap()
                .1
        };

        let server_fut = async {
            server_stream.write_all(&server_hs_msg[..]).await.unwrap();
        };

        let (received_server_hs, _) = join!(client_hs_fut, server_fut);

        let received_server_hs: Handshake = received_server_hs;

        assert_eq!(*b"\x00\x00\x00\x00\x00\x10\x00\x05", received_server_hs.reserved.data);
        assert_eq!(client_hs_data.info_hash, received_server_hs.info_hash);

        assert_eq!(
            *b"\x2d\x42\x54\x37\x61\x35\x57\x2d\x11\xb4\x8d\x05\x19\x2c\x3e\x33\x88\x7c\x4b\xca",
            received_server_hs.peer_id
        );

        assert!(received_server_hs.reserved[44]); // Extension protocol
        assert!(received_server_hs.reserved[56]); // DHT
        assert!(received_server_hs.reserved[58]); // Fast Extension
    }
}
