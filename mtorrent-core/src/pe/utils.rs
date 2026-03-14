use super::{Decryptor, PrefixedStream};
use crate::pwp::PROTOCOL_STRING;
use bytes::BufMut;
use std::io;
use std::mem::MaybeUninit;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

/// Return value of [`detect_encryption()`].
pub enum MaybeEncrypted<T> {
    Plain(T),
    Encrypted(T),
}

/// Determine if the stream is likely encrypted or not by checking if the first bytes match the
/// BitTorrent protocol string.
pub async fn detect_encryption<S: AsyncRead + Unpin>(
    mut stream: S,
) -> io::Result<MaybeEncrypted<PrefixedStream<io::Cursor<[u8; PROTOCOL_STRING.len()]>, S>>> {
    let mut buf = [0u8; PROTOCOL_STRING.len()];
    stream.read_exact(&mut buf).await?;
    let is_unecrypted = buf == PROTOCOL_STRING;

    let stream = PrefixedStream::new(io::Cursor::new(buf), stream);
    if is_unecrypted {
        Ok(MaybeEncrypted::Plain(stream))
    } else {
        Ok(MaybeEncrypted::Encrypted(stream))
    }
}

/// ```ignore
/// sha1_of![b""];
/// ```
macro_rules! sha1_of {
    ($($slices:expr),+) => {{
        let mut hasher = sha1_smol::Sha1::new();
        $(hasher.update($slices);)+
        hasher.digest().bytes()
    }};
}
pub(super) use sha1_of;

pub(super) fn xor_arrays<const N: usize>(arr1: [u8; N], arr2: [u8; N]) -> [u8; N] {
    let mut result = [0u8; N];
    for i in 0..N {
        result[i] = arr1[i] ^ arr2[i];
    }
    result
}

/// Read and discard encrypted data from `stream`.
pub(super) async fn consume_encrypted<const MAX_LEN: usize>(
    mut stream: impl AsyncReadExt + Unpin,
    len: usize,
    decryptor: &mut Decryptor,
    what: &'static str,
) -> io::Result<()> {
    let mut buf = [MaybeUninit::<u8>::uninit(); MAX_LEN];
    let mut rd = ReadBuf::uninit(&mut buf);
    let mut rd = rd.take(len);

    while 0 != stream.read_buf(&mut rd).await? {}

    if rd.filled().len() != len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("stream exhausted before {what} fully read"),
        ));
    }

    decryptor.decrypt(rd.filled_mut());
    Ok(())
}

/// Read data from `source` until `pattern` is found, consuming and discarding the pattern and all
/// data before it.
pub(super) async fn consume_through<const N: usize>(
    mut source: impl AsyncReadExt + Unpin,
    pattern: &[u8; N],
) -> io::Result<()> {
    let mut storage = [MaybeUninit::<u8>::uninit(); N];
    let mut buf = ReadBuf::uninit(&mut storage);

    let mut overlap_ind = None;

    loop {
        let max_to_read = overlap_ind.unwrap_or(N);
        let bytes_read = source.read_buf(&mut buf.take(max_to_read)).await?;
        if 0 == bytes_read {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "stream exhausted before pattern found",
            ));
        }
        unsafe { buf.advance_mut(bytes_read) }

        if let Some(last_n) = buf.filled().last_chunk::<N>() {
            overlap_ind = overlap_start_index(pattern, last_n);

            match overlap_ind {
                Some(0) => break,
                None => buf.clear(),
                Some(n) => {
                    buf.filled_mut().copy_within(n.., 0);
                    buf.set_filled(N - n);
                }
            }
        }
    }

    Ok(())
}

/// Returns index into `data` such that data[ret..] == pattern[..-ret]
fn overlap_start_index<const N: usize>(pattern: &[u8; N], data: &[u8; N]) -> Option<usize> {
    let mut data_ind = 0;
    let mut pattern_ind = 0;
    let mut ret = None;

    while data_ind < N {
        if data[data_ind] == pattern[pattern_ind] {
            ret.get_or_insert(data_ind);
            data_ind += 1;
            pattern_ind += 1;
        } else {
            if let Some(old_ret) = ret.take() {
                data_ind = old_ret;
                pattern_ind = 0;
            }
            data_ind += 1;
        }
    }

    ret
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter;

    #[test]
    fn test_overlap_start_index() {
        // No overlap
        assert_eq!(overlap_start_index(b"wxyz", b"abcd"), None);

        // Full overlap: data == pattern → entire data is a prefix of pattern
        assert_eq!(overlap_start_index(b"abcd", b"abcd"), Some(0));

        // Partial overlap at middle: data[2..] = "cd" == pattern[..2]
        assert_eq!(overlap_start_index(b"cdef", b"abcd"), Some(2));

        // Single-byte overlap at end: data[3..] = "a" == pattern[..1]
        assert_eq!(overlap_start_index(b"axyz", b"bcda"), Some(3));

        // Prefers earliest (longest) overlap: both i=0 and i=2 are valid
        assert_eq!(overlap_start_index(&[1, 2, 1, 2], &[1, 2, 1, 2]), Some(0));

        // Repeated bytes: data[1..] = [1,1,1] == pattern[..3]
        assert_eq!(overlap_start_index(&[1, 1, 1, 2], &[1, 1, 1, 1]), Some(1));

        // Backtrack then find shorter: data[1..] = [1,2] == pattern[..2]
        assert_eq!(overlap_start_index(&[1, 2, 3], &[1, 1, 2]), Some(1));

        // Self-overlapping pattern: data[2..] = [1,2,1] == pattern[..3]
        assert_eq!(overlap_start_index(&[1, 2, 1, 2, 3], &[1, 2, 1, 2, 1]), Some(2));
    }

    #[tokio::test]
    async fn test_consume_through_pattern_at_start() {
        let pattern: [u8; 4] = [7, 8, 9, 10];
        let tail = [100u8, 101, 102, 103];

        let mut input = Vec::new();
        input.extend_from_slice(&pattern);
        input.extend_from_slice(&tail);

        let mut source = io::Cursor::new(input);
        consume_through(&mut source, &pattern).await.unwrap();

        let mut buf = Vec::new();
        source.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.as_slice(), &tail);
    }

    #[tokio::test]
    async fn test_consume_through_empty_tail() {
        // Pattern is at the very end of the stream; nothing should remain after consuming.
        let head = [0u8, 1, 2, 3, 4, 5];
        let pattern: [u8; 4] = [7, 8, 9, 10];

        let mut input = Vec::new();
        input.extend_from_slice(&head);
        input.extend_from_slice(&pattern);

        let mut source = io::Cursor::new(input);
        consume_through(&mut source, &pattern).await.unwrap();

        let mut buf = Vec::new();
        source.read_to_end(&mut buf).await.unwrap();
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn test_consume_through_not_found() {
        // Stream ends without containing the pattern → UnexpectedEof.
        let pattern: [u8; 4] = [7, 8, 9, 10];
        let data = [0u8, 1, 2, 3, 4, 5];

        let mut source = io::Cursor::new(data);
        let err = consume_through(&mut source, &pattern).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn test_consume_through_self_overlapping_pattern() {
        // Pattern [1,2,1,2,3] has an internal overlap: the prefix [1,2] also appears
        // at position 2 within the pattern.  The stream starts with a false match
        // [1,2,1,2,1] before the real occurrence [1,2,1,2,3], exercising backtracking.
        let pattern: [u8; 5] = [1, 2, 1, 2, 3];
        let tail = [99u8, 100];

        let mut input: Vec<u8> = vec![1, 2, 1, 2, 1, 2, 3]; // false start at 0, real match at 2
        input.extend_from_slice(&tail);

        let mut source = io::Cursor::new(input);
        consume_through(&mut source, &pattern).await.unwrap();

        let mut buf = Vec::new();
        source.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.as_slice(), &tail);
    }

    #[tokio::test]
    async fn test_consume_through_multiple_occurrences_stops_at_first() {
        // The pattern appears twice; consume_through must stop at the first occurrence.
        let pattern: [u8; 3] = [1, 2, 3];
        let between = [50u8, 51, 52];

        let mut input = Vec::new();
        input.extend_from_slice(&[9u8, 8, 7]); // head before first occurrence
        input.extend_from_slice(&pattern);
        input.extend_from_slice(&between);
        input.extend_from_slice(&pattern); // second occurrence

        let mut source = io::Cursor::new(input);
        consume_through(&mut source, &pattern).await.unwrap();

        let mut buf = Vec::new();
        source.read_to_end(&mut buf).await.unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(&between);
        expected.extend_from_slice(&pattern);
        assert_eq!(buf, expected);
    }

    #[tokio::test]
    async fn test_consume_through_buffer_exhaustion() {
        let data: &[u8] = &[
            144, 37, 224, 143, 67, 254, 129, 194, 32, 127, 151, 215, 163, 80, 106, 252, 181, 23,
            132, 37, 53, 13, 156, 161, 189, 157, 209, 38, 142, 221, 192, 27, 229, 224, 50, 204, 91,
            99, 94, 173, 25, 201, 161, 160, 251, 41, 58, 128, 156, 233, 160, 195, 234, 179, 140,
            160, 14, 194, 161, 87, 203, 148, 114, 2, 24, 122, 18, 117, 196, 86, 153, 147, 35, 241,
            182, 173, 212, 107, 80, 14, 49, 125, 91, 100, 10, 232, 36, 166, 250, 241, 82, 118, 6,
            53, 188, 24, 41, 176, 109, 20, 99, 120, 191, 218, 114, 91, 161, 178, 27, 137, 184, 251,
            52, 222, 116, 232, 153, 101, 173, 121, 229, 39, 247, 65, 1, 46, 216, 14, 1, 2, 3, 4, 5,
            162, 244, 37, 212, 65, 33, 45, 215, 68, 110, 244, 216, 155, 107, 160, 199, 149, 175,
            168, 75, 51, 195, 151, 235, 166, 68, 181, 163, 12, 153, 243, 211, 245, 148, 122, 106,
            250, 195, 215, 122, 218, 43, 0, 204, 241, 186, 223, 201, 101, 188, 170, 244, 226, 195,
            86, 254, 81, 157, 192, 141, 100, 12, 62, 179,
        ];
        let pattern: [u8; 5] = [1, 2, 3, 4, 5];

        let mut source = io::Cursor::new(data);
        consume_through(&mut source, &pattern).await.unwrap();

        let expected_tail = [
            162, 244, 37, 212, 65, 33, 45, 215, 68, 110, 244, 216, 155, 107, 160, 199, 149, 175,
            168, 75, 51, 195, 151, 235, 166, 68, 181, 163, 12, 153, 243, 211, 245, 148, 122, 106,
            250, 195, 215, 122, 218, 43, 0, 204, 241, 186, 223, 201, 101, 188, 170, 244, 226, 195,
            86, 254, 81, 157, 192, 141, 100, 12, 62, 179,
        ];
        let mut buf = Vec::new();
        source.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.as_slice(), &expected_tail);
    }

    #[tokio::test]
    async fn test_consume_through_fuzz() {
        for _ in 0..10_000 {
            let pattern: [u8; 5] = [1, 2, 3, 4, 5];
            let tail = b"tail data";

            let input = {
                let head_len = (rand::random::<u16>() % 512) as usize;
                let mut tmp: Vec<u8> = iter::repeat_with(rand::random).take(head_len).collect();
                tmp.extend_from_slice(&pattern);
                tmp.extend_from_slice(tail);
                tmp
            };

            let mut source = io::Cursor::new(input);
            if let Err(e) = consume_through(&mut source, &pattern).await {
                panic!("consume_through failed: {:?}\n{:?}", e, source.get_ref());
            }

            let mut buf = Vec::new();
            source.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, tail);
        }
    }

    #[tokio::test]
    async fn test_is_stream_unencrypted() {
        let unencrypted_stream = io::Cursor::new(PROTOCOL_STRING);
        match detect_encryption(unencrypted_stream).await.unwrap() {
            MaybeEncrypted::Plain(mut stream) => {
                let mut buf = Vec::new();
                stream.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf, PROTOCOL_STRING);
            }
            MaybeEncrypted::Encrypted(_) => panic!("unencrypted stream misclassified as encrypted"),
        }

        let encrypted_stream = io::Cursor::new(b"not the protocol string");
        match detect_encryption(encrypted_stream).await.unwrap() {
            MaybeEncrypted::Plain(_) => panic!("encrypted stream misclassified as unencrypted"),
            MaybeEncrypted::Encrypted(mut stream) => {
                let mut buf = Vec::new();
                stream.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf, b"not the protocol string");
            }
        }
    }
}
