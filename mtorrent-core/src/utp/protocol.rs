use super::seq::{Seq, seq};
use bytes::{Buf, BufMut};
use log::log_enabled;
use std::time::{Duration, UNIX_EPOCH};
use std::{cmp, fmt, io, mem};
use thiserror::Error;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeVer {
    Data = 0x01,
    Fin = 0x11,
    State = 0x21,
    Reset = 0x31,
    Syn = 0x41,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    type_ver: TypeVer,
    extension: u8,
    connection_id: u16,
    timestamp_us: u32,
    timestamp_diff_us: u32,
    wnd_size: u32,
    seq_nr: Seq,
    ack_nr: Seq,
}

impl Header {
    pub const MIN_SIZE: usize = 20;

    pub fn type_ver(&self) -> TypeVer {
        self.type_ver
    }

    pub fn seq_nr(&self) -> Seq {
        self.seq_nr
    }

    pub fn ack_nr(&self) -> Seq {
        self.ack_nr
    }

    /// # Errors
    /// Returns error if `dst` buffer is not big enough.
    pub fn encode_to(&self, dst: &mut impl BufMut) -> io::Result<()> {
        if dst.remaining_mut() < Self::MIN_SIZE {
            return Err(io::Error::new(io::ErrorKind::OutOfMemory, "dest buffer too short"));
        }
        dst.put_u8(self.type_ver as u8);
        dst.put_u8(self.extension);
        dst.put_u16(self.connection_id);
        dst.put_u32(self.timestamp_us);
        dst.put_u32(self.timestamp_diff_us);
        dst.put_u32(self.wnd_size);
        dst.put_u16(self.seq_nr.into());
        dst.put_u16(self.ack_nr.into());
        Ok(())
    }

    /// # Errors
    /// Returns error if `src` buffer is not big enough or contains unrecognized type or version.
    pub fn decode_from(src: &mut impl Buf) -> io::Result<Self> {
        if src.remaining() < Self::MIN_SIZE {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "src buffer too short"));
        }
        let type_ver = match src.get_u8() {
            i if i == TypeVer::Data as u8 => TypeVer::Data,
            i if i == TypeVer::Fin as u8 => TypeVer::Fin,
            i if i == TypeVer::State as u8 => TypeVer::State,
            i if i == TypeVer::Reset as u8 => TypeVer::Reset,
            i if i == TypeVer::Syn as u8 => TypeVer::Syn,
            i => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("unsupported type_ver ({i:#04x})"),
                ));
            }
        };
        let extension = src.get_u8();
        let connection_id = src.get_u16();
        let timestamp_us = src.get_u32();
        let timestamp_diff_us = src.get_u32();
        let wnd_size = src.get_u32();
        let seq_nr = src.get_u16().into();
        let ack_nr = src.get_u16().into();
        Ok(Header {
            type_ver,
            extension,
            connection_id,
            timestamp_us,
            timestamp_diff_us,
            wnd_size,
            seq_nr,
            ack_nr,
        })
    }
}

#[derive(Debug)]
pub struct Extension<'d> {
    #[cfg_attr(not(test), expect(dead_code))]
    pub ext_type: u8,
    #[cfg_attr(not(test), expect(dead_code))]
    pub data: &'d [u8],
}

pub struct ExtensionIter<'d> {
    ext_type: u8,
    rest: &'d [u8],
}

impl<'d> ExtensionIter<'d> {
    pub fn new(header: &Header, data: &'d [u8]) -> Self {
        Self {
            ext_type: header.extension,
            rest: data,
        }
    }

    pub fn into_rest(self) -> &'d [u8] {
        self.rest
    }

    fn parse_next_extension(&mut self) -> io::Result<Extension<'d>> {
        let next_ext_type = self.rest.try_get_u8()?;
        let this_ext_len = self.rest.try_get_u8()? as usize;
        let this_ext_data = self.rest.split_off(..this_ext_len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "buffer not long enough to parse extension")
        })?;
        Ok(Extension {
            ext_type: mem::replace(&mut self.ext_type, next_ext_type),
            data: this_ext_data,
        })
    }
}

impl<'d> Iterator for ExtensionIter<'d> {
    type Item = io::Result<Extension<'d>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ext_type == 0 {
            None
        } else {
            Some(self.parse_next_extension().inspect_err(|_e| {
                self.ext_type = 0; // return None next time to avoid inifinite loop
            }))
        }
    }
}

pub fn skip_extensions(buffer: &mut impl Buf, header: &Header) -> io::Result<()> {
    let mut iter = ExtensionIter::new(header, buffer.chunk());
    for parse_result in &mut iter {
        let ext = parse_result?;
        if log_enabled!(log::Level::Trace) {
            log::trace!("Parsed extension: {ext:?}");
        }
    }
    buffer.advance(buffer.chunk().len() - iter.into_rest().len());
    Ok(())
}

pub fn dbg_hdr_and_ext(header: &Header, payload: &[u8]) -> impl fmt::Debug {
    struct Dump<'h, 'b> {
        header: &'h Header,
        payload: &'b [u8],
    }

    impl<'h, 'b> fmt::Debug for Dump<'h, 'b> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_list()
                .entry(self.header)
                .entries(ExtensionIter::new(self.header, self.payload))
                .finish()
        }
    }

    Dump { header, payload }
}

#[derive(Debug)]
pub struct ConnectionState {
    conn_id_recv: u16,
    conn_id_send: u16,
    next_local_seq: Seq,  // next tx seq_nr
    last_remote_seq: Seq, // last rx seq_nr
    remote_wnd: u32,
    local_wnd: u32,
    reply_micro: u32,
}

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("invalid packet ({0})")]
    Invalid(&'static str),
    #[error("duplicate packet")]
    Duplicate,
    #[error("seq jump (expected {expected_seq}, got {received_seq})")]
    OutOfOrder {
        expected_seq: Seq,
        received_seq: Seq,
    },
}

impl ConnectionState {
    const MAX_LOCAL_WINDOW: u32 = 128 * 1024;
    const MIN_LOCAL_WINDOW: u32 = 1024;
    const MIN_WINDOW: u32 = 150;

    pub fn new_outbound(conn_id_recv: u16) -> Self {
        let conn_id_send = conn_id_recv.wrapping_add(1);
        Self {
            conn_id_recv,
            conn_id_send,
            next_local_seq: Seq::ZERO,
            last_remote_seq: Seq::ZERO,
            remote_wnd: 0,
            local_wnd: Self::MAX_LOCAL_WINDOW,
            reply_micro: 0,
        }
    }

    pub fn new_inbound(syn: &Header) -> Self {
        debug_assert!(syn.type_ver == TypeVer::Syn);
        Self {
            conn_id_recv: syn.connection_id.wrapping_add(1),
            conn_id_send: syn.connection_id,
            next_local_seq: rand::random::<u16>().into(),
            last_remote_seq: syn.seq_nr,
            remote_wnd: syn.wnd_size,
            local_wnd: Self::MAX_LOCAL_WINDOW,
            reply_micro: 0,
        }
    }

    pub fn max_window_size(&self) -> usize {
        cmp::min(self.remote_wnd, self.local_wnd) as usize
    }

    pub fn shrink_local_window(&mut self) {
        self.local_wnd = cmp::max(self.local_wnd - 1024, Self::MIN_LOCAL_WINDOW);
    }

    pub fn grow_local_window(&mut self) {
        self.local_wnd = cmp::min(self.local_wnd + 1024, Self::MAX_LOCAL_WINDOW);
    }

    pub fn validate_header(&self, received_header: &Header) -> Result<(), ValidationError> {
        if received_header.connection_id != self.conn_id_recv {
            return Err(ValidationError::Invalid("unexpected connection ID"));
        }

        if let TypeVer::Syn = received_header.type_ver() {
            return Err(ValidationError::Duplicate);
        }

        if received_header.ack_nr >= self.next_local_seq {
            return Err(ValidationError::Invalid("invalid ack nr"));
        }

        if received_header.seq_nr <= self.last_remote_seq {
            return Err(ValidationError::Duplicate);
        }

        if received_header.seq_nr > self.last_remote_seq + seq(1) {
            return Err(ValidationError::OutOfOrder {
                expected_seq: self.last_remote_seq + seq(1),
                received_seq: received_header.seq_nr,
            });
        }

        Ok(())
    }

    pub fn process_header(&mut self, received_header: &Header) {
        self.remote_wnd = cmp::max(received_header.wnd_size, Self::MIN_WINDOW);
        if received_header.type_ver != TypeVer::State {
            self.last_remote_seq = received_header.seq_nr;
        } else if self.last_remote_seq == Seq::ZERO {
            // initial STATE packet during outbound handshake
            self.last_remote_seq = received_header.seq_nr - seq(1);
        }
        if received_header.timestamp_us != 0 {
            self.reply_micro = current_timestamp_us() - received_header.timestamp_us;
        }
    }

    pub fn generate_header(&mut self, type_ver: TypeVer) -> Header {
        let seq_nr = self.next_local_seq;
        if type_ver != TypeVer::State {
            self.next_local_seq.increment();
        }
        Header {
            type_ver,
            extension: 0,
            connection_id: if type_ver == TypeVer::Syn {
                self.conn_id_recv
            } else {
                self.conn_id_send
            },
            timestamp_us: current_timestamp_us(),
            timestamp_diff_us: self.reply_micro,
            wnd_size: self.local_wnd,
            seq_nr,
            ack_nr: self.last_remote_seq,
        }
    }
}

fn current_timestamp_us() -> u32 {
    #[cfg(test)]
    if let Some(timestamp) = FAKE_CURRENT_TIMESTAMP_US.get() {
        return timestamp;
    }

    UNIX_EPOCH.elapsed().unwrap_or(Duration::ZERO).as_micros() as u32 // truncating cast
}

#[cfg(test)]
thread_local! {
    pub(super) static FAKE_CURRENT_TIMESTAMP_US: std::cell::Cell<Option<u32>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_header() {
        let header = Header {
            type_ver: TypeVer::Syn,
            extension: 0,
            connection_id: 0x1234,
            timestamp_us: 0x56789abc,
            timestamp_diff_us: 0xdef01234,
            wnd_size: 0x456789ab,
            seq_nr: 0x9abc.into(),
            ack_nr: 0xdef0.into(),
        };

        let mut buf = Vec::with_capacity(Header::MIN_SIZE);
        header.encode_to(&mut buf).unwrap();

        let expected_bytes = [
            0x41, 0x00, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x45, 0x67,
            0x89, 0xab, 0x9a, 0xbc, 0xde, 0xf0,
        ];
        assert_eq!(&buf[..], &expected_bytes);
    }

    #[test]
    fn test_decode_header() {
        let bytes = [
            0x21, 0x00, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x45, 0x67,
            0x89, 0xab, 0x9a, 0xbc, 0xde, 0xf0,
        ];
        let mut buf = &bytes[..];
        let header = Header::decode_from(&mut buf).unwrap();

        assert_eq!(header.type_ver, TypeVer::State);
        assert_eq!(header.extension, 0);
        assert_eq!(header.connection_id, 0x1234);
        assert_eq!(header.timestamp_us, 0x56789abc);
        assert_eq!(header.timestamp_diff_us, 0xdef01234);
        assert_eq!(header.wnd_size, 0x456789ab);
        assert_eq!(header.seq_nr, 0x9abc.into());
        assert_eq!(header.ack_nr, 0xdef0.into());
    }

    #[test]
    fn test_encode_decode_header() {
        let original_header = Header {
            type_ver: TypeVer::Fin,
            extension: 1,
            connection_id: 0x4321,
            timestamp_us: 0xabcdef01,
            timestamp_diff_us: 0x23456789,
            wnd_size: 0x89abcdef,
            seq_nr: 0xfedc.into(),
            ack_nr: 0xba98.into(),
        };

        let mut buf = Vec::with_capacity(Header::MIN_SIZE);
        original_header.encode_to(&mut buf).unwrap();

        let mut buf_slice = &buf[..];
        let decoded_header = Header::decode_from(&mut buf_slice).unwrap();

        assert_eq!(original_header, decoded_header);
    }

    #[test]
    fn test_parse_extensions() {
        let data_with_ext = [0x02, 0x02, 0x03, 0x04, 0x00, 0x01, b'x'];
        let header = Header {
            type_ver: TypeVer::Data,
            extension: 1,
            connection_id: 0,
            timestamp_us: 0,
            timestamp_diff_us: 0,
            wnd_size: 0,
            seq_nr: 0.into(),
            ack_nr: 0.into(),
        };

        let mut iter = ExtensionIter::new(&header, data_with_ext.as_slice());
        let ext1 = iter.next().unwrap().unwrap();
        assert_eq!(ext1.ext_type, 1);
        assert_eq!(ext1.data, &[0x03, 0x04]);

        let ext2 = iter.next().unwrap().unwrap();
        assert_eq!(ext2.ext_type, 2);
        assert_eq!(ext2.data, b"x");

        assert!(iter.next().is_none());
        assert!(iter.rest.is_empty());
    }

    #[test]
    fn test_parse_malformed_extensions() {
        let data_with_ext = [0x02, 0x02, 0x03, 0x04, 0x00, 0x01];
        let header = Header {
            type_ver: TypeVer::Data,
            extension: 1,
            connection_id: 0,
            timestamp_us: 0,
            timestamp_diff_us: 0,
            wnd_size: 0,
            seq_nr: 0.into(),
            ack_nr: 0.into(),
        };

        let mut iter = ExtensionIter::new(&header, data_with_ext.as_slice());
        let ext1 = iter.next().unwrap().unwrap();
        assert_eq!(ext1.ext_type, 1);
        assert_eq!(ext1.data, &[0x03, 0x04]);

        let ext2_err = iter.next().unwrap().unwrap_err();
        assert_eq!(ext2_err.kind(), io::ErrorKind::InvalidData);

        assert!(iter.next().is_none());
        assert!(iter.rest.is_empty());
    }

    #[test]
    fn test_skip_extensions() {
        let mut data_with_ext = &[0x00, 0x02, 0x03, 0x04, b'm'][..]; // next_ext_type=0, len=2, data=[0x03, 0x04]
        let header = Header {
            type_ver: TypeVer::Data,
            extension: 1,
            connection_id: 0,
            timestamp_us: 0,
            timestamp_diff_us: 0,
            wnd_size: 0,
            seq_nr: 0.into(),
            ack_nr: 0.into(),
        };

        skip_extensions(&mut data_with_ext, &header).unwrap();
        assert_eq!(data_with_ext, &[b'm'][..]); // All extension bytes should be skipped
    }

    #[test]
    fn test_current_timestamp() {
        assert_ne!(current_timestamp_us(), 0);
    }

    #[test]
    fn test_outbound_handshake_sequence() {
        // Test the outbound handshake sequence as done in Connection::outbound()
        let conn_id_recv = 0x1234;
        let mut state = ConnectionState::new_outbound(conn_id_recv);

        // Verify initial state
        assert_eq!(state.conn_id_recv, conn_id_recv);
        assert_eq!(state.conn_id_send, conn_id_recv.wrapping_add(1));
        assert_eq!(state.next_local_seq, Seq::ZERO);
        assert_eq!(state.last_remote_seq, Seq::ZERO);
        assert_eq!(state.local_wnd, ConnectionState::MAX_LOCAL_WINDOW);
        assert_eq!(state.remote_wnd, 0);

        // Step 1: Generate SYN header (as done in Connection::outbound)
        let syn_header = state.generate_header(TypeVer::Syn);

        // Verify SYN header properties
        assert_eq!(syn_header.type_ver(), TypeVer::Syn);
        assert_eq!(syn_header.connection_id, conn_id_recv); // SYN uses conn_id_recv
        assert_eq!(syn_header.seq_nr(), Seq::ZERO); // First packet starts at 0
        assert_eq!(syn_header.ack_nr(), Seq::ZERO); // No remote seq received yet
        assert_eq!(syn_header.wnd_size, ConnectionState::MAX_LOCAL_WINDOW);

        // After generating SYN, next_local_seq should be incremented
        assert_eq!(state.next_local_seq, Seq::ONE);

        // Step 2: Simulate receiving STATE response from remote
        let remote_seq = Seq::from(42);
        let remote_window = 2048;
        let state_response = Header {
            type_ver: TypeVer::State,
            extension: 0,
            connection_id: state.conn_id_recv,
            timestamp_us: 100,
            timestamp_diff_us: 0,
            wnd_size: remote_window,
            seq_nr: remote_seq,
            ack_nr: Seq::ZERO, // Acknowledging our SYN
        };

        // Step 3: Process the STATE response (as done in Connection::outbound)
        FAKE_CURRENT_TIMESTAMP_US.set(Some(200));
        state.process_header(&state_response);

        // Verify state after processing STATE response
        assert_eq!(state.last_remote_seq, remote_seq - Seq::ONE); // Initial STATE sets to seq_nr - 1
        assert_eq!(state.remote_wnd, remote_window);
        assert_eq!(state.reply_micro, 100); // 200 - 100

        // The connection should now be ready for data exchange
        // Verify we can generate a DATA header
        let data_header = state.generate_header(TypeVer::Data);
        assert_eq!(data_header.type_ver(), TypeVer::Data);
        assert_eq!(data_header.connection_id, state.conn_id_send); // DATA uses conn_id_send
        assert_eq!(data_header.seq_nr(), Seq::ONE); // Next seq after SYN
        assert_eq!(data_header.ack_nr(), remote_seq - Seq::ONE); // Ack the remote's effective seq
        assert_eq!(state.next_local_seq, Seq::from(2)); // Should increment after DATA
    }

    #[test]
    fn test_inbound_handshake_sequence() {
        // Test the inbound handshake sequence as done in Connection::inbound()

        // Step 1: Simulate the received SYN packet (already parsed before creating connection)
        let remote_conn_id = 0x5678;
        let remote_seq = Seq::from(100);
        let remote_window = 1536;
        let syn_header = Header {
            type_ver: TypeVer::Syn,
            extension: 0,
            connection_id: remote_conn_id,
            timestamp_us: 50,
            timestamp_diff_us: 0,
            wnd_size: remote_window,
            seq_nr: remote_seq,
            ack_nr: Seq::ZERO,
        };

        // Step 2: Create inbound connection state (as done in Connection::inbound)
        let mut state = ConnectionState::new_inbound(&syn_header);

        // Verify initial inbound state
        assert_eq!(state.conn_id_recv, remote_conn_id.wrapping_add(1));
        assert_eq!(state.conn_id_send, remote_conn_id);
        assert_eq!(state.last_remote_seq, remote_seq); // Set from SYN
        assert_eq!(state.remote_wnd, remote_window); // Set from SYN
        assert_eq!(state.local_wnd, ConnectionState::MAX_LOCAL_WINDOW);
        // next_local_seq is random, so we can't test exact value
        let initial_local_seq = state.next_local_seq;

        // Step 3: Generate STATE response (as done in Connection::inbound)
        let state_header = state.generate_header(TypeVer::State);

        // Verify STATE header properties
        assert_eq!(state_header.type_ver(), TypeVer::State);
        assert_eq!(state_header.connection_id, state.conn_id_send); // STATE uses conn_id_send
        assert_eq!(state_header.seq_nr(), initial_local_seq); // STATE doesn't increment seq
        assert_eq!(state_header.ack_nr(), remote_seq); // Ack the received SYN
        assert_eq!(state_header.wnd_size, ConnectionState::MAX_LOCAL_WINDOW);

        // STATE packets don't increment next_local_seq
        assert_eq!(state.next_local_seq, initial_local_seq);

        // Step 4: Simulate receiving DATA response from remote
        let data_seq = remote_seq + Seq::ONE; // Remote increments after SYN
        let data_header = Header {
            type_ver: TypeVer::Data,
            extension: 0,
            connection_id: state.conn_id_recv,
            timestamp_us: 150,
            timestamp_diff_us: 0,
            wnd_size: remote_window,
            seq_nr: data_seq,
            ack_nr: initial_local_seq - Seq::ONE, // Acknowledge something less than our next seq
        };

        // Step 5: Validate the incoming DATA header
        let validation_result = state.validate_header(&data_header);
        assert!(
            validation_result.is_ok(),
            "DATA header validation should pass: {:?}",
            validation_result.unwrap_err()
        );

        // Step 6: Process the DATA header (as done in Connection::inbound)
        FAKE_CURRENT_TIMESTAMP_US.set(Some(250));
        state.process_header(&data_header);

        // Verify state after processing DATA
        assert_eq!(state.last_remote_seq, data_seq); // Updated to DATA seq
        assert_eq!(state.remote_wnd, remote_window);
        assert_eq!(state.reply_micro, 100); // 250 - 150

        // The connection should now be ready for continued data exchange
        // Verify we can generate another DATA header
        let next_data_header = state.generate_header(TypeVer::Data);
        assert_eq!(next_data_header.type_ver(), TypeVer::Data);
        assert_eq!(next_data_header.connection_id, state.conn_id_send);
        assert_eq!(next_data_header.seq_nr(), initial_local_seq); // First DATA uses initial seq
        assert_eq!(next_data_header.ack_nr(), data_seq); // Ack the received DATA
        assert_eq!(state.next_local_seq, initial_local_seq + Seq::ONE); // Incremented after DATA
    }
}
