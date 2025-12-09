use bytes::{Buf, BufMut};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{cmp, io};

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
    seq_nr: u16,
    ack_nr: u16,
}

impl Header {
    pub const MIN_SIZE: usize = 20;

    pub fn has_extensions(&self) -> bool {
        self.extension != 0
    }

    pub fn type_ver(&self) -> TypeVer {
        self.type_ver
    }

    pub fn seq_nr(&self) -> u16 {
        self.seq_nr
    }

    pub fn ack_nr(&self) -> u16 {
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
        dst.put_u16(self.seq_nr);
        dst.put_u16(self.ack_nr);
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
        let seq_nr = src.get_u16();
        let ack_nr = src.get_u16();
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

pub struct Extension<'d> {
    #[expect(dead_code)]
    pub ext_type: u8,
    #[expect(dead_code)]
    pub data: &'d [u8],
}

pub struct ExtensionIter<'d>(&'d [u8]);

impl<'d> Iterator for ExtensionIter<'d> {
    type Item = Extension<'d>;

    fn next(&mut self) -> Option<Self::Item> {
        let ext_type = self.0.try_get_u8().ok()?;
        if ext_type == 0 {
            None
        } else {
            let len = self.0.try_get_u8().ok()? as usize;
            self.0.split_off(..len).map(|data| Extension { ext_type, data })
        }
    }
}

impl<'d> ExtensionIter<'d> {
    pub fn new(data: &'d [u8]) -> Self {
        ExtensionIter(data)
    }

    pub fn remainder(&self) -> &'d [u8] {
        self.0
    }
}

pub fn skip_extensions(buffer: &mut impl Buf, header: &Header) {
    if header.has_extensions() {
        let mut extensions = ExtensionIter::new(buffer.chunk());
        while extensions.next().is_some() {}
        let headers_len = buffer.chunk().len() - extensions.remainder().len();
        buffer.advance(headers_len);
    }
}

pub struct ConnectionState {
    conn_id_recv: u16,
    conn_id_send: u16,
    seq_nr: u16, // last tx seq_nr
    ack_nr: u16, // last rx seq_nr
    remote_wnd: u32,
    local_wnd: u32,

    packet_size: usize,
}

impl ConnectionState {
    const LOCAL_WINDOW: u32 = 1024 * 16;
    const INIT_PACKET_SIZE: usize = 1024;

    pub fn new_outbound() -> Self {
        let conn_id_recv = rand::random();
        let conn_id_send = conn_id_recv + 1;
        Self {
            conn_id_recv,
            conn_id_send,
            seq_nr: 0,
            ack_nr: 0,
            remote_wnd: 0,
            local_wnd: Self::LOCAL_WINDOW,
            packet_size: Self::INIT_PACKET_SIZE,
        }
    }

    pub fn new_inbound(syn: &Header) -> Self {
        debug_assert!(syn.type_ver == TypeVer::Syn);
        Self {
            conn_id_recv: syn.connection_id + 1,
            conn_id_send: syn.connection_id,
            seq_nr: rand::random(),
            ack_nr: syn.seq_nr,
            remote_wnd: syn.wnd_size,
            local_wnd: Self::LOCAL_WINDOW,
            packet_size: Self::INIT_PACKET_SIZE,
        }
    }

    pub fn packet_size(&self) -> usize {
        self.packet_size
    }

    pub fn max_window_size(&self) -> usize {
        cmp::min(self.remote_wnd, self.local_wnd) as usize
    }

    pub fn validate_header(&self, received_header: &Header) -> bool {
        received_header.connection_id == self.conn_id_recv
            && (received_header.seq_nr == self.ack_nr + 1
                || received_header.type_ver == TypeVer::State)
    }

    pub fn process_header(&mut self, received_header: &Header) {
        self.remote_wnd = received_header.wnd_size;
        if received_header.type_ver != TypeVer::State || self.ack_nr == 0 {
            self.ack_nr = received_header.seq_nr;
        }
    }

    pub fn generate_header(&mut self, type_ver: TypeVer) -> Header {
        if type_ver != TypeVer::State {
            self.seq_nr += 1;
        }
        Header {
            type_ver,
            extension: 0,
            connection_id: if type_ver == TypeVer::Syn {
                self.conn_id_recv
            } else {
                self.conn_id_send
            },
            timestamp_us: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .and_then(|d| u32::try_from(d.as_micros()).ok())
                .unwrap_or_default(),
            timestamp_diff_us: 0, // TODO
            wnd_size: self.local_wnd,
            seq_nr: self.seq_nr,
            ack_nr: self.ack_nr,
        }
    }
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
            seq_nr: 0x9abc,
            ack_nr: 0xdef0,
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
        assert_eq!(header.seq_nr, 0x9abc);
        assert_eq!(header.ack_nr, 0xdef0);
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
            seq_nr: 0xfedc,
            ack_nr: 0xba98,
        };

        let mut buf = Vec::with_capacity(Header::MIN_SIZE);
        original_header.encode_to(&mut buf).unwrap();

        let mut buf_slice = &buf[..];
        let decoded_header = Header::decode_from(&mut buf_slice).unwrap();

        assert_eq!(original_header, decoded_header);
    }

    #[test]
    fn test_skip_extensions() {
        let mut data_with_ext = &[0x01, 0x02, 0x03, 0x04, 0x00, b'm'][..]; // ext_type=1, len=2, data=[0x03, 0x04], ext_type=0
        let header = Header {
            type_ver: TypeVer::Data,
            extension: 1,
            connection_id: 0,
            timestamp_us: 0,
            timestamp_diff_us: 0,
            wnd_size: 0,
            seq_nr: 0,
            ack_nr: 0,
        };

        skip_extensions(&mut data_with_ext, &header);
        assert_eq!(data_with_ext, &[b'm'][..]); // All extension bytes should be skipped
    }
}
