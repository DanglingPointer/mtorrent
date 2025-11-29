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

#[derive(Debug, Clone)]
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
