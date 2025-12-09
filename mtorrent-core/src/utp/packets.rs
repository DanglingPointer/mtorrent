use super::protocol::Header;
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::ReadBuf;

#[derive(Debug)]
pub struct OutboundPacket(BytesMut);

impl OutboundPacket {
    pub fn new(packet_size: usize) -> Self {
        assert!(packet_size > Header::MIN_SIZE);

        let mut storage = BytesMut::with_capacity(packet_size);
        debug_assert_eq!(storage.capacity(), packet_size);

        unsafe { storage.advance_mut(Header::MIN_SIZE) }
        Self(storage)
    }

    pub fn write<R>(&mut self, f: impl FnOnce(&mut ReadBuf<'_>) -> R) -> R {
        let mut read_buf = ReadBuf::uninit(self.0.spare_capacity_mut());
        let result = f(&mut read_buf);
        let filled = read_buf.filled().len();
        unsafe { self.0.advance_mut(filled) }
        result
    }

    pub fn total_size(&self) -> usize {
        self.0.len()
    }

    pub fn is_full(&self) -> bool {
        self.0.len() == self.0.capacity()
    }

    pub fn has_data(&self) -> bool {
        self.0.len() > Header::MIN_SIZE
    }

    pub fn finalize(mut self, header: &Header) -> Bytes {
        _ = header.encode_to(&mut self.0.as_mut());
        self.0.freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_outbound_packet_finalize() {
        let packet_size = 1500;
        let mut outbound_packet = OutboundPacket::new(packet_size);
        assert!(!outbound_packet.has_data());

        let data = b"Hello, uTP!";
        outbound_packet.write(|buf| buf.put_slice(data));
        assert!(!outbound_packet.is_full());

        let header = [0x01u8; Header::MIN_SIZE];
        let header = Header::decode_from(&mut &header[..]).unwrap();
        let packet = outbound_packet.finalize(&header);

        assert_eq!(packet.len(), Header::MIN_SIZE + data.len());

        let decoded_header = Header::decode_from(&mut &packet[..]).unwrap();
        assert_eq!(decoded_header, header);

        let slice = &packet[Header::MIN_SIZE..];
        assert_eq!(slice, data);
    }

    #[test]
    fn test_outbound_packet_full() {
        let packet_size = 100;
        let mut outbound_packet = OutboundPacket::new(packet_size);
        assert!(!outbound_packet.is_full());

        outbound_packet.write(|buf| buf.put_slice(&vec![0u8; packet_size - Header::MIN_SIZE]));
        assert!(outbound_packet.is_full());
        assert_eq!(outbound_packet.write(|buf| buf.remaining()), 0);

        let header = [0x11u8; Header::MIN_SIZE];
        let header = Header::decode_from(&mut &header[..]).unwrap();
        let packet = outbound_packet.finalize(&header);

        assert_eq!(packet.len(), packet_size);

        let decoded_header = Header::decode_from(&mut &packet[..]).unwrap();
        assert_eq!(decoded_header, header);

        let slice = &packet[Header::MIN_SIZE..];
        assert_eq!(slice, &vec![0u8; packet_size - Header::MIN_SIZE][..]);
    }
}
