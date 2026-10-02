use super::protocol::Header;
use super::seq::Seq;
use bytes::Bytes;
use std::iter;

/// Buffers out-of-order DATA packets until the gap before them is filled.
///
/// The caller must drain [`Reorderer::next_packet()`] after each in-order DATA packet, before
/// adding any more packets.
pub struct Reorderer {
    ringbuf: Box<[Option<(Header, Bytes)>]>,
}

impl Reorderer {
    /// Power of two, so that `seq_nr % REORDER_WINDOW` stays contiguous across seq wraparound.
    const REORDER_WINDOW: u16 = 512;

    pub fn new() -> Self {
        const { assert!(Self::REORDER_WINDOW.is_power_of_two()) }
        Self {
            ringbuf: iter::repeat_n(None, Self::REORDER_WINDOW as usize).collect(),
        }
    }

    /// Drops packets that are not within the window ahead of `expected_seq`.
    pub fn add_packet(&mut self, header: Header, data: Bytes, expected_seq: Seq) {
        let actual_seq = header.seq_nr;
        debug_assert_ne!(actual_seq, expected_seq);
        if u16::from(actual_seq - expected_seq) < Self::REORDER_WINDOW {
            self.ringbuf[Self::index_of(actual_seq)] = Some((header, data));
        }
    }

    /// Takes the buffered packet that follows `last_seq`, if any.
    pub fn next_packet(&mut self, last_seq: Seq) -> Option<(Header, Bytes)> {
        let expected_next_seq = last_seq + Seq::ONE;
        let packet = self.ringbuf[Self::index_of(expected_next_seq)].take();
        debug_assert!(packet.as_ref().is_none_or(|(header, _)| header.seq_nr == expected_next_seq));
        packet
    }

    fn index_of(seq: Seq) -> usize {
        (u16::from(seq) % Self::REORDER_WINDOW) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utp::protocol::TypeVer;
    use crate::utp::seq::seq;

    const WINDOW: u16 = Reorderer::REORDER_WINDOW;

    fn data_packet(seq_nr: Seq) -> (Header, Bytes) {
        let header = Header {
            type_ver: TypeVer::Data,
            extension: 0,
            connection_id: 42,
            timestamp_us: 0,
            timestamp_diff_us: 0,
            wnd_size: 0,
            seq_nr,
            ack_nr: seq(0),
        };
        (header, Bytes::from(format!("payload {seq_nr}")))
    }

    fn add(reorderer: &mut Reorderer, seq_nr: Seq, expected_seq: Seq) {
        let (header, data) = data_packet(seq_nr);
        reorderer.add_packet(header, data, expected_seq);
    }

    /// Delivers `first` and drains the reorderer, returning all delivered seq numbers.
    fn deliver_and_drain(reorderer: &mut Reorderer, first: Seq) -> Vec<Seq> {
        let mut delivered = vec![first];
        let mut last_seq = first;
        while let Some((header, data)) = reorderer.next_packet(last_seq) {
            assert_eq!(header.seq_nr, last_seq + seq(1));
            assert_eq!(data, data_packet(header.seq_nr).1);
            last_seq = header.seq_nr;
            delivered.push(last_seq);
        }
        delivered
    }

    #[test]
    fn test_empty_reorderer_returns_nothing() {
        let mut reorderer = Reorderer::new();
        assert!(reorderer.next_packet(seq(0)).is_none());
        assert!(reorderer.next_packet(seq(u16::MAX)).is_none());
    }

    #[test]
    fn test_buffered_packets_are_released_in_order_after_gap_is_filled() {
        let mut reorderer = Reorderer::new();
        add(&mut reorderer, seq(13), seq(10));
        add(&mut reorderer, seq(11), seq(10));
        add(&mut reorderer, seq(12), seq(10));
        assert!(reorderer.next_packet(seq(8)).is_none());

        assert_eq!(deliver_and_drain(&mut reorderer, seq(10)), [10, 11, 12, 13].map(seq));
        assert!(reorderer.next_packet(seq(13)).is_none());
    }

    #[test]
    fn test_draining_stops_at_next_gap() {
        let mut reorderer = Reorderer::new();
        add(&mut reorderer, seq(11), seq(10));
        add(&mut reorderer, seq(13), seq(10));
        add(&mut reorderer, seq(14), seq(10));

        assert_eq!(deliver_and_drain(&mut reorderer, seq(10)), [10, 11].map(seq));
        assert_eq!(deliver_and_drain(&mut reorderer, seq(12)), [12, 13, 14].map(seq));
    }

    #[test]
    fn test_duplicate_packet_is_delivered_once() {
        let mut reorderer = Reorderer::new();
        add(&mut reorderer, seq(11), seq(10));
        add(&mut reorderer, seq(11), seq(10));

        assert_eq!(deliver_and_drain(&mut reorderer, seq(10)), [10, 11].map(seq));
        assert!(reorderer.next_packet(seq(11)).is_none());
    }

    #[test]
    fn test_reordering_across_seq_wraparound() {
        let mut reorderer = Reorderer::new();
        let expected = seq(u16::MAX - 2);
        for seq_nr in [u16::MAX - 1, u16::MAX, 0, 1, 2] {
            add(&mut reorderer, seq(seq_nr), expected);
        }

        assert_eq!(
            deliver_and_drain(&mut reorderer, expected),
            [u16::MAX - 2, u16::MAX - 1, u16::MAX, 0, 1, 2].map(seq)
        );
    }

    #[test]
    fn test_full_window_across_wraparound_has_no_collisions() {
        let mut reorderer = Reorderer::new();
        let expected = seq(u16::MAX - WINDOW / 2);
        for distance in 1..WINDOW {
            add(&mut reorderer, expected + seq(distance), expected);
        }

        let delivered = deliver_and_drain(&mut reorderer, expected);
        let expected_delivered: Vec<_> = (0..WINDOW).map(|d| expected + seq(d)).collect();
        assert_eq!(delivered, expected_delivered);
    }

    #[test]
    fn test_packet_at_window_edge_is_accepted() {
        let mut reorderer = Reorderer::new();
        let expected = seq(100);
        let last_in_window = expected + seq(WINDOW - 1);
        add(&mut reorderer, last_in_window, expected);

        assert!(reorderer.next_packet(last_in_window - seq(2)).is_none());
        let (header, _) = reorderer.next_packet(last_in_window - seq(1)).unwrap();
        assert_eq!(header.seq_nr, last_in_window);
    }

    #[test]
    fn test_packet_beyond_window_is_dropped() {
        let mut reorderer = Reorderer::new();
        let expected = seq(100);
        for distance in [WINDOW, WINDOW + 1, u16::MAX / 2] {
            let seq_nr = expected + seq(distance);
            add(&mut reorderer, seq_nr, expected);
            assert!(reorderer.next_packet(seq_nr - seq(1)).is_none(), "distance {distance}");
        }
    }

    #[test]
    fn test_packet_behind_expected_is_dropped() {
        let mut reorderer = Reorderer::new();
        let expected = seq(100);
        for seq_nr in [expected - seq(1), expected - seq(WINDOW)] {
            add(&mut reorderer, seq_nr, expected);
            assert!(reorderer.next_packet(seq_nr - seq(1)).is_none(), "seq_nr {seq_nr}");
        }
    }

    #[test]
    fn test_slots_are_reused_after_window_advances() {
        let mut reorderer = Reorderer::new();
        let mut expected = seq(0);
        // wrap around the sequence space twice, filling the whole window each time
        for _ in 0..(2 * (u16::MAX as u32 + 1) / (WINDOW as u32 - 1)) {
            let last_in_window = expected + seq(WINDOW - 1);
            for distance in 1..WINDOW {
                add(&mut reorderer, expected + seq(distance), expected);
            }
            let delivered = deliver_and_drain(&mut reorderer, expected);
            assert_eq!(delivered.last(), Some(&last_in_window));
            expected = last_in_window + seq(1);
        }
    }
}
