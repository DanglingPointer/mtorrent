use bytes::BytesMut;
use futures_util::io;
use local_async_utils::prelude::*;
use std::cmp::{Ordering, Reverse};
use std::collections::binary_heap::PeekMut;
use std::collections::{BinaryHeap, VecDeque};
use std::time::Duration;
use tokio::time::Instant;

pub struct BufferPool {
    default_buffer_capacity: usize,
    buffers: Vec<BytesMut>,
}

impl BufferPool {
    pub fn new(default_buffer_capacity: usize) -> Self {
        Self {
            default_buffer_capacity,
            buffers: vec![BytesMut::with_capacity(default_buffer_capacity)],
        }
    }

    pub fn reclaim_buffer(&mut self, mut buffer: BytesMut) {
        buffer.clear();
        self.buffers.push(buffer);
    }

    pub fn get_buffer(&mut self) -> BytesMut {
        self.buffers
            .pop()
            .unwrap_or_else(|| BytesMut::with_capacity(self.default_buffer_capacity))
    }
}

struct InFlight {
    retransmit_at: Instant,
    seq_nr: u16,
    packet: BytesMut,
}

impl PartialEq for InFlight {
    fn eq(&self, other: &Self) -> bool {
        self.retransmit_at == other.retransmit_at
    }
}
impl Eq for InFlight {}
impl Ord for InFlight {
    fn cmp(&self, other: &Self) -> Ordering {
        Reverse(&self.retransmit_at).cmp(&Reverse(&other.retransmit_at))
    }
}
impl PartialOrd for InFlight {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct Retransmitter {
    in_flight_packets: BinaryHeap<InFlight>,
    oldest_unacked_seq_nr: u16, // seq_nr of retransmit_attempts[0]
    retransmit_attempts: VecDeque<usize>,
}

impl Retransmitter {
    const INITIAL_RTO: Duration = millisec!(1500); // as per rfc8489 section-6.2.1
    const MAX_RETRANSMITS: usize = 2;

    pub fn new() -> Self {
        Self {
            in_flight_packets: BinaryHeap::new(),
            oldest_unacked_seq_nr: 0,
            retransmit_attempts: VecDeque::new(),
        }
    }

    pub fn total_bytes_in_flight(&self) -> usize {
        self.in_flight_packets.iter().fold(0, |total, entry| total + entry.packet.len())
    }

    pub fn next_retransmit_time(&self) -> Option<Instant> {
        self.in_flight_packets.peek().map(|entry| entry.retransmit_at)
    }

    pub fn next_packet_to_retransmit(&mut self) -> Option<(BytesMut, u16)> {
        if let Some(entry) = self.in_flight_packets.peek_mut()
            && entry.retransmit_at <= Instant::now()
        {
            let entry = PeekMut::pop(entry);
            Some((entry.packet, entry.seq_nr))
        } else {
            None
        }
    }

    pub fn add_packet(&mut self, packet: BytesMut, seq_nr: u16) -> io::Result<()> {
        let retransmits_made = if self.retransmit_attempts.is_empty() {
            self.oldest_unacked_seq_nr = seq_nr;
            self.retransmit_attempts.push_back(0);
            0
        } else {
            let oldest_unacked_seq_nr = self.oldest_unacked_seq_nr;
            let latest_unacked_seq_nr =
                oldest_unacked_seq_nr + self.retransmit_attempts.len() as u16 - 1;
            if seq_nr < self.oldest_unacked_seq_nr {
                return Err(io::Error::other("acked packet was re-sent, should never happen"));
            }
            if seq_nr <= latest_unacked_seq_nr {
                let index = seq_nr - self.oldest_unacked_seq_nr;
                self.retransmit_attempts[index as usize] += 1;
                self.retransmit_attempts[index as usize]
            } else if seq_nr == latest_unacked_seq_nr + 1 {
                self.retransmit_attempts.push_back(0);
                0
            } else {
                return Err(io::Error::other(format!(
                    "jump in tx seq nr (expected {}, got {seq_nr})",
                    latest_unacked_seq_nr + 1
                )));
            }
        };
        if retransmits_made < Self::MAX_RETRANSMITS {
            let next_rto = Self::INITIAL_RTO * (2 << retransmits_made) as u32;
            self.in_flight_packets.push(InFlight {
                retransmit_at: Instant::now() + next_rto,
                seq_nr,
                packet,
            });
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::TimedOut, "reached max retransmits"))
        }
    }

    pub fn discard_acked_packets(
        &mut self,
        last_acked_seq_nr: u16,
        buffer_pool: &mut BufferPool,
    ) -> usize {
        let packets_to_discard =
            (last_acked_seq_nr + 1).saturating_sub(self.oldest_unacked_seq_nr) as usize;
        if packets_to_discard > 0 {
            self.retransmit_attempts.drain(..packets_to_discard);
            self.oldest_unacked_seq_nr += packets_to_discard as u16;
            // self.in_flight_packets.retain(|in_flight| in_flight.seq_nr > last_acked_seq_nr);
            for _ in 0..packets_to_discard {
                let entry = self.in_flight_packets.pop().unwrap_or_else(|| unreachable!());
                assert!(entry.seq_nr <= last_acked_seq_nr);
                buffer_pool.reclaim_buffer(entry.packet);
            }
        }
        packets_to_discard
    }
}
