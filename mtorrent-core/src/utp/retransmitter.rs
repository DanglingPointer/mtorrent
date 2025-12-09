use bytes::Bytes;
use local_async_utils::prelude::*;
use pin_project_lite::pin_project;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::{Instant, Sleep, sleep_until};

#[derive(Debug)]
struct InFlight {
    retransmit_at: Instant,
    sent_times: usize,
    packet: Bytes,
    seq_nr: u16,
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

pin_project! {
    #[derive(Debug)]
    struct Inner {
        in_flight_packets: BinaryHeap<InFlight>,
        #[pin]
        timer: Option<Sleep>,
    }
}

#[derive(Debug)]
pub struct Retransmitter(Pin<Box<Inner>>);

impl Retransmitter {
    const INITIAL_RTO: Duration = millisec!(1500); // as per rfc8489 section-6.2.1
    const MAX_RETRANSMITS: usize = 2;

    pub fn new() -> Self {
        Self(Box::pin(Inner {
            in_flight_packets: BinaryHeap::new(),
            timer: None,
        }))
    }

    pub fn total_bytes_in_flight(&self) -> usize {
        self.0
            .in_flight_packets
            .iter()
            .fold(0, |total, entry| total + entry.packet.len())
    }

    pub fn poll_next_retry(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Bytes>> {
        let mut this = self.0.as_mut().project();

        if let Some(timer) = this.timer.as_mut().as_pin_mut()
            && timer.poll(cx).is_ready()
        {
            if let Some(mut in_flight) = this.in_flight_packets.pop() {
                if in_flight.sent_times == Self::MAX_RETRANSMITS + 1 {
                    return Poll::Ready(Err(io::ErrorKind::TimedOut.into()));
                }

                let packet = in_flight.packet.clone(); // shallow copy

                let next_rto = Self::INITIAL_RTO * (2 << in_flight.sent_times) as u32;
                in_flight.retransmit_at = Instant::now() + next_rto;
                in_flight.sent_times += 1;
                this.in_flight_packets.push(in_flight);
                self.update_timer();

                return Poll::Ready(Ok(packet));
            } else {
                this.timer.set(None);
            }
        }
        Poll::Pending
    }

    pub fn add_new_packet(&mut self, packet: Bytes, seq_nr: u16) -> io::Result<()> {
        if self.0.in_flight_packets.iter().any(|in_flight| in_flight.seq_nr == seq_nr) {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        let in_flight = InFlight {
            retransmit_at: Instant::now() + Self::INITIAL_RTO,
            sent_times: 1,
            packet,
            seq_nr,
        };

        self.0.as_mut().project().in_flight_packets.push(in_flight);
        self.update_timer();

        Ok(())
    }

    pub fn discard_acked_packets(&mut self, last_acked_seq_nr: u16) {
        let init_len = self.0.in_flight_packets.len();
        self.0
            .as_mut()
            .project()
            .in_flight_packets
            .retain(|in_flight| in_flight.seq_nr > last_acked_seq_nr);
        if init_len != self.0.in_flight_packets.len() {
            self.update_timer();
        }
    }

    fn update_timer(&mut self) {
        let mut this = self.0.as_mut().project();
        if let Some(next_retransmit_at) =
            this.in_flight_packets.peek().map(|entry| entry.retransmit_at)
        {
            if let Some(timer) = this.timer.as_mut().as_pin_mut()
                && timer.deadline() != next_retransmit_at
            {
                timer.reset(next_retransmit_at);
            } else {
                this.timer.set(Some(sleep_until(next_retransmit_at)));
            }
        } else if this.timer.is_some() {
            this.timer.set(None);
        }
    }
}
