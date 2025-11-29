use super::seq::Seq;
use bytes::Bytes;
use local_async_utils::prelude::*;
use std::future::poll_fn;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use std::{cmp, collections::VecDeque};
use tokio::time::{Instant, Sleep, sleep_until};

struct InFlight {
    sent_at: Instant,
    sent_times: usize,
    packet: Bytes,
    seq_nr: Seq,
}

#[derive(Default)]
struct Rtt {
    rtt: u128,
    rtt_var: u128,
}

pub struct Retransmitter {
    timer: Pin<Box<Option<Sleep>>>,
    send_queue: VecDeque<InFlight>,
    rtt: Option<Rtt>,
    timeout: Duration,
    duplicate_ack_count: usize,
    packet_size: usize,
}

impl Retransmitter {
    const MAX_PACKET_SIZE: usize = 1024 * 8;
    const MIN_PACKET_SIZE: usize = 1024;
    const INITIAL_RTO: Duration = sec!(1);

    pub fn new() -> Self {
        Self {
            timer: Box::pin(None),
            send_queue: VecDeque::new(),
            rtt: None,
            timeout: Self::INITIAL_RTO,
            duplicate_ack_count: 0,
            packet_size: Self::MAX_PACKET_SIZE,
        }
    }

    pub fn packet_size(&self) -> usize {
        self.packet_size
    }

    pub fn total_bytes_in_flight(&self) -> usize {
        self.send_queue.iter().fold(0, |total, entry| total + entry.packet.len())
    }

    fn poll_next_retransmit(&mut self, cx: &mut Context<'_>) -> Poll<Bytes> {
        let packet_lost =
            self.timer.as_mut().as_pin_mut().is_some_and(|timer| timer.poll(cx).is_ready())
                || self.duplicate_ack_count >= 2;

        if packet_lost && let Some(mut in_flight) = self.send_queue.pop_front() {
            let packet = in_flight.packet.clone();

            in_flight.sent_at = Instant::now();
            in_flight.sent_times += 1;
            self.send_queue.push_back(in_flight);

            self.duplicate_ack_count = 0;
            self.update_timer();

            self.packet_size = (self.packet_size / 2).max(Self::MIN_PACKET_SIZE);
            // TODO:
            // max_window = 150;

            Poll::Ready(packet)
        } else {
            Poll::Pending
        }
    }

    pub async fn next_retransmit(&mut self) -> Bytes {
        poll_fn(|cx| self.poll_next_retransmit(cx)).await
    }

    pub fn add_new_packet(&mut self, packet: Bytes, seq_nr: Seq) {
        assert!(!self.send_queue.iter().any(|in_flight| in_flight.seq_nr == seq_nr));

        self.send_queue.push_back(InFlight {
            sent_at: Instant::now(),
            sent_times: 1,
            packet,
            seq_nr,
        });
        self.update_timer();
    }

    pub fn process_ack(&mut self, acked_seq_nr: Seq) {
        if let Some(InFlight {
            sent_at,
            sent_times,
            ..
        }) = self.send_queue.iter().find(|entry| entry.seq_nr == acked_seq_nr)
        {
            self.duplicate_ack_count = 0;

            if *sent_times == 1 {
                // update RTT and RTO
                let Rtt { rtt, rtt_var } = self.rtt.get_or_insert_default();
                let packet_rtt = sent_at.elapsed().as_millis();
                let abs_delta = rtt.abs_diff(packet_rtt);
                *rtt_var += abs_delta.saturating_sub(*rtt_var) / 4;
                *rtt += packet_rtt.saturating_sub(*rtt) / 8;

                self.timeout = millisec!(cmp::max(*rtt + *rtt_var * 4, 500) as u64);
            }

            self.send_queue.retain(|in_flight| in_flight.seq_nr > acked_seq_nr);
            self.update_timer();

            if self.send_queue.is_empty() {
                self.packet_size = (self.packet_size * 2).min(Self::MAX_PACKET_SIZE);
            }
        } else if !self.send_queue.is_empty() {
            self.duplicate_ack_count += 1;
            if self.duplicate_ack_count == 2 {
                // TODO:
                // max_window /= 2;
                let tmp_timeout = mem::replace(&mut self.timeout, Duration::ZERO);
                self.update_timer();
                self.timeout = tmp_timeout;
            }
        }
    }

    fn update_timer(&mut self) {
        if let Some(oldest_send_time) = self.send_queue.front().map(|in_flight| in_flight.sent_at) {
            let next_timeout = oldest_send_time + self.timeout;
            if let Some(timer) = self.timer.as_mut().as_pin_mut()
                && timer.deadline() > next_timeout
            {
                timer.reset(next_timeout);
            } else {
                self.timer.set(Some(sleep_until(next_timeout)));
            }
        } else if self.timer.is_some() {
            self.timer.set(None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::seq::seq;
    use super::*;
    use futures_util::FutureExt;
    use tokio::time;

    #[tokio::test(start_paused = true)]
    async fn test_regular_retransmit() {
        let mut retransmitter = Retransmitter::new();

        let send_time = Instant::now();
        retransmitter.add_new_packet(Bytes::from_static(b"packet1"), seq(1));
        retransmitter.add_new_packet(Bytes::from_static(b"packet2"), seq(2));
        assert_eq!(retransmitter.send_queue.len(), 2);

        for i in 1..=2 {
            let retransmit = retransmitter.next_retransmit().await;
            assert_eq!(&retransmit[..], b"packet1");
            assert_eq!(send_time.elapsed(), Retransmitter::INITIAL_RTO * i);

            let retransmit = retransmitter.next_retransmit().await;
            assert_eq!(&retransmit[..], b"packet2");
            assert_eq!(send_time.elapsed(), Retransmitter::INITIAL_RTO * i);
        }

        // Acknowledge packet 1
        retransmitter.process_ack(seq(1));
        assert_eq!(retransmitter.send_queue.len(), 1);

        for i in 3..=4 {
            let retransmit = retransmitter.next_retransmit().await;
            assert_eq!(&retransmit[..], b"packet2");
            assert_eq!(send_time.elapsed(), Retransmitter::INITIAL_RTO * i);
        }

        retransmitter.process_ack(seq(2));
        assert_eq!(retransmitter.send_queue.len(), 0);
        time::timeout(Duration::MAX, retransmitter.next_retransmit()).await.unwrap_err();
    }

    #[tokio::test(start_paused = true)]
    async fn test_fast_retransmit() {
        let mut retransmitter = Retransmitter::new();

        retransmitter.add_new_packet(Bytes::from_static(b"packet0"), seq(10));
        retransmitter.process_ack(seq(10));
        time::timeout(Duration::MAX, retransmitter.next_retransmit()).await.unwrap_err();

        let send_time = Instant::now();
        retransmitter.add_new_packet(Bytes::from_static(b"packet1"), seq(11));
        retransmitter.add_new_packet(Bytes::from_static(b"packet2"), seq(12));
        assert_eq!(retransmitter.send_queue.len(), 2);

        time::sleep(millisec!(10)).await;
        retransmitter.process_ack(seq(10));
        time::sleep(millisec!(10)).await;
        retransmitter.process_ack(seq(10));

        let retransmit = retransmitter.next_retransmit().now_or_never().unwrap();
        assert_eq!(&retransmit[..], b"packet1");
        assert!(retransmitter.next_retransmit().now_or_never().is_none());

        for i in 1..=2 {
            let retransmit = retransmitter.next_retransmit().await;
            assert_eq!(&retransmit[..], b"packet2");
            assert_eq!(send_time.elapsed(), Retransmitter::INITIAL_RTO / 2 * i);

            let retransmit = retransmitter.next_retransmit().await;
            assert_eq!(&retransmit[..], b"packet1");
            assert_eq!(send_time.elapsed(), Retransmitter::INITIAL_RTO / 2 * i + millisec!(20));
        }
    }
}
