use super::seq::Seq;
use bytes::Bytes;
use futures_util::Stream;
use local_async_utils::prelude::*;
use std::cmp;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::{Instant, Sleep, sleep_until};

struct InFlight {
    sent_at: Instant,
    sent_times: usize,
    packet: Bytes,
    seq_nr: Seq,
    /// Considered lost after a timeout and waiting to be retransmitted. Not counted as in flight.
    need_resend: bool,
}

#[derive(Default)]
struct Rtt {
    rtt: u128,
    rtt_var: u128,
}

/// Keeps track of sent packets and decides when to retransmit them, mirroring libutp:
/// - on timeout, all packets are considered lost and only the oldest one is retransmitted
///   immediately (yielded by the `Stream` impl), the rest are retransmitted when the window allows
///   (see [`Retransmitter::resend_lost`]);
/// - after a timeout, each incoming ack that leaves the oldest unacked packet at
///   `fast_resend_seq_nr` triggers a retransmit of that packet (see
///   [`Retransmitter::process_ack`]);
/// - the retransmission timeout doubles on each timeout and is reset to the RTO on each ack;
/// - the congestion window is reset to a single packet on timeout. Since there is no delay-based
///   (LEDBAT) congestion control, it grows like in TCP: exponentially during slow start and by one
///   packet per RTT afterwards.
pub struct Retransmitter {
    timer: Pin<Box<Option<Sleep>>>,
    /// Sorted by seq_nr
    send_queue: VecDeque<InFlight>,
    rtt: Option<Rtt>,
    /// Retransmission timeout computed from the RTT estimate
    rto: Duration,
    /// Current retransmission timeout, `rto` with exponential backoff applied
    timeout: Duration,
    /// Congestion window in bytes
    cwnd: usize,
    /// Slow start threshold in bytes
    ssthresh: usize,
    fast_timeout: bool,
    /// Lowest seq_nr that may be fast-resent, ensures each packet is fast-resent only once
    fast_resend_seq_nr: Option<Seq>,
}

impl Stream for Retransmitter {
    type Item = Bytes;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.send_queue.is_empty() {
            self.as_mut().timer.set(None);
            Poll::Ready(None)
        } else {
            self.get_mut().poll_next_retransmit(cx).map(Some)
        }
    }
}

impl Retransmitter {
    pub const PACKET_SIZE: usize = 1472; // ethernet MTU
    const INITIAL_RTO: Duration = sec!(1);
    const MIN_RTO: Duration = sec!(1);
    const MAX_TIMEOUT: Duration = sec!(60);

    pub fn new() -> Self {
        Self {
            timer: Box::pin(None),
            send_queue: VecDeque::new(),
            rtt: None,
            rto: Self::INITIAL_RTO,
            timeout: Self::INITIAL_RTO,
            cwnd: Self::PACKET_SIZE,
            ssthresh: usize::MAX,
            fast_timeout: false,
            fast_resend_seq_nr: None,
        }
    }

    /// Bytes sent but not yet acked, excluding packets that are considered lost.
    pub fn total_bytes_in_flight(&self) -> usize {
        self.send_queue
            .iter()
            .filter(|entry| !entry.need_resend)
            .fold(0, |total, entry| total + entry.packet.len())
    }

    /// Whether any packets are waiting for [`Retransmitter::resend_lost`].
    pub fn has_lost_packets(&self) -> bool {
        self.send_queue.iter().any(|entry| entry.need_resend)
    }

    /// Whether a packet of `len` bytes may be sent now without exceeding the congestion window or
    /// the receive window of the peer.
    pub fn can_send(&self, len: usize, peer_window: usize) -> bool {
        let in_flight = self.total_bytes_in_flight();
        in_flight == 0 || in_flight + len <= cmp::min(self.cwnd, peer_window)
    }

    fn poll_next_retransmit(&mut self, cx: &mut Context<'_>) -> Poll<Bytes> {
        let timed_out =
            self.timer.as_mut().as_pin_mut().is_some_and(|timer| timer.poll(cx).is_ready());

        if timed_out && !self.send_queue.is_empty() {
            self.timeout = cmp::min(self.timeout * 2, Self::MAX_TIMEOUT);
            self.arm_timer();

            // reset the congestion window to fit one packet, to start over again
            self.ssthresh = cmp::max(self.cwnd / 2, 2 * Self::PACKET_SIZE);
            self.cwnd = Self::PACKET_SIZE;

            // every packet should be considered lost
            for entry in &mut self.send_queue {
                entry.need_resend = true;
            }
            self.fast_timeout = true;
            Poll::Ready(self.retransmit(0))
        } else {
            Poll::Pending
        }
    }

    /// Retransmit the oldest lost packet if it fits into the window.
    pub fn resend_lost(&mut self, peer_window: usize) -> Option<Bytes> {
        let index = self.send_queue.iter().position(|entry| entry.need_resend)?;
        if self.can_send(self.send_queue[index].packet.len(), peer_window) {
            Some(self.retransmit(index))
        } else {
            None
        }
    }

    fn retransmit(&mut self, index: usize) -> Bytes {
        let entry = &mut self.send_queue[index];
        entry.sent_at = Instant::now();
        entry.sent_times += 1;
        entry.need_resend = false;
        entry.packet.clone()
    }

    pub fn add_new_packet(&mut self, packet: Bytes, seq_nr: Seq) {
        assert!(self.send_queue.back().is_none_or(|last| last.seq_nr < seq_nr));

        if self.send_queue.is_empty() {
            self.timeout = self.rto;
            self.arm_timer();
        }
        self.fast_resend_seq_nr.get_or_insert(seq_nr);
        self.send_queue.push_back(InFlight {
            sent_at: Instant::now(),
            sent_times: 1,
            packet,
            seq_nr,
            need_resend: false,
        });
    }

    /// Process an incoming ack (including duplicate ones). Returns a packet that needs to be
    /// retransmitted immediately, if any.
    pub fn process_ack(&mut self, acked_seq_nr: Seq) -> Option<Bytes> {
        if let Some(InFlight {
            sent_at,
            sent_times,
            ..
        }) = self.send_queue.iter().find(|entry| entry.seq_nr == acked_seq_nr)
        {
            if *sent_times == 1 {
                // update RTT and RTO
                let packet_rtt = sent_at.elapsed().as_millis();
                match &mut self.rtt {
                    Some(Rtt { rtt, rtt_var }) => {
                        let abs_delta = rtt.abs_diff(packet_rtt);
                        *rtt_var = (3 * *rtt_var + abs_delta) / 4;
                        *rtt = (7 * *rtt + packet_rtt) / 8;
                    }
                    None => {
                        self.rtt = Some(Rtt {
                            rtt: packet_rtt,
                            rtt_var: packet_rtt / 2,
                        });
                    }
                }

                let estimate = self.rtt.as_ref().unwrap();
                self.rto = cmp::max(
                    millisec!((estimate.rtt + estimate.rtt_var * 4) as u64),
                    Self::MIN_RTO,
                );
            }

            // consider the window to be limiting the sending rate if more than half of it is used
            let cwnd_limited = self.total_bytes_in_flight() * 2 > self.cwnd;
            let acked_bytes: usize = self
                .send_queue
                .iter()
                .take_while(|entry| entry.seq_nr <= acked_seq_nr)
                .map(|entry| entry.packet.len())
                .sum();
            self.send_queue.retain(|in_flight| in_flight.seq_nr > acked_seq_nr);

            // don't grow the window when it's not what limits the sending rate
            if cwnd_limited {
                self.grow_cwnd(acked_bytes);
            }

            self.timeout = self.rto;
            if self.send_queue.is_empty() {
                self.timer.set(None);
            } else {
                self.arm_timer();
            }
        }

        let next_seq_nr = acked_seq_nr + Seq::ONE;
        if self.fast_resend_seq_nr.is_none_or(|seq_nr| seq_nr < next_seq_nr) {
            self.fast_resend_seq_nr = Some(next_seq_nr);
        }

        if self.fast_timeout {
            let oldest_unacked = self.send_queue.front().map_or(next_seq_nr, |entry| entry.seq_nr);
            if Some(oldest_unacked) != self.fast_resend_seq_nr {
                // the packet that timed out has already been resent, leave fast-timeout mode
                self.fast_timeout = false;
            } else if !self.send_queue.is_empty() {
                // resend the oldest packet and increment fast_resend_seq_nr to not allow another
                // fast resend on it again
                self.fast_resend_seq_nr = Some(oldest_unacked + Seq::ONE);
                return Some(self.retransmit(0));
            }
        }
        None
    }

    fn grow_cwnd(&mut self, acked_bytes: usize) {
        if self.cwnd < self.ssthresh {
            // slow start
            self.cwnd = cmp::min(self.cwnd + acked_bytes, self.ssthresh);
        } else {
            // congestion avoidance: one packet per RTT
            self.cwnd += cmp::max(Self::PACKET_SIZE * acked_bytes / self.cwnd, 1);
        }
    }

    fn arm_timer(&mut self) {
        let deadline = Instant::now() + self.timeout;
        match self.timer.as_mut().as_pin_mut() {
            Some(timer) => timer.reset(deadline),
            None => self.timer.set(Some(sleep_until(deadline))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::seq::seq;
    use super::*;
    use futures_util::{FutureExt, StreamExt};
    use tokio::time;

    const P1: Bytes = Bytes::from_static(b"packet1");
    const P2: Bytes = Bytes::from_static(b"packet2");
    const P3: Bytes = Bytes::from_static(b"packet3");
    const P4: Bytes = Bytes::from_static(b"packet4");

    fn retransmitter_with_packets(packets: &[Bytes]) -> Retransmitter {
        let mut retransmitter = Retransmitter::new();
        for (packet, seq_nr) in packets.iter().zip(1..) {
            retransmitter.add_new_packet(packet.clone(), seq(seq_nr));
        }
        retransmitter
    }

    #[tokio::test(start_paused = true)]
    async fn test_timeout_resends_only_oldest_packet() {
        let send_time = Instant::now();
        let mut retransmitter = retransmitter_with_packets(&[P1, P2, P3]);

        for expected_elapsed in [sec!(1), sec!(3)] {
            let retransmit = retransmitter.next().await.unwrap();
            assert_eq!(retransmit, P1);
            assert_eq!(send_time.elapsed(), expected_elapsed);
            assert!(retransmitter.next().now_or_never().is_none());

            // the other packets are considered lost and don't count as in flight
            assert!(retransmitter.has_lost_packets());
            assert_eq!(retransmitter.total_bytes_in_flight(), P1.len());
        }

        assert_eq!(retransmitter.process_ack(seq(3)), None);
        assert!(retransmitter.send_queue.is_empty());
        assert!(!retransmitter.has_lost_packets());
        assert!(retransmitter.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn test_lost_packets_are_resent_when_window_allows() {
        let mut retransmitter = retransmitter_with_packets(&[P1, P2, P3]);
        assert_eq!(retransmitter.next().await.unwrap(), P1);

        assert_eq!(retransmitter.resend_lost(P1.len()), None);
        assert_eq!(retransmitter.resend_lost(P1.len() + P2.len()), Some(P2));
        assert_eq!(retransmitter.resend_lost(P1.len() + P2.len()), None);
        assert_eq!(retransmitter.resend_lost(usize::MAX), Some(P3));
        assert_eq!(retransmitter.resend_lost(usize::MAX), None);
        assert!(!retransmitter.has_lost_packets());
        assert_eq!(retransmitter.total_bytes_in_flight(), P1.len() + P2.len() + P3.len());
    }

    #[tokio::test(start_paused = true)]
    async fn test_lost_packet_is_resent_when_nothing_in_flight() {
        let mut retransmitter = retransmitter_with_packets(&[P1, P2]);
        assert_eq!(retransmitter.next().await.unwrap(), P1);

        // exit fast-timeout mode with a duplicate ack
        assert_eq!(retransmitter.process_ack(seq(0)), Some(P1));
        assert_eq!(retransmitter.process_ack(seq(0)), None);
        assert_eq!(retransmitter.process_ack(seq(1)), None);

        assert_eq!(retransmitter.total_bytes_in_flight(), 0);
        assert_eq!(retransmitter.resend_lost(0), Some(P2));
    }

    #[tokio::test(start_paused = true)]
    async fn test_fast_timeout_resends_next_oldest_packet_on_each_ack() {
        let mut retransmitter = retransmitter_with_packets(&[P1, P2, P3]);
        assert_eq!(retransmitter.next().await.unwrap(), P1);

        assert_eq!(retransmitter.process_ack(seq(1)), Some(P2));
        assert_eq!(retransmitter.total_bytes_in_flight(), P2.len());
        assert!(retransmitter.has_lost_packets());

        assert_eq!(retransmitter.process_ack(seq(2)), Some(P3));
        assert!(!retransmitter.has_lost_packets());

        assert_eq!(retransmitter.process_ack(seq(3)), None);
        assert!(retransmitter.send_queue.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn test_fast_timeout_ends_on_duplicate_ack() {
        let mut retransmitter = retransmitter_with_packets(&[P1, P2, P3, P4]);
        assert_eq!(retransmitter.next().await.unwrap(), P1);

        assert_eq!(retransmitter.process_ack(seq(1)), Some(P2));
        // P2 has already been fast-resent
        assert_eq!(retransmitter.process_ack(seq(1)), None);
        assert_eq!(retransmitter.process_ack(seq(2)), None);

        // remaining lost packets are left for resend_lost()
        assert_eq!(retransmitter.resend_lost(usize::MAX), Some(P3));
        assert_eq!(retransmitter.resend_lost(usize::MAX), Some(P4));
        assert_eq!(retransmitter.resend_lost(usize::MAX), None);
    }

    #[tokio::test(start_paused = true)]
    async fn test_no_fast_resend_without_timeout() {
        let mut retransmitter = retransmitter_with_packets(&[P1, P2, P3]);

        assert_eq!(retransmitter.process_ack(seq(1)), None);
        assert_eq!(retransmitter.process_ack(seq(1)), None);
        assert_eq!(retransmitter.process_ack(seq(2)), None);
        assert!(!retransmitter.has_lost_packets());
        assert_eq!(retransmitter.total_bytes_in_flight(), P3.len());
    }

    #[tokio::test(start_paused = true)]
    async fn test_timeout_doubles_and_is_reset_on_ack() {
        let mut retransmitter = retransmitter_with_packets(&[P1, P2]);

        let mut last_retransmit = Instant::now();
        for expected_timeout in [sec!(1), sec!(2), sec!(4), sec!(8)] {
            assert_eq!(retransmitter.next().await.unwrap(), P1);
            assert_eq!(last_retransmit.elapsed(), expected_timeout);
            last_retransmit = Instant::now();
        }

        // fast resend of P2, and the timer is restarted with the initial RTO
        assert_eq!(retransmitter.process_ack(seq(1)), Some(P2));
        let ack_time = Instant::now();
        assert_eq!(retransmitter.next().await.unwrap(), P2);
        assert_eq!(ack_time.elapsed(), Retransmitter::INITIAL_RTO);
    }

    #[tokio::test(start_paused = true)]
    async fn test_timeout_is_capped() {
        let mut retransmitter = retransmitter_with_packets(&[P1]);
        for _ in 0..10 {
            assert_eq!(retransmitter.next().await.unwrap(), P1);
        }
        assert_eq!(retransmitter.timeout, Retransmitter::MAX_TIMEOUT);
    }

    fn full_packet(fill: u8) -> Bytes {
        Bytes::from(vec![fill; Retransmitter::PACKET_SIZE])
    }

    #[tokio::test(start_paused = true)]
    async fn test_cwnd_slow_start_reset_on_timeout_and_congestion_avoidance() {
        const PS: usize = Retransmitter::PACKET_SIZE;
        let mut retransmitter = Retransmitter::new();

        // slow start from a single packet
        retransmitter.add_new_packet(full_packet(1), seq(1));
        assert!(!retransmitter.can_send(PS, usize::MAX));
        assert_eq!(retransmitter.process_ack(seq(1)), None);
        assert_eq!(retransmitter.cwnd, 2 * PS);

        retransmitter.add_new_packet(full_packet(2), seq(2));
        retransmitter.add_new_packet(full_packet(3), seq(3));
        assert!(!retransmitter.can_send(PS, usize::MAX));
        assert_eq!(retransmitter.process_ack(seq(3)), None);
        assert_eq!(retransmitter.cwnd, 4 * PS);

        // timeout resets the window to a single packet
        for i in 4..=6 {
            retransmitter.add_new_packet(full_packet(i), seq(i as u16));
        }
        assert_eq!(retransmitter.next().await.unwrap(), full_packet(4));
        assert_eq!(retransmitter.cwnd, PS);
        assert_eq!(retransmitter.ssthresh, 2 * PS);
        assert_eq!(retransmitter.resend_lost(usize::MAX), None);

        // slow start up to ssthresh
        assert_eq!(retransmitter.process_ack(seq(4)), Some(full_packet(5)));
        assert_eq!(retransmitter.cwnd, 2 * PS);
        assert_eq!(retransmitter.resend_lost(usize::MAX), Some(full_packet(6)));
        assert_eq!(retransmitter.resend_lost(usize::MAX), None);

        // congestion avoidance: one packet per window
        assert_eq!(retransmitter.process_ack(seq(6)), None);
        assert_eq!(retransmitter.cwnd, 3 * PS);
    }

    #[tokio::test(start_paused = true)]
    async fn test_cwnd_doesnt_grow_when_not_limiting() {
        let mut retransmitter = retransmitter_with_packets(&[P1, P2]);
        retransmitter.process_ack(seq(2));
        assert_eq!(retransmitter.cwnd, Retransmitter::PACKET_SIZE);
    }

    #[tokio::test(start_paused = true)]
    async fn test_peer_window_limits_sending() {
        let mut retransmitter = Retransmitter::new();
        retransmitter.cwnd = 10 * Retransmitter::PACKET_SIZE;
        assert!(retransmitter.can_send(P1.len(), 0));
        retransmitter.add_new_packet(P1, seq(1));
        assert!(retransmitter.can_send(P2.len(), P1.len() + P2.len()));
        assert!(!retransmitter.can_send(P2.len(), P1.len() + P2.len() - 1));
    }

    #[tokio::test(start_paused = true)]
    async fn test_rto_decreases_after_latency_improves() {
        let mut retransmitter = Retransmitter::new();

        retransmitter.add_new_packet(Bytes::from_static(b"packet"), seq(1));
        time::sleep(millisec!(1000)).await;
        retransmitter.process_ack(seq(1));
        assert_eq!(retransmitter.rtt.as_ref().unwrap().rtt, 1000);
        assert_eq!(retransmitter.rtt.as_ref().unwrap().rtt_var, 500);
        let slow_timeout = retransmitter.timeout;

        for seq_nr in 2..=9 {
            retransmitter.add_new_packet(Bytes::from_static(b"packet"), seq(seq_nr));
            time::sleep(millisec!(100)).await;
            retransmitter.process_ack(seq(seq_nr));
        }

        assert!(retransmitter.rtt.as_ref().unwrap().rtt < 1000);
        assert!(retransmitter.rtt.as_ref().unwrap().rtt_var < 500);
        assert!(retransmitter.timeout < slow_timeout);
    }
}
