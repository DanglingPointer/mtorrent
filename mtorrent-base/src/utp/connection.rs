use super::protocol::{
    ConnectionState, Header, TypeVer, ValidationError, dbg_header_extensions, skip_extensions,
};
use super::retransmitter::Retransmitter;
use super::seq::{Seq, seq};
use bytes::buf::Limit;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_util::{FutureExt, StreamExt};
use local_async_utils::prelude::*;
use log::log_enabled;
use mtorrent_utils::local_watch;
use std::hash::BuildHasher;
use std::net::SocketAddr;
use std::time::Duration;
use std::{io, mem};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;
use tokio::{select, time};

#[derive(Default, Debug)]
struct EgressStats {
    retransmit_count: u64,
    data_count: u64,
    ack_count: u64,
}

#[derive(Default, Debug)]
struct IngressStats {
    in_order_count: u64,
    duplicate_count: u64,
    seq_jump_count: u64,
}

struct EgressProcessor {
    state: LocalShared<ConnectionState>,

    ack_received_notifier: local_watch::Receiver<Seq>,
    ack_required_notifier: local_condvar::Receiver,

    receiver: local_pipe::ReadEnd,
    sender: local_bounded::Sender<Bytes>,
}

fn init_send_buf(packet_size: usize) -> Limit<BytesMut> {
    let mut b = BytesMut::with_capacity(packet_size).limit(packet_size);
    // make space for header
    unsafe { b.advance_mut(Header::MIN_SIZE) };
    b
}

fn finalize_send_buf(buf: Limit<BytesMut>, header: &Header) -> Bytes {
    let mut inner = buf.into_inner();
    // write header at the head
    _ = header.encode_to(&mut inner.as_mut());
    inner.freeze()
}

fn resize_send_buf(buf: &mut Limit<BytesMut>, new_packet_size: usize) {
    let filled_bytes = buf.get_ref().len();
    let max_remaining = new_packet_size.saturating_sub(filled_bytes);
    buf.set_limit(max_remaining);
}

impl EgressProcessor {
    async fn run(&mut self, peer_addr: &SocketAddr, stats: &mut EgressStats) -> io::Result<()> {
        define_with!(self.state);

        let mut retransmitter = Retransmitter::new();
        let mut send_buffer = init_send_buf(retransmitter.packet_size());

        macro_rules! tx_allowed {
            () => {{
                retransmitter.total_bytes_in_flight() == 0
                    || retransmitter.total_bytes_in_flight() + send_buffer.get_ref().len()
                        <= with!(|state| state.max_window_size())
            }};
        }

        macro_rules! send_data_if_ready {
            () => {{
                if send_buffer.get_ref().len() > Header::MIN_SIZE && tx_allowed!() {
                    let buf =
                        mem::replace(&mut send_buffer, init_send_buf(retransmitter.packet_size()));
                    let header = with!(|state| state.generate_header(TypeVer::Data));
                    let packet = finalize_send_buf(buf, &header);
                    self.sender.send(packet.clone()).await?;
                    stats.data_count += 1;
                    if log_enabled!(log::Level::Trace) {
                        log::trace!("TX-{peer_addr}: {header:?}");
                    }
                    retransmitter.add_new_packet(packet, header.seq_nr);
                    // clear pending ack notification if any
                    self.ack_received_notifier.wait_and_get().now_or_never();
                }
            }};
        }

        loop {
            select! {
                biased;
                ack = self.ack_received_notifier.wait_and_get() => {
                    let Some(ack) = ack else {
                        return Ok(()); // ingress processor exited
                    };
                    retransmitter.process_ack(ack);
                    if retransmitter.total_bytes_in_flight()  == 0 {
                        with!(|state| state.grow_local_window());
                    }
                    send_data_if_ready!();
                }
                Some(packet) = retransmitter.next() => {
                    self.sender.send(packet).await?;
                    stats.retransmit_count += 1;
                    if log_enabled!(log::Level::Trace) {
                        log::trace!("TX-{peer_addr}: <retransmit>");
                    }
                    // max packet size might've changed, update send_buffer
                    resize_send_buf(&mut send_buffer, retransmitter.packet_size());
                    with!(|state| state.shrink_local_window());
                }
                read_result = self.receiver.read_buf(&mut send_buffer), if tx_allowed!() => {
                    match read_result {
                        Err(e) => {
                            log::debug!("Egress processor for {peer_addr} exiting: pipe failure ({e})");
                            return Ok(());
                        }
                        Ok(0) if send_buffer.remaining_mut() > 0 => {
                            log::debug!("Egress processor for {peer_addr} exiting: pipe closed");
                            return Ok(());
                        }
                        Ok(_bytes_read) => {}
                    }
                    send_data_if_ready!();
                }
                ack_required = self.ack_required_notifier.wait_for_one() => {
                    if !ack_required {
                        return Ok(()); // ingress processor exited
                    }
                    let header = with!(|state| state.generate_header(TypeVer::State));
                    let mut buf = BytesMut::with_capacity(Header::MIN_SIZE);
                    header.encode_to(&mut buf)?;
                    self.sender.send(buf.freeze()).await?;
                    stats.ack_count += 1;
                    if log_enabled!(log::Level::Trace) {
                        log::trace!("TX-{peer_addr}: {header:?}");
                    }
                }
            }
        }
    }
}

struct IngressProcessor {
    state: LocalShared<ConnectionState>,

    ack_received_reporter: local_watch::Sender<Seq>,
    ack_required_reporter: local_condvar::Sender,

    sender: local_pipe::WriteEnd,
    receiver: local_bounded::Receiver<Bytes>,
}

impl IngressProcessor {
    async fn run(&mut self, peer_addr: &SocketAddr, stats: &mut IngressStats) -> io::Result<()> {
        define_with!(self.state);

        let mut last_received_ack = None;

        while let Some(mut packet) = self.receiver.next().await {
            let header = Header::decode_from(&mut packet)?;
            skip_extensions(&mut packet, &header)?;

            if log_enabled!(log::Level::Trace) {
                log::trace!(
                    "RX-{peer_addr}: {:?} payload_size={}",
                    dbg_header_extensions(&header, &packet),
                    packet.len()
                );
            }

            match with!(|state| state.validate_header(&header)) {
                Ok(()) => {
                    stats.in_order_count += 1;
                    if last_received_ack.is_none_or(|last| header.ack_nr > last) {
                        last_received_ack = Some(header.ack_nr);
                        self.ack_received_reporter.set_and_notify(header.ack_nr);
                    }
                    match header.type_ver {
                        TypeVer::State => {
                            with!(|state| state.process_header(&header));
                        }
                        TypeVer::Data => {
                            if let Err(e) = write_and_flush(&mut self.sender, &mut packet).await {
                                log::debug!(
                                    "Ingress processor for {peer_addr} exiting: pipe closed ({e})"
                                );
                                return Ok(());
                            }
                            with!(|state| state.process_header(&header));
                            self.ack_required_reporter.signal_one();
                        }
                        TypeVer::Fin => {
                            log::debug!("Ingress processor for {peer_addr} exiting: received FIN");
                            return Ok(());
                        }
                        TypeVer::Reset => {
                            return Err(io::Error::new(
                                io::ErrorKind::ConnectionReset,
                                "received RESET",
                            ));
                        }
                        TypeVer::Syn => {
                            return Err(io::Error::other("received unexpected SYN"));
                        }
                    }
                }
                Err(e) => match e {
                    e @ ValidationError::Invalid(_) => {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, e));
                    }
                    ValidationError::Duplicate => {
                        stats.duplicate_count += 1;
                        if log_enabled!(log::Level::Trace) {
                            log::trace!(
                                "Received duplicate {:?} packet from {peer_addr}, seq_nr={}, ack_nr={}",
                                header.type_ver,
                                header.seq_nr,
                                header.ack_nr
                            );
                        }
                        if header.type_ver == TypeVer::Data {
                            // our ack might've been lost, retransmit it
                            self.ack_required_reporter.signal_one();
                        }
                    }
                    e @ ValidationError::OutOfOrder { .. } => {
                        stats.seq_jump_count += 1;
                        if log_enabled!(log::Level::Trace) {
                            log::trace!("Received out-of-order packet from {peer_addr}: {e}");
                        }
                    }
                },
            }
        }

        Ok(())
    }
}

/// Send `packet` and wait for a reply that satisfies `is_reply`, retransmitting `packet` on
/// timeout. Packets that don't satisfy `is_reply` (e.g. late packets from a previous connection
/// with the same peer) are ignored. Never times out, so the caller is responsible for cancelling.
async fn resend_until_received(
    packet: Bytes,
    ingress: &mut local_bounded::Receiver<Bytes>,
    egress: &mut local_bounded::Sender<Bytes>,
    is_reply: impl Fn(&Header) -> bool,
) -> io::Result<(Header, Bytes)> {
    const RTO: Duration = sec!(3);

    loop {
        egress.send(packet.clone()).await?;

        let deadline = Instant::now() + RTO;
        while let Ok(received) = time::timeout_at(deadline, ingress.next()).await {
            let mut received = received.ok_or(io::Error::from(io::ErrorKind::BrokenPipe))?;
            let header = Header::decode_from(&mut received)?;
            if is_reply(&header) {
                return Ok((header, received));
            }
        }
    }
}

async fn write_and_flush(
    w: &mut (impl AsyncWriteExt + Unpin),
    src: &mut impl Buf,
) -> io::Result<()> {
    w.write_all_buf(src).await?;
    w.flush().await?;
    Ok(())
}

pub struct Connection {
    egress: EgressProcessor,
    ingress: IngressProcessor,
    peer_addr: SocketAddr,
}

impl Connection {
    /// # Outbound flow:
    /// ```ignore
    /// ----> SYN
    /// <---- STATE
    /// ```
    pub async fn outbound(
        peer_addr: SocketAddr,
        pipe: local_pipe::DuplexEnd,
        mut ingress: local_bounded::Receiver<Bytes>,
        mut egress: local_bounded::Sender<Bytes>,
        hasher_factory: &impl BuildHasher,
    ) -> io::Result<Self> {
        // create random connection id which is constant for a given addr, so that we don't drop
        // late packets after reconnect
        let conn_id = hasher_factory.hash_one(peer_addr) as u16;
        let mut state = ConnectionState::new_outbound(conn_id);

        let (ack_received_reporter, ack_received_notifier) = local_watch::channel(Seq::ZERO);
        let (ack_required_reporter, ack_required_notifier) = local_condvar::condvar();

        // generate SYN
        let syn = state.generate_header(TypeVer::Syn);
        let mut buffer = BytesMut::with_capacity(Header::MIN_SIZE);
        syn.encode_to(&mut buffer)?;

        // wait for STATE
        let (header, _) = resend_until_received(buffer.freeze(), &mut ingress, &mut egress, |h| {
            h.connection_id == syn.connection_id && h.ack_nr == syn.seq_nr
        })
        .await?;

        match header.type_ver {
            TypeVer::State => {
                state.process_header(&header);
            }
            typever => {
                return Err(io::Error::other(format!("unexpected first packet ({typever:?})")));
            }
        }

        let (pipe_reader, pipe_writer) = pipe.into_split();
        let state = LocalShared::new(state);

        Ok(Self {
            egress: EgressProcessor {
                state: state.clone(),
                ack_received_notifier,
                ack_required_notifier,
                receiver: pipe_reader,
                sender: egress,
            },
            ingress: IngressProcessor {
                state,
                ack_received_reporter,
                ack_required_reporter,
                sender: pipe_writer,
                receiver: ingress,
            },
            peer_addr,
        })
    }

    /// # Inbound flow (SYN is already received)
    /// ```ignore
    /// ----> STATE
    /// <---- DATA
    /// ```
    pub async fn inbound(
        peer_addr: SocketAddr,
        mut pipe: local_pipe::DuplexEnd,
        mut ingress: local_bounded::Receiver<Bytes>,
        mut egress: local_bounded::Sender<Bytes>,
        recv_syn: Header,
    ) -> io::Result<Self> {
        let mut state = ConnectionState::new_inbound(&recv_syn);
        let (ack_required_reporter, ack_required_notifier) = local_condvar::condvar();
        let (ack_received_reporter, ack_received_notifier) = local_watch::channel(Seq::ZERO);

        // generate STATE
        let state_header = state.generate_header(TypeVer::State);
        let mut buffer = BytesMut::with_capacity(Header::MIN_SIZE);
        state_header.encode_to(&mut buffer)?;

        // wait for DATA; the remote acks our STATE with seq_nr - 1 (see
        // `ConnectionState::process_header`)
        let (header, mut packet) =
            resend_until_received(buffer.freeze(), &mut ingress, &mut egress, |h| {
                h.connection_id == state.conn_id_recv() && h.ack_nr == state_header.seq_nr - seq(1)
            })
            .await?;
        skip_extensions(&mut packet, &header)?;
        match header.type_ver {
            TypeVer::Data => {
                write_and_flush(&mut pipe, &mut packet).await?;
                state.process_header(&header);
                ack_required_reporter.signal_one();
            }
            typever => {
                return Err(io::Error::other(format!("unexpected first packet ({typever:?})")));
            }
        }

        let (pipe_reader, pipe_writer) = pipe.into_split();
        let state = LocalShared::new(state);

        Ok(Self {
            egress: EgressProcessor {
                state: state.clone(),
                ack_received_notifier,
                ack_required_notifier,
                receiver: pipe_reader,
                sender: egress,
            },
            ingress: IngressProcessor {
                state,
                ack_received_reporter,
                ack_required_reporter,
                sender: pipe_writer,
                receiver: ingress,
            },
            peer_addr,
        })
    }

    pub async fn run(mut self, mut canceller: local_condvar::Receiver) -> io::Result<()> {
        let mut out_stats = EgressStats::default();
        let mut in_stats = IngressStats::default();

        let result = select! {
            biased;
            r = self.egress.run(&self.peer_addr, &mut out_stats) => r,
            r = self.ingress.run(&self.peer_addr, &mut in_stats) => r,
            _ = canceller.wait_for_one() => Err(io::ErrorKind::Interrupted.into()), // send Reset if cancelled upstream
        };

        log::debug!("Connection stats for {}: {out_stats:?} {in_stats:?}", self.peer_addr);

        let final_packet_type = match result {
            Ok(()) => TypeVer::Fin,
            Err(_) => TypeVer::Reset,
        };

        // drop local pipe to notify upstream task
        drop(self.ingress.sender);
        drop(self.egress.receiver);

        let mut buffer = BytesMut::with_capacity(Header::MIN_SIZE);
        self.egress
            .state
            .with(|state| state.generate_header(final_packet_type).encode_to(&mut buffer))?;
        self.egress.sender.send(buffer.freeze()).await?;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::RandomState;
    use std::net::Ipv4Addr;
    use tokio::join;

    const PEER_ADDR: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 6881);

    fn packet(
        type_ver: TypeVer,
        connection_id: u16,
        seq_nr: Seq,
        ack_nr: Seq,
        data: &[u8],
    ) -> Bytes {
        let header = Header {
            type_ver,
            extension: 0,
            connection_id,
            timestamp_us: 0,
            timestamp_diff_us: 0,
            wnd_size: 128 * 1024,
            seq_nr,
            ack_nr,
        };
        let mut buf = BytesMut::new();
        header.encode_to(&mut buf).unwrap();
        buf.extend_from_slice(data);
        buf.freeze()
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_outbound_handshake_ignores_late_packets_from_previous_connection() {
        let (egress_tx, mut egress_rx) = local_bounded::channel(1);
        let (mut ingress_tx, ingress_rx) = local_bounded::channel(8);
        let (pipe, _pipe) = local_pipe::duplex_pipe(1024);

        let hasher_factory = RandomState::new();
        let connect_fut =
            Connection::outbound(PEER_ADDR, pipe, ingress_rx, egress_tx, &hasher_factory);
        let peer_fut = async {
            let mut syn = egress_rx.next().await.unwrap();
            let syn = Header::decode_from(&mut syn).unwrap();
            assert_eq!(syn.type_ver, TypeVer::Syn);

            // FIN from the previous connection, which had the same connection ID
            let late_fin = packet(TypeVer::Fin, syn.connection_id, seq(100), seq(5), &[]);
            ingress_tx.send(late_fin).await.unwrap();

            let state = packet(TypeVer::State, syn.connection_id, seq(200), syn.seq_nr, &[]);
            ingress_tx.send(state).await.unwrap();
        };
        let (connect_result, ()) = join!(connect_fut, peer_fut);
        assert!(connect_result.is_ok());
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_inbound_handshake_ignores_late_packets_from_previous_connection() {
        let (egress_tx, mut egress_rx) = local_bounded::channel(1);
        let (mut ingress_tx, ingress_rx) = local_bounded::channel(8);
        let (pipe, mut remote_pipe) = local_pipe::duplex_pipe(1024);

        let syn_bytes = packet(TypeVer::Syn, 1000, seq(0), seq(0), &[]);
        let syn = Header::decode_from(&mut syn_bytes.clone()).unwrap();

        let connect_fut = Connection::inbound(PEER_ADDR, pipe, ingress_rx, egress_tx, syn);
        let peer_fut = async {
            let mut state = egress_rx.next().await.unwrap();
            let state = Header::decode_from(&mut state).unwrap();
            assert_eq!(state.type_ver, TypeVer::State);
            assert_eq!(state.connection_id, 1000);

            // DATA from the previous connection, which had the same connection ID
            let late_data = packet(TypeVer::Data, 1001, seq(1), state.seq_nr + seq(10), b"old");
            ingress_tx.send(late_data).await.unwrap();

            let data = packet(TypeVer::Data, 1001, seq(1), state.seq_nr - seq(1), b"new");
            ingress_tx.send(data).await.unwrap();

            let mut buf = [0u8; 3];
            remote_pipe.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"new");
        };
        let (connect_result, ()) = join!(connect_fut, peer_fut);
        assert!(connect_result.is_ok());
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_inbound_handshake_resends_state_until_cancelled() {
        let (egress_tx, mut egress_rx) = local_bounded::channel(1);
        let (_ingress_tx, ingress_rx) = local_bounded::channel(8);
        let (pipe, _pipe) = local_pipe::duplex_pipe(1024);

        let syn =
            Header::decode_from(&mut packet(TypeVer::Syn, 1000, seq(0), seq(0), &[])).unwrap();

        let connect_fut = time::timeout(
            sec!(10),
            Connection::inbound(PEER_ADDR, pipe, ingress_rx, egress_tx, syn),
        );
        let count_sent_fut = async {
            let mut sent_count = 0;
            while let Some(mut sent) = egress_rx.next().await {
                let header = Header::decode_from(&mut sent).unwrap();
                assert_eq!(header.type_ver, TypeVer::State);
                sent_count += 1;
            }
            sent_count
        };
        let (connect_result, sent_count) = join!(connect_fut, count_sent_fut);
        assert!(connect_result.is_err(), "handshake should still be in progress");
        // sent at 0s, 3s, 6s and 9s
        assert_eq!(sent_count, 4);
    }
}
