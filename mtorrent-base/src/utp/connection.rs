use super::protocol::{
    ConnectionState, Header, TypeVer, ValidationError, dbg_header_extensions, skip_extensions,
};
use super::retransmitter::Retransmitter;
use super::seq::Seq;
use bytes::buf::Limit;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_util::{FutureExt, StreamExt};
use local_async_utils::prelude::*;
use log::log_enabled;
use mtorrent_utils::local_watch;
use std::hash::BuildHasher;
use std::net::SocketAddr;
use std::pin::pin;
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

impl EgressProcessor {
    async fn run(&mut self, peer_addr: &SocketAddr, stats: &mut EgressStats) -> io::Result<()> {
        define_with!(self.state);

        let mut retransmitter = Retransmitter::new();
        let mut send_buffer = init_send_buf(Retransmitter::PACKET_SIZE);

        macro_rules! tx_allowed {
            () => {{
                // lost packets must be retransmitted before any new data
                !retransmitter.has_lost_packets()
                    && retransmitter.can_send(
                        send_buffer.get_ref().len(),
                        with!(|state| state.remote_window_size()),
                    )
            }};
        }

        macro_rules! send_retransmit {
            ($packet:expr) => {{
                self.sender.send($packet).await?;
                stats.retransmit_count += 1;
                if log_enabled!(log::Level::Trace) {
                    log::trace!("TX-{peer_addr}: <retransmit>");
                }
            }};
        }

        macro_rules! send_data_if_ready {
            () => {{
                if send_buffer.get_ref().len() > Header::MIN_SIZE && tx_allowed!() {
                    self.ack_required_notifier.wait_for_one().now_or_never();
                    let buf =
                        mem::replace(&mut send_buffer, init_send_buf(Retransmitter::PACKET_SIZE));
                    let header = with!(|state| state.generate_header(TypeVer::Data));
                    let packet = finalize_send_buf(buf, &header);
                    self.sender.send(packet.clone()).await?;
                    stats.data_count += 1;
                    if log_enabled!(log::Level::Trace) {
                        log::trace!("TX-{peer_addr}: {header:?}");
                    }
                    retransmitter.add_new_packet(packet, header.seq_nr);
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
                    if let Some(packet) = retransmitter.process_ack(ack) {
                        send_retransmit!(packet);
                    }
                    while let Some(packet) =
                        retransmitter.resend_lost(with!(|state| state.remote_window_size()))
                    {
                        send_retransmit!(packet);
                    }
                    send_data_if_ready!();
                }
                Some(packet) = retransmitter.next() => {
                    send_retransmit!(packet);
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
                    // duplicate acks are needed too, they end fast-timeout mode in Retransmitter
                    if last_received_ack.is_none_or(|last| header.ack_nr >= last) {
                        last_received_ack = Some(header.ack_nr);
                        self.ack_received_reporter.set_and_notify(header.ack_nr);
                    }
                    match header.type_ver {
                        TypeVer::State => {
                            with!(|state| state.process_header(&header));
                        }
                        TypeVer::Data => {
                            with!(|state| state.process_header(&header));
                            self.ack_required_reporter.signal_one();
                            if let Err(e) = write_and_flush(&mut self.sender, &mut packet).await {
                                log::debug!(
                                    "Ingress processor for {peer_addr} exiting: pipe closed ({e})"
                                );
                                return Ok(());
                            }
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

/// Send handshake `packet` and wait for a reply to it, retransmitting `packet` on timeout. Packets
/// that aren't a reply (e.g. late packets from a previous connection with the same peer) are
/// ignored. Never times out, so the caller is responsible for cancelling.
async fn send_handshake_until_reply(
    packet: Bytes,
    ingress: &mut local_bounded::Receiver<Bytes>,
    egress: &mut local_bounded::Sender<Bytes>,
    state: &ConnectionState,
) -> io::Result<(Header, Bytes)> {
    const RTO: Duration = sec!(3);

    let mut retransmit_timer = pin!(time::sleep_until(Instant::now()));

    let mut filter_received = pin!(async {
        loop {
            let mut received =
                ingress.next().await.ok_or(io::Error::from(io::ErrorKind::BrokenPipe))?;

            if let Ok(header) = Header::decode_from(&mut received)
                && state.validate_initial_header(&header).is_ok()
            {
                return Ok::<_, io::Error>((header, received));
            }
        }
    });

    loop {
        egress.send(packet.clone()).await?;
        retransmit_timer.as_mut().reset(Instant::now() + RTO);
        select! {
            biased;
            result = &mut filter_received => return result,
            _ = &mut retransmit_timer => (),
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
        let mut buffer = BytesMut::with_capacity(Header::MIN_SIZE);
        state.generate_header(TypeVer::Syn).encode_to(&mut buffer)?;

        // wait for STATE
        let (header, _) =
            send_handshake_until_reply(buffer.freeze(), &mut ingress, &mut egress, &state).await?;

        match header.type_ver {
            TypeVer::State => {
                state.process_header(&header);
            }
            TypeVer::Fin => {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            TypeVer::Reset => {
                return Err(io::ErrorKind::ConnectionReset.into());
            }
            typever => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected first packet ({typever:?})"),
                ));
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
        let mut buffer = BytesMut::with_capacity(Header::MIN_SIZE);
        state.generate_header(TypeVer::State).encode_to(&mut buffer)?;

        // wait for DATA
        let (header, mut packet) =
            send_handshake_until_reply(buffer.freeze(), &mut ingress, &mut egress, &state).await?;
        skip_extensions(&mut packet, &header)?;
        match header.type_ver {
            TypeVer::Data => {
                write_and_flush(&mut pipe, &mut packet).await?;
                state.process_header(&header);
                ack_required_reporter.signal_one();
            }
            TypeVer::Fin => {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            TypeVer::Reset => {
                return Err(io::ErrorKind::ConnectionReset.into());
            }
            typever => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected first packet ({typever:?})"),
                ));
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
    use crate::utp::seq::seq;
    use rstest::rstest;
    use std::hash::RandomState;
    use std::net::Ipv4Addr;
    use tokio::io::AsyncReadExt;
    use tokio::{join, task};

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

        let connect_task = task::spawn_local(async move {
            let hasher_factory = RandomState::new();
            Connection::outbound(PEER_ADDR, pipe, ingress_rx, egress_tx, &hasher_factory).await
        });

        task::yield_now().await;
        let mut syn = egress_rx.next().now_or_never().expect("SYN not sent").unwrap();
        let syn = Header::decode_from(&mut syn).unwrap();
        assert_eq!(syn.type_ver, TypeVer::Syn);

        // FIN from the previous connection, which had the same connection ID
        let late_fin = packet(TypeVer::Fin, syn.connection_id, seq(100), seq(5), &[]);
        ingress_tx.try_send(late_fin).unwrap();

        // malformed packet
        ingress_tx.try_send(Bytes::from_static(b"garbage")).unwrap();

        let state = packet(TypeVer::State, syn.connection_id, seq(200), syn.seq_nr, &[]);
        ingress_tx.try_send(state).unwrap();
        task::yield_now().await;

        let connect_result = connect_task.now_or_never().expect("handshake not finished").unwrap();
        assert!(connect_result.is_ok());
    }

    #[rstest]
    #[case::reset(TypeVer::Reset, io::ErrorKind::ConnectionReset)]
    #[case::fin(TypeVer::Fin, io::ErrorKind::UnexpectedEof)]
    #[case::data(TypeVer::Data, io::ErrorKind::InvalidData)]
    #[case::syn(TypeVer::Syn, io::ErrorKind::InvalidData)]
    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_outbound_handshake_fails_on_unexpected_reply(
        #[case] reply_type: TypeVer,
        #[case] expected_error: io::ErrorKind,
    ) {
        let (egress_tx, mut egress_rx) = local_bounded::channel(1);
        let (mut ingress_tx, ingress_rx) = local_bounded::channel(8);
        let (pipe, _pipe) = local_pipe::duplex_pipe(1024);

        let connect_task = task::spawn_local(async move {
            let hasher_factory = RandomState::new();
            Connection::outbound(PEER_ADDR, pipe, ingress_rx, egress_tx, &hasher_factory).await
        });

        task::yield_now().await;
        let mut syn = egress_rx.next().now_or_never().expect("SYN not sent").unwrap();
        let syn = Header::decode_from(&mut syn).unwrap();

        let reply = packet(reply_type, syn.connection_id, seq(200), syn.seq_nr, &[]);
        ingress_tx.try_send(reply).unwrap();
        task::yield_now().await;

        let connect_result = connect_task.now_or_never().expect("handshake not finished").unwrap();
        let error = connect_result.err().expect("handshake should fail");
        assert_eq!(error.kind(), expected_error, "{error}");
    }

    #[rstest]
    #[case::reset(TypeVer::Reset)]
    #[case::fin(TypeVer::Fin)]
    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_outbound_handshake_ignores_late_termination_packets(#[case] late_type: TypeVer) {
        let (egress_tx, mut egress_rx) = local_bounded::channel(1);
        let (mut ingress_tx, ingress_rx) = local_bounded::channel(8);
        let (pipe, _pipe) = local_pipe::duplex_pipe(1024);

        let connect_task = task::spawn_local(async move {
            let hasher_factory = RandomState::new();
            Connection::outbound(PEER_ADDR, pipe, ingress_rx, egress_tx, &hasher_factory).await
        });

        task::yield_now().await;
        let mut syn = egress_rx.next().now_or_never().expect("SYN not sent").unwrap();
        let syn = Header::decode_from(&mut syn).unwrap();

        let late = packet(late_type, syn.connection_id, seq(100), syn.seq_nr + seq(10), &[]);
        ingress_tx.try_send(late).unwrap();

        let state = packet(TypeVer::State, syn.connection_id, seq(200), syn.seq_nr, &[]);
        ingress_tx.try_send(state).unwrap();
        task::yield_now().await;

        let connect_result = connect_task.now_or_never().expect("handshake not finished").unwrap();
        assert!(connect_result.is_ok());
    }

    #[rstest]
    #[case::reset(TypeVer::Reset, io::ErrorKind::ConnectionReset)]
    #[case::fin(TypeVer::Fin, io::ErrorKind::UnexpectedEof)]
    #[case::state(TypeVer::State, io::ErrorKind::InvalidData)]
    #[case::syn(TypeVer::Syn, io::ErrorKind::InvalidData)]
    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_inbound_handshake_fails_on_unexpected_reply(
        #[case] reply_type: TypeVer,
        #[case] expected_error: io::ErrorKind,
    ) {
        let (egress_tx, mut egress_rx) = local_bounded::channel(1);
        let (mut ingress_tx, ingress_rx) = local_bounded::channel(8);
        let (pipe, mut remote_pipe) = local_pipe::duplex_pipe(1024);

        let syn =
            Header::decode_from(&mut packet(TypeVer::Syn, 1000, seq(0), seq(0), &[])).unwrap();

        let connect_task =
            task::spawn_local(Connection::inbound(PEER_ADDR, pipe, ingress_rx, egress_tx, syn));

        task::yield_now().await;
        let mut state = egress_rx.next().now_or_never().expect("STATE not sent").unwrap();
        let state = Header::decode_from(&mut state).unwrap();

        let reply = packet(reply_type, 1001, seq(1), state.seq_nr - seq(1), b"x");
        ingress_tx.try_send(reply).unwrap();
        task::yield_now().await;

        let connect_result = connect_task.now_or_never().expect("handshake not finished").unwrap();
        let error = connect_result.err().expect("handshake should fail");
        assert_eq!(error.kind(), expected_error, "{error}");

        let mut buf = Vec::new();
        remote_pipe
            .read_to_end(&mut buf)
            .now_or_never()
            .expect("pipe should be closed")
            .unwrap();
        assert!(buf.is_empty(), "no data should be written: {buf:?}");
    }

    #[rstest]
    #[case::reset(TypeVer::Reset)]
    #[case::fin(TypeVer::Fin)]
    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_inbound_handshake_ignores_late_termination_packets(#[case] late_type: TypeVer) {
        let (egress_tx, mut egress_rx) = local_bounded::channel(1);
        let (mut ingress_tx, ingress_rx) = local_bounded::channel(8);
        let (pipe, mut remote_pipe) = local_pipe::duplex_pipe(1024);

        let syn =
            Header::decode_from(&mut packet(TypeVer::Syn, 1000, seq(0), seq(0), &[])).unwrap();

        let connect_task =
            task::spawn_local(Connection::inbound(PEER_ADDR, pipe, ingress_rx, egress_tx, syn));

        task::yield_now().await;
        let mut state = egress_rx.next().now_or_never().expect("STATE not sent").unwrap();
        let state = Header::decode_from(&mut state).unwrap();

        let late = packet(late_type, 1001, seq(1), state.seq_nr + seq(10), &[]);
        ingress_tx.try_send(late).unwrap();

        let data = packet(TypeVer::Data, 1001, seq(1), state.seq_nr - seq(1), b"new");
        ingress_tx.try_send(data).unwrap();
        task::yield_now().await;

        let connect_result = connect_task.now_or_never().expect("handshake not finished").unwrap();
        assert!(connect_result.is_ok());

        let mut buf = [0u8; 3];
        remote_pipe
            .read_exact(&mut buf)
            .now_or_never()
            .expect("data not written")
            .unwrap();
        assert_eq!(&buf, b"new");
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_inbound_handshake_ignores_late_packets_from_previous_connection() {
        let (egress_tx, mut egress_rx) = local_bounded::channel(1);
        let (mut ingress_tx, ingress_rx) = local_bounded::channel(8);
        let (pipe, mut remote_pipe) = local_pipe::duplex_pipe(1024);

        let syn_bytes = packet(TypeVer::Syn, 1000, seq(0), seq(0), &[]);
        let syn = Header::decode_from(&mut syn_bytes.clone()).unwrap();

        let connect_task =
            task::spawn_local(Connection::inbound(PEER_ADDR, pipe, ingress_rx, egress_tx, syn));

        task::yield_now().await;
        let mut state = egress_rx.next().now_or_never().expect("STATE not sent").unwrap();
        let state = Header::decode_from(&mut state).unwrap();
        assert_eq!(state.type_ver, TypeVer::State);
        assert_eq!(state.connection_id, 1000);

        // DATA from the previous connection, which had the same connection ID
        let late_data = packet(TypeVer::Data, 1001, seq(1), state.seq_nr + seq(10), b"old");
        ingress_tx.try_send(late_data).unwrap();

        let data = packet(TypeVer::Data, 1001, seq(1), state.seq_nr - seq(1), b"new");
        ingress_tx.try_send(data).unwrap();
        task::yield_now().await;

        let connect_result = connect_task.now_or_never().expect("handshake not finished").unwrap();
        assert!(connect_result.is_ok());

        let mut buf = [0u8; 3];
        remote_pipe
            .read_exact(&mut buf)
            .now_or_never()
            .expect("data not written")
            .unwrap();
        assert_eq!(&buf, b"new");
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

    const REMOTE_SEQ: Seq = seq(200);

    /// Connection that has completed an outbound handshake and is running.
    struct Established {
        egress_rx: local_bounded::Receiver<Bytes>,
        ingress_tx: local_bounded::Sender<Bytes>,
        remote_pipe: local_pipe::DuplexEnd,
        conn_id: u16,
        _canceller: local_condvar::Sender,
    }

    impl Established {
        async fn new(egress_capacity: usize) -> Self {
            let (egress_tx, mut egress_rx) = local_bounded::channel(egress_capacity);
            let (mut ingress_tx, ingress_rx) = local_bounded::channel(8);
            let (pipe, remote_pipe) = local_pipe::duplex_pipe(1024);

            let connect_task = task::spawn_local(async move {
                let hasher_factory = RandomState::new();
                Connection::outbound(PEER_ADDR, pipe, ingress_rx, egress_tx, &hasher_factory).await
            });
            task::yield_now().await;
            let mut syn = egress_rx.next().now_or_never().expect("SYN not sent").unwrap();
            let syn = Header::decode_from(&mut syn).unwrap();

            // remote's first DATA packet will have REMOTE_SEQ
            let state = packet(TypeVer::State, syn.connection_id, REMOTE_SEQ, syn.seq_nr, &[]);
            ingress_tx.try_send(state).unwrap();
            task::yield_now().await;
            let connection =
                connect_task.now_or_never().expect("handshake not finished").unwrap().unwrap();

            let (canceller, cancel_receiver) = local_condvar::condvar();
            task::spawn_local(connection.run(cancel_receiver));
            task::yield_now().await;

            Self {
                egress_rx,
                ingress_tx,
                remote_pipe,
                conn_id: syn.connection_id,
                _canceller: canceller,
            }
        }

        fn write_local_data(&mut self, data: &[u8]) {
            write_and_flush(&mut self.remote_pipe, &mut &data[..])
                .now_or_never()
                .expect("pipe full")
                .unwrap();
        }

        fn receive_remote_packet(
            &mut self,
            type_ver: TypeVer,
            seq_nr: Seq,
            ack_nr: Seq,
            data: &[u8],
        ) {
            let packet = packet(type_ver, self.conn_id, seq_nr, ack_nr, data);
            self.ingress_tx.try_send(packet).unwrap();
        }

        async fn sent_headers(&mut self) -> Vec<Header> {
            let mut headers = Vec::new();
            loop {
                task::yield_now().await;
                match self.egress_rx.next().now_or_never() {
                    Some(Some(mut packet)) => {
                        headers.push(Header::decode_from(&mut packet).unwrap());
                    }
                    _ => return headers,
                }
            }
        }
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_data_packet_acks_received_data_instead_of_separate_state() {
        let mut conn = Established::new(1).await;

        // first packet fills the egress channel
        conn.write_local_data(b"local1");
        task::yield_now().await;
        // second packet is blocked on send
        conn.write_local_data(b"local2");
        task::yield_now().await;
        // while blocked, remote data arrives which requires an ack, and more local data is written
        conn.receive_remote_packet(TypeVer::Data, REMOTE_SEQ, seq(0), b"remote");
        task::yield_now().await;
        conn.write_local_data(b"local3");

        // the third packet carries the ack, so no separate STATE is sent
        let sent = conn.sent_headers().await;
        let summary: Vec<_> = sent.iter().map(|h| (h.type_ver, h.ack_nr)).collect();
        assert_eq!(
            summary,
            [
                (TypeVer::Data, REMOTE_SEQ - seq(1)),
                (TypeVer::Data, REMOTE_SEQ - seq(1)),
                (TypeVer::Data, REMOTE_SEQ),
            ]
        );
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_data_received_during_blocked_send_is_acked() {
        let mut conn = Established::new(1).await;

        // first packet fills the egress channel
        conn.write_local_data(b"local1");
        task::yield_now().await;
        // second packet is blocked on send
        conn.write_local_data(b"local2");
        task::yield_now().await;
        // remote data arrives while blocked
        conn.receive_remote_packet(TypeVer::Data, REMOTE_SEQ, seq(0), b"remote");
        task::yield_now().await;

        let sent = conn.sent_headers().await;
        let summary: Vec<_> = sent.iter().map(|h| (h.type_ver, h.ack_nr)).collect();
        assert_eq!(
            summary,
            [
                (TypeVer::Data, REMOTE_SEQ - seq(1)),
                (TypeVer::Data, REMOTE_SEQ - seq(1)),
                (TypeVer::State, REMOTE_SEQ),
            ]
        );
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_ack_received_during_blocked_send_is_processed() {
        let mut conn = Established::new(1).await;

        // first packet fills the egress channel
        conn.write_local_data(b"local1");
        task::yield_now().await;
        // second packet is blocked on send
        conn.write_local_data(b"local2");
        task::yield_now().await;
        // remote acks both packets while blocked
        conn.receive_remote_packet(TypeVer::State, REMOTE_SEQ, seq(2), &[]);
        task::yield_now().await;

        let sent = conn.sent_headers().await;
        let sent_seqs: Vec<_> = sent.iter().map(|h| (h.type_ver, h.seq_nr)).collect();
        assert_eq!(sent_seqs, [(TypeVer::Data, seq(1)), (TypeVer::Data, seq(2))]);

        // acked packets must not be retransmitted
        time::sleep(sec!(10)).await;
        let sent = conn.sent_headers().await;
        assert!(sent.is_empty(), "unexpected retransmit: {sent:?}");
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_timeout_retransmits_oldest_packet_and_rest_after_ack() {
        let mut conn = Established::new(8).await;

        for data in [b"local1", b"local2", b"local3"] {
            conn.write_local_data(data);
            task::yield_now().await;
        }
        let sent = conn.sent_headers().await;
        let sent_seqs: Vec<_> = sent.iter().map(|h| (h.type_ver, h.seq_nr)).collect();
        assert_eq!(
            sent_seqs,
            [
                (TypeVer::Data, seq(1)),
                (TypeVer::Data, seq(2)),
                (TypeVer::Data, seq(3))
            ]
        );

        // only the oldest packet is retransmitted on timeout
        time::sleep(sec!(1)).await;
        let sent = conn.sent_headers().await;
        let sent_seqs: Vec<_> = sent.iter().map(|h| h.seq_nr).collect();
        assert_eq!(sent_seqs, [seq(1)]);

        // ack of the retransmitted packet triggers fast resend of the next one, and the remaining
        // lost packet is resent because it fits into the window
        conn.receive_remote_packet(TypeVer::State, REMOTE_SEQ, seq(1), &[]);
        let sent = conn.sent_headers().await;
        let sent_seqs: Vec<_> = sent.iter().map(|h| h.seq_nr).collect();
        assert_eq!(sent_seqs, [seq(2), seq(3)]);

        // duplicate ack doesn't trigger any retransmits
        conn.receive_remote_packet(TypeVer::State, REMOTE_SEQ, seq(1), &[]);
        let sent = conn.sent_headers().await;
        assert!(sent.is_empty(), "unexpected retransmit: {sent:?}");

        conn.receive_remote_packet(TypeVer::State, REMOTE_SEQ, seq(3), &[]);
        time::sleep(sec!(10)).await;
        let sent = conn.sent_headers().await;
        assert!(sent.is_empty(), "unexpected retransmit: {sent:?}");
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_data_is_acked_when_local_pipe_is_full() {
        let mut conn = Established::new(8).await;

        // payload exceeds the pipe capacity, so delivery to the local side blocks
        let payload = vec![0xAB; 2000];
        conn.receive_remote_packet(TypeVer::Data, REMOTE_SEQ, seq(0), &payload);

        // ack is sent even though the local side hasn't read anything
        let sent = conn.sent_headers().await;
        let summary: Vec<_> = sent.iter().map(|h| (h.type_ver, h.ack_nr)).collect();
        assert_eq!(summary, [(TypeVer::State, REMOTE_SEQ)]);

        // outgoing data acks the received packet as well
        conn.write_local_data(b"local1");
        let sent = conn.sent_headers().await;
        let summary: Vec<_> = sent.iter().map(|h| (h.type_ver, h.ack_nr)).collect();
        assert_eq!(summary, [(TypeVer::Data, REMOTE_SEQ)]);

        // draining the pipe delivers the whole payload and doesn't trigger extra acks
        let mut received = Vec::new();
        while received.len() < payload.len() {
            let bytes_read = conn
                .remote_pipe
                .read_buf(&mut received)
                .now_or_never()
                .expect("data not delivered")
                .unwrap();
            assert_ne!(bytes_read, 0, "pipe closed");
            task::yield_now().await;
        }
        assert_eq!(received, payload);
        let sent = conn.sent_headers().await;
        assert!(sent.is_empty(), "unexpected packets: {sent:?}");
    }
}
