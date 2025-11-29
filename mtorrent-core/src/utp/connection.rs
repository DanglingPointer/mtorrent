use super::protocol::{ConnectionState, Header, TypeVer, ValidationError, skip_extensions};
use super::retransmitter::Retransmitter;
use super::seq::Seq;
use bytes::{BufMut, Bytes, BytesMut, buf::Limit};
use futures_util::{FutureExt, StreamExt};
use local_async_utils::prelude::*;
use log::log_enabled;
use mtorrent_utils::local_watch;
use std::hash::BuildHasher;
use std::net::SocketAddr;
use std::{io, mem};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{select, time};

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
    async fn run(&mut self, peer_addr: &SocketAddr) -> io::Result<()> {
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
                    retransmitter.add_new_packet(packet, header.seq_nr());
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
                packet = retransmitter.next_retransmit() => {
                    self.sender.send(packet).await?;
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
                        Ok(0) if Limit::limit(&send_buffer) > 0 => {
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
    async fn run(&mut self, peer_addr: &SocketAddr) -> io::Result<()> {
        define_with!(self.state);

        let mut last_received_ack = None;

        while let Some(mut packet) = self.receiver.next().await {
            let header = Header::decode_from(&mut packet)?;
            skip_extensions(&mut packet, &header)?;

            match with!(|state| state.validate_header(&header)) {
                Ok(()) => {
                    if last_received_ack.is_none_or(|last| header.ack_nr() > last) {
                        last_received_ack = Some(header.ack_nr());
                        self.ack_received_reporter.set_and_notify(header.ack_nr());
                    }
                    match header.type_ver() {
                        TypeVer::State => {
                            with!(|state| state.process_header(&header));
                        }
                        TypeVer::Data => {
                            if let Err(e) = self.sender.write_all_buf(&mut packet).await {
                                log::debug!(
                                    "Ingress processor for {peer_addr} exiting: pipe closed ({e})"
                                );
                                return Ok(());
                            }
                            with!(|state| state.process_header(&header));
                            self.ack_required_reporter.signal_one();
                        }
                        TypeVer::Fin => {
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
                        if log_enabled!(log::Level::Debug) {
                            log::debug!(
                                "Received duplicate {:?} packet from {peer_addr}, seq_nr={}, ack_nr={}",
                                header.type_ver(),
                                header.seq_nr(),
                                header.ack_nr()
                            );
                        }
                        if header.type_ver() == TypeVer::Data {
                            self.ack_required_reporter.signal_one();
                        }
                    }
                    e @ ValidationError::OutOfOrder { .. } => {
                        if log_enabled!(log::Level::Debug) {
                            log::debug!("Received out-of-order packet from {peer_addr}: {e}");
                        }
                    }
                },
            }
        }

        Ok(())
    }
}

async fn resend_until_received(
    packet: Bytes,
    ingress: &mut local_bounded::Receiver<Bytes>,
    egress: &mut local_bounded::Sender<Bytes>,
) -> io::Result<Bytes> {
    let rto = sec!(3);
    loop {
        egress.send(packet.clone()).await?;

        if let Ok(received) = time::timeout(rto, ingress.next()).await {
            return received.ok_or(io::ErrorKind::BrokenPipe.into());
        }
    }
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
        // create random connection id which is constant for a given addr, so that we don't drop late packets after reconnect
        let conn_id = hasher_factory.hash_one(peer_addr) as u16;
        let mut state = ConnectionState::new_outbound(conn_id);

        let (ack_received_reporter, ack_received_notifier) = local_watch::channel(Seq::ZERO);
        let (ack_required_reporter, ack_required_notifier) = local_condvar::condvar();

        // generate SYN
        let mut buffer = BytesMut::with_capacity(Header::MIN_SIZE);
        state.generate_header(TypeVer::Syn).encode_to(&mut buffer)?;

        let mut packet = resend_until_received(buffer.freeze(), &mut ingress, &mut egress).await?;

        // parse STATE
        let header = Header::decode_from(&mut packet)?;
        match header.type_ver() {
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
        let mut buffer = BytesMut::with_capacity(Header::MIN_SIZE);
        state.generate_header(TypeVer::State).encode_to(&mut buffer)?;

        let mut packet = resend_until_received(buffer.freeze(), &mut ingress, &mut egress).await?;

        // parse DATA
        let header = Header::decode_from(&mut packet)?;
        skip_extensions(&mut packet, &header)?;
        match header.type_ver() {
            TypeVer::Data => {
                pipe.write_all_buf(&mut packet).await?;
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

    pub async fn run(mut self) -> io::Result<()> {
        let result = select! {
            biased;
            r = self.egress.run(&self.peer_addr) => r,
            r = self.ingress.run(&self.peer_addr) => r,
        };

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
