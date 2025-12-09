use super::packets::OutboundPacket;
use super::protocol::{ConnectionState, Header, TypeVer, skip_extensions};
use super::retransmitter::Retransmitter;
use bytes::{Buf, Bytes, BytesMut};
use futures_util::StreamExt;
use local_async_utils::prelude::*;
use std::collections::VecDeque;
use std::future::poll_fn;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::{io, mem};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task;

pub struct Connection {
    state: ConnectionState,
    retransmitter: Retransmitter,

    local_io: local_pipe::DuplexEnd,
    ingress: local_bounded::Receiver<Bytes>,
    egress: local_bounded::Sender<Bytes>,

    pending_retransmit: Option<Bytes>,
    pending_tx_packet: OutboundPacket, // packet to be sent once the window allows
    unprocessed_rx_packets: VecDeque<(Header, Bytes)>, // packets read from the socket but not forwarded yet
    ack_required: bool,
}

impl Connection {
    /// # Outbound flow:
    /// ```ignore
    /// ----> SYN
    /// <---- STATE
    /// ```
    pub async fn outbound(
        local_io: local_pipe::DuplexEnd,
        ingress: local_bounded::Receiver<Bytes>,
        egress: local_bounded::Sender<Bytes>,
    ) -> io::Result<Self> {
        let state = ConnectionState::new_outbound();
        let packet_size = state.packet_size();

        let mut this = Self {
            state,
            retransmitter: Retransmitter::new(),
            local_io,
            ingress,
            egress,
            pending_retransmit: None,
            pending_tx_packet: OutboundPacket::new(packet_size),
            unprocessed_rx_packets: VecDeque::new(),
            ack_required: false,
        };

        // send SYN
        let mut buffer = BytesMut::with_capacity(Header::MIN_SIZE);
        this.state.generate_header(TypeVer::Syn).encode_to(&mut buffer)?;
        this.egress.send(buffer.freeze()).await?;

        // receive STATE
        let mut packet = this.ingress.next().await.ok_or(io::ErrorKind::BrokenPipe)?;
        let header = Header::decode_from(&mut packet)?;
        match header.type_ver() {
            TypeVer::State => {
                this.state.process_header(&header);
            }
            typever => {
                return Err(io::Error::other(format!("unexpected first packet ({typever:?})")));
            }
        }

        Ok(this)
    }

    /// # Inbound flow (SYN is already received)
    /// ```ignore
    /// ----> STATE
    /// <---- DATA
    /// ```
    pub async fn inbound(
        local_io: local_pipe::DuplexEnd,
        ingress: local_bounded::Receiver<Bytes>,
        egress: local_bounded::Sender<Bytes>,
        recv_syn: Header,
    ) -> io::Result<Self> {
        let state = ConnectionState::new_inbound(&recv_syn);
        let packet_size = state.packet_size();

        let mut this = Self {
            state,
            retransmitter: Retransmitter::new(),
            local_io,
            ingress,
            egress,
            pending_retransmit: None,
            pending_tx_packet: OutboundPacket::new(packet_size),
            unprocessed_rx_packets: VecDeque::new(),
            ack_required: false,
        };

        // send STATE
        let mut buffer = BytesMut::with_capacity(Header::MIN_SIZE);
        this.state.generate_header(TypeVer::State).encode_to(&mut buffer)?;
        this.egress.send(buffer.freeze()).await?;

        // receive DATA
        let mut packet = this.ingress.next().await.ok_or(io::ErrorKind::BrokenPipe)?;
        let header = Header::decode_from(&mut packet)?;
        skip_extensions(&mut packet, &header);
        match header.type_ver() {
            TypeVer::Data => {
                this.state.process_header(&header);
                this.unprocessed_rx_packets.push_back((header, packet));
                this.ack_required = true;
            }
            typever => {
                return Err(io::Error::other(format!("unexpected first packet ({typever:?})")));
            }
        }

        Ok(this)
    }

    pub async fn run(mut self) -> io::Result<()> {
        let result = poll_fn(|cx| self.poll_run(cx)).await;

        let final_packet_type = match result {
            Ok(()) => TypeVer::Fin,
            Err(_) => TypeVer::Reset,
        };
        let mut buffer = BytesMut::with_capacity(Header::MIN_SIZE);
        self.state.generate_header(final_packet_type).encode_to(&mut buffer)?;
        self.egress.send(buffer.freeze()).await?;
        result
    }

    fn can_flush_pending_tx_packet(&self) -> bool {
        self.pending_tx_packet.has_data()
            && (self.retransmitter.total_bytes_in_flight() == 0
                || self.pending_tx_packet.total_size() + self.retransmitter.total_bytes_in_flight()
                    <= self.state.max_window_size())
    }

    fn poll_run(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            let coop = ready!(task::coop::poll_proceed(cx));
            let mut made_progress = false;

            // poll retransmitter
            if self.pending_retransmit.is_none()
                && let Poll::Ready(packet) = self.retransmitter.poll_next_retry(cx)?
            {
                self.pending_retransmit = Some(packet);
                made_progress = true;
            }

            // poll local IO for egress data
            if !self.pending_tx_packet.is_full() {
                let prev_len = self.pending_tx_packet.total_size();
                if let Poll::Ready(result) = self
                    .pending_tx_packet
                    .write(|buf| Pin::new(&mut self.local_io).poll_read(cx, buf))
                {
                    if result.is_err() || self.pending_tx_packet.total_size() == prev_len {
                        // local IO closed
                        return Poll::Ready(Ok(()));
                    }
                    made_progress = true;
                }
            }

            // poll send egress
            if self.pending_retransmit.is_some()
                || self.can_flush_pending_tx_packet()
                || self.ack_required
            {
                match self.egress.poll_ready(cx) {
                    Poll::Ready(false) => {
                        // IoDriver exited
                        return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
                    }
                    Poll::Ready(true) => {
                        if let Some(packet) = self.pending_retransmit.take() {
                            self.egress.try_send(packet)?;
                        } else if self.can_flush_pending_tx_packet() {
                            let packet = mem::replace(
                                &mut self.pending_tx_packet,
                                OutboundPacket::new(self.state.packet_size()),
                            );
                            let header = self.state.generate_header(TypeVer::Data);
                            let packet = packet.finalize(&header);

                            self.egress.try_send(packet.clone())?;
                            self.retransmitter.add_new_packet(packet, header.seq_nr())?;
                            self.ack_required = false;
                        } else {
                            let mut buf = BytesMut::with_capacity(Header::MIN_SIZE);
                            self.state.generate_header(TypeVer::State).encode_to(&mut buf)?;
                            self.egress.try_send(buf.freeze())?;
                            self.ack_required = false;
                        }
                        made_progress = true;
                    }
                    Poll::Pending => {}
                }
            }

            // poll receive ingress
            if let Poll::Ready(result) = self.ingress.poll_next_unpin(cx) {
                let Some(mut buffer) = result else {
                    return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
                };
                // remove header and extensions
                let header = Header::decode_from(&mut buffer)?;
                skip_extensions(&mut buffer, &header);
                if self.state.validate_header(&header) {
                    self.retransmitter.discard_acked_packets(header.ack_nr());
                    // enqueue packet if data, else process immediately
                    match header.type_ver() {
                        TypeVer::Data => {
                            // TODO: check current window
                            self.unprocessed_rx_packets.push_back((header, buffer));
                        }
                        TypeVer::State => {
                            self.state.process_header(&header);
                        }
                        TypeVer::Fin => {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "connection closed by remote",
                            )));
                        }
                        TypeVer::Reset => {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "connection reset by remote",
                            )));
                        }
                        TypeVer::Syn => {
                            return Poll::Ready(Err(io::Error::other("received unexpected SYN")));
                        }
                    }
                }
                made_progress = true;
            }

            // submit ingress to local IO
            if let Some((header, packet)) = self.unprocessed_rx_packets.front_mut()
                && let Poll::Ready(result) = Pin::new(&mut self.local_io).poll_write(cx, &*packet)
            {
                match result {
                    Ok(0) | Err(_) => {
                        // local IO closed
                        return Poll::Ready(Ok(()));
                    }
                    Ok(written) => {
                        packet.advance(written);
                        if packet.is_empty() {
                            // update state if the packet is fully processed
                            self.state.process_header(&*header);
                            self.ack_required = true;
                            self.unprocessed_rx_packets.pop_front();
                        }
                    }
                }
                made_progress = true;
            }

            if !made_progress {
                return Poll::Pending;
            }

            coop.made_progress();
        }
    }
}
