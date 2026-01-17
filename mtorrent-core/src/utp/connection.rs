use super::packets::OutboundPacket;
use super::protocol::{ConnectionState, Header, TypeVer, skip_extensions};
use super::retransmitter::Retransmitter;
use bytes::{Buf, Bytes, BytesMut};
use futures_util::StreamExt;
use local_async_utils::prelude::*;
use mtorrent_utils::loop_select::loop_select;
use std::collections::VecDeque;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{io, mem};
use tokio::io::{AsyncRead, AsyncWrite};

pub struct Connection {
    state: ConnectionState,
    retransmitter: Retransmitter,

    pipe: local_pipe::DuplexEnd,
    ingress: local_bounded::Receiver<Bytes>,
    egress: local_bounded::Sender<Bytes>,

    pending_retransmit: Option<Bytes>,
    pending_tx_packet: OutboundPacket, // packet to be sent once the window allows
    unprocessed_rx_packets: VecDeque<(Header, Bytes)>, // packets read from the socket but not forwarded yet
    send_ack: bool,
}

impl Connection {
    /// # Outbound flow:
    /// ```ignore
    /// ----> SYN
    /// <---- STATE
    /// ```
    pub async fn outbound(
        pipe: local_pipe::DuplexEnd,
        ingress: local_bounded::Receiver<Bytes>,
        egress: local_bounded::Sender<Bytes>,
    ) -> io::Result<Self> {
        let state = ConnectionState::new_outbound();
        let packet_size = state.packet_size();

        let mut this = Self {
            state,
            retransmitter: Retransmitter::new(),
            pipe,
            ingress,
            egress,
            pending_retransmit: None,
            pending_tx_packet: OutboundPacket::new(packet_size),
            unprocessed_rx_packets: VecDeque::new(),
            send_ack: false,
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
        pipe: local_pipe::DuplexEnd,
        ingress: local_bounded::Receiver<Bytes>,
        egress: local_bounded::Sender<Bytes>,
        recv_syn: Header,
    ) -> io::Result<Self> {
        let state = ConnectionState::new_inbound(&recv_syn);
        let packet_size = state.packet_size();

        let mut this = Self {
            state,
            retransmitter: Retransmitter::new(),
            pipe,
            ingress,
            egress,
            pending_retransmit: None,
            pending_tx_packet: OutboundPacket::new(packet_size),
            unprocessed_rx_packets: VecDeque::new(),
            send_ack: false,
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
                this.send_ack = true;
            }
            typever => {
                return Err(io::Error::other(format!("unexpected first packet ({typever:?})")));
            }
        }

        Ok(this)
    }

    pub async fn run(mut self) -> io::Result<()> {
        let result = loop_select(
            &mut self,
            [
                Self::poll_retransmitter,
                Self::poll_receive_egress,
                Self::poll_send_egress,
                Self::poll_receive_ingress,
                Self::poll_send_ingress,
            ],
        )
        .await;

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

    fn poll_retransmitter(&mut self, cx: &mut Context<'_>) -> Poll<ControlFlow<io::Result<()>>> {
        if self.pending_retransmit.is_none()
            && let Poll::Ready(result) = self.retransmitter.poll_next_retry(cx)
        {
            match result {
                Ok(packet) => {
                    self.pending_retransmit = Some(packet);
                    return Poll::Ready(ControlFlow::Continue(()));
                }
                Err(e) => {
                    return Poll::Ready(ControlFlow::Break(Err(e)));
                }
            }
        }
        Poll::Pending
    }

    fn poll_receive_egress(&mut self, cx: &mut Context<'_>) -> Poll<ControlFlow<io::Result<()>>> {
        if !self.pending_tx_packet.is_full() {
            let prev_len = self.pending_tx_packet.total_size();
            if let Poll::Ready(result) =
                self.pending_tx_packet.write(|buf| Pin::new(&mut self.pipe).poll_read(cx, buf))
            {
                return if result.is_err() || self.pending_tx_packet.total_size() == prev_len {
                    // local IO closed
                    Poll::Ready(ControlFlow::Break(Ok(())))
                } else {
                    Poll::Ready(ControlFlow::Continue(()))
                };
            }
        }
        Poll::Pending
    }

    fn poll_send_egress(&mut self, cx: &mut Context<'_>) -> Poll<ControlFlow<io::Result<()>>> {
        macro_rules! do_try {
            ($action:expr) => {{
                match $action {
                    Ok(val) => val,
                    Err(e) => return Poll::Ready(ControlFlow::Break(Err(e.into()))),
                }
            }};
        }
        if self.pending_retransmit.is_some() || self.can_flush_pending_tx_packet() || self.send_ack
        {
            match self.egress.poll_ready(cx) {
                Poll::Ready(false) => {
                    // IoDriver exited
                    return Poll::Ready(ControlFlow::Break(Err(io::ErrorKind::BrokenPipe.into())));
                }
                Poll::Ready(true) => {
                    if let Some(packet) = self.pending_retransmit.take() {
                        do_try!(self.egress.try_send(packet));
                    } else if self.can_flush_pending_tx_packet() {
                        let packet = mem::replace(
                            &mut self.pending_tx_packet,
                            OutboundPacket::new(self.state.packet_size()),
                        );
                        let header = self.state.generate_header(TypeVer::Data);
                        let packet = packet.finalize(&header);

                        do_try!(self.egress.try_send(packet.clone()));
                        do_try!(self.retransmitter.add_new_packet(packet, header.seq_nr()));
                        self.send_ack = false;
                    } else {
                        let mut buf = BytesMut::with_capacity(Header::MIN_SIZE);
                        do_try!(self.state.generate_header(TypeVer::State).encode_to(&mut buf));
                        do_try!(self.egress.try_send(buf.freeze()));
                        self.send_ack = false;
                    }
                    return Poll::Ready(ControlFlow::Continue(()));
                }
                Poll::Pending => {}
            }
        }
        Poll::Pending
    }

    fn poll_receive_ingress(&mut self, cx: &mut Context<'_>) -> Poll<ControlFlow<io::Result<()>>> {
        if let Poll::Ready(result) = self.ingress.poll_next_unpin(cx) {
            let Some(mut buffer) = result else {
                return Poll::Ready(ControlFlow::Break(Err(io::ErrorKind::BrokenPipe.into())));
            };
            // remove header and extensions
            let header = match Header::decode_from(&mut buffer) {
                Ok(h) => h,
                Err(e) => {
                    return Poll::Ready(ControlFlow::Break(Err(e)));
                }
            };
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
                        return Poll::Ready(ControlFlow::Break(Ok(())));
                    }
                    TypeVer::Reset => {
                        return Poll::Ready(ControlFlow::Break(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "connection reset by remote",
                        ))));
                    }
                    TypeVer::Syn => {
                        return Poll::Ready(ControlFlow::Break(Err(io::Error::other(
                            "received unexpected SYN",
                        ))));
                    }
                }
                Poll::Ready(ControlFlow::Continue(()))
            } else {
                // unexpected header
                Poll::Ready(ControlFlow::Break(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "header validation failed",
                ))))
            }
        } else {
            Poll::Pending
        }
    }

    fn poll_send_ingress(&mut self, cx: &mut Context<'_>) -> Poll<ControlFlow<io::Result<()>>> {
        if let Some((header, packet)) = self.unprocessed_rx_packets.front_mut()
            && let Poll::Ready(result) = Pin::new(&mut self.pipe).poll_write(cx, &*packet)
        {
            match result {
                Ok(0) | Err(_) => {
                    // local IO closed
                    Poll::Ready(ControlFlow::Break(Ok(())))
                }
                Ok(written) => {
                    packet.advance(written);
                    if packet.is_empty() {
                        // update state if the packet is fully processed
                        self.state.process_header(&*header);
                        self.send_ack = true;
                        self.unprocessed_rx_packets.pop_front();
                    }
                    Poll::Ready(ControlFlow::Continue(()))
                }
            }
        } else {
            Poll::Pending
        }
    }
}
