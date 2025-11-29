use super::Header;
use super::driver::{Action, DataReceiver, DataSender, PeerConnection};
use super::protocol::{ConnectionData, ExtensionIter, TypeVer};
use super::utils::{BufferPool, Retransmitter};
use bytes::{Buf, BytesMut};
use enum_dispatch::enum_dispatch;
use pin_project::pin_project;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{io, mem};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{Sleep, sleep_until};

#[pin_project(project = ConnectionProj)]
pub struct Connection<IO: AsyncRead + AsyncWrite> {
    state: State,

    remote_addr: SocketAddr,
    data: ConnectionData,
    retransmitter: Retransmitter,
    buffer_pool: BufferPool,

    pending_tx_packet: BytesMut, // packet to be sent once the window allows
    pending_rx_data_packets: VecDeque<(Header, BytesMut)>, // packets read from the socket but not forwarded yet

    #[pin]
    local_io: IO,
    #[pin]
    retransmit_timer: Option<Sleep>,
    ack_required: bool,
}

impl<IO: AsyncRead + AsyncWrite> Connection<IO> {
    #[expect(dead_code)]
    pub fn new(pipe: IO, remote_addr: SocketAddr, data: ConnectionData) -> Self {
        let mut buffer_pool = BufferPool::new(data.packet_size());
        Self {
            state: Init.into(),
            remote_addr,
            data,
            retransmitter: Retransmitter::new(),
            pending_tx_packet: buffer_pool.get_buffer(),
            buffer_pool,
            pending_rx_data_packets: VecDeque::new(),
            local_io: pipe,
            retransmit_timer: None,
            ack_required: false,
        }
    }
}

impl<IO: AsyncRead + AsyncWrite> PeerConnection for Connection<IO> {
    fn peer_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    fn poll_next_action(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Action> {
        let this = self.project();
        this.state.poll_next_action(this, cx)
    }

    fn process_ingress<R: DataReceiver>(self: Pin<&mut Self>, receiver: R) -> io::Result<()> {
        let this = self.project();
        this.state.process_ingress(this, receiver)
    }

    fn produce_egress<S: DataSender>(self: Pin<&mut Self>, sender: S) -> io::Result<()> {
        let this = self.project();
        this.state.produce_egress(this, sender)
    }
}

impl<'pin, IO: AsyncRead + AsyncWrite> ConnectionProj<'pin, IO> {
    fn can_flush_pending_tx_packet(&self) -> bool {
        self.pending_tx_packet.len() > Header::MIN_SIZE
            && (self.retransmitter.total_bytes_in_flight() == 0
                || self.pending_tx_packet.len() + self.retransmitter.total_bytes_in_flight()
                    <= self.data.max_window_size())
    }
}

#[enum_dispatch]
trait PeerConnectionState {
    fn poll_next_action<'pin, IO: AsyncRead + AsyncWrite>(
        self,
        this: ConnectionProj<'pin, IO>,
        cx: &mut Context<'_>,
    ) -> Poll<Action>;

    fn process_ingress<'pin, IO: AsyncRead + AsyncWrite, R: DataReceiver>(
        self,
        this: ConnectionProj<'pin, IO>,
        receiver: R,
    ) -> io::Result<()>;

    fn produce_egress<'pin, IO: AsyncRead + AsyncWrite, S: DataSender>(
        self,
        this: ConnectionProj<'pin, IO>,
        sender: S,
    ) -> io::Result<()>;
}

#[enum_dispatch(PeerConnectionState)]
#[derive(Clone, Copy)]
enum State {
    Init,
    // SynSent,
    // SynRecv,
    Connected,
    // FinSent,
    // FinRecv,
    Closed,
}

#[derive(Clone, Copy)]
struct Init;

impl PeerConnectionState for Init {
    fn poll_next_action<'pin, IO: AsyncRead + AsyncWrite>(
        self,
        _this: ConnectionProj<'pin, IO>,
        _cx: &mut Context<'_>,
    ) -> Poll<Action> {
        todo!()
    }

    fn process_ingress<'pin, IO: AsyncRead + AsyncWrite, R: DataReceiver>(
        self,
        this: ConnectionProj<'pin, IO>,
        _receiver: R,
    ) -> io::Result<()> {
        *this.state = Connected.into();
        todo!()
    }

    fn produce_egress<'pin, IO: AsyncRead + AsyncWrite, S: DataSender>(
        self,
        _this: ConnectionProj<'pin, IO>,
        _sender: S,
    ) -> io::Result<()> {
        todo!()
    }
}

#[derive(Clone, Copy)]
struct Connected;

impl PeerConnectionState for Connected {
    fn poll_next_action<'pin, IO: AsyncRead + AsyncWrite>(
        self,
        mut this: ConnectionProj<'pin, IO>,
        cx: &mut Context<'_>,
    ) -> Poll<Action> {
        let mut made_progress = false;

        // poll local IO for egress data
        if this.pending_tx_packet.len() < this.data.packet_size() {
            if this.pending_tx_packet.is_empty() {
                // make space for header, to be written later before sending
                this.pending_tx_packet.extend_from_slice(&[0u8; Header::MIN_SIZE]);
            }
            let max_bytes_to_write = this.data.packet_size() - this.pending_tx_packet.len();
            if this.pending_tx_packet.capacity() < this.data.packet_size() {
                this.pending_tx_packet
                    .reserve(this.data.packet_size() - this.pending_tx_packet.capacity());
            }
            let buf = this.pending_tx_packet.spare_capacity_mut();
            let mut buf = ReadBuf::uninit(&mut buf[..max_bytes_to_write]);
            if let Poll::Ready(result) = this.local_io.as_mut().poll_read(cx, &mut buf) {
                match result {
                    Ok(()) => {
                        let filled = buf.filled().len() + this.pending_tx_packet.len();
                        unsafe { this.pending_tx_packet.set_len(filled) }
                        made_progress = true;
                    }
                    Err(_e) => return Poll::Ready(Action::RemoveConnection), // TODO: go to FIN state
                }
            }
        }

        // submit received data to local IO
        if let Some((header, packet)) = this.pending_rx_data_packets.front_mut()
            && let Poll::Ready(result) = this.local_io.as_mut().poll_write(cx, &*packet)
        {
            match result {
                Ok(0) => return Poll::Ready(Action::RemoveConnection), // TODO: go to FIN state
                Ok(written) => {
                    made_progress = true;
                    packet.advance(written);
                    if packet.is_empty() {
                        // update state if the packet is fully processed
                        this.data.process_header(&*header);
                        *this.ack_required = true;
                        this.buffer_pool.reclaim_buffer(mem::take(packet));
                        this.pending_rx_data_packets.pop_front();
                    }
                }
                Err(_e) => return Poll::Ready(Action::RemoveConnection), // TODO: go to FIN state
            }
        }

        let mut pending_egress = this.can_flush_pending_tx_packet();

        // poll retransmit timer
        if let Some(timer) = this.retransmit_timer.as_mut().as_pin_mut()
            && timer.poll(cx).is_ready()
        {
            this.retransmit_timer.set(None);
            pending_egress = true;
            made_progress = true;
        }

        if !made_progress {
            Poll::Pending
        } else if pending_egress || *this.ack_required {
            Poll::Ready(Action::FlushEgress)
        } else {
            Poll::Ready(Action::None)
        }
    }

    fn process_ingress<'pin, IO: AsyncRead + AsyncWrite, R: DataReceiver>(
        self,
        mut this: ConnectionProj<'pin, IO>,
        receiver: R,
    ) -> io::Result<()> {
        let mut buffer = this.buffer_pool.get_buffer();
        receiver.try_recv_buf(&mut buffer)?;

        // remove header and extensions
        let header = Header::decode_from(&mut buffer)?;
        if header.has_extensions() {
            let mut extensions = ExtensionIter::new(buffer.chunk());
            while extensions.next().is_some() {
                // ignore extensions for now
            }
            let headers_len = buffer.chunk().len() - extensions.remainder().len();
            buffer.advance(headers_len);
        }

        if this.data.validate_header(&header) {
            // delay or stop retransmit timer
            if this.retransmitter.discard_acked_packets(header.ack_nr(), this.buffer_pool) > 0
                && let Some(timer) = this.retransmit_timer.as_mut().as_pin_mut()
            {
                if let Some(retransmit_at) = this.retransmitter.next_retransmit_time() {
                    timer.reset(retransmit_at)
                } else {
                    this.retransmit_timer.set(None);
                }
            }
            // enqueue packet if data, else process immediately
            if header.is_data() {
                this.pending_rx_data_packets.push_back((header, buffer));
            } else {
                this.data.process_header(&header);
                this.buffer_pool.reclaim_buffer(buffer);
            }
        } else {
            this.buffer_pool.reclaim_buffer(buffer);
        }
        Ok(())
    }

    fn produce_egress<'pin, IO: AsyncRead + AsyncWrite, S: DataSender>(
        self,
        mut this: ConnectionProj<'pin, IO>,
        sender: S,
    ) -> io::Result<()> {
        if let Some((packet, seq_nr)) = this.retransmitter.next_packet_to_retransmit() {
            // retransmit unacked packet
            sender.try_send(&packet)?;
            this.retransmitter.add_packet(packet, seq_nr)?;
        } else if this.can_flush_pending_tx_packet() {
            // send new packet
            let header = this.data.generate_header(TypeVer::Data);
            header.encode_to(&mut this.pending_tx_packet.as_mut())?;
            let packet = mem::replace(this.pending_tx_packet, this.buffer_pool.get_buffer());
            sender.try_send(&packet)?;
            this.retransmitter.add_packet(packet, header.seq_nr())?;
        } else {
            // send ack
            let mut header_buf = [0u8; Header::MIN_SIZE];
            this.data
                .generate_header(TypeVer::State)
                .encode_to(&mut header_buf.as_mut_slice())?;
            sender.try_send(&header_buf)?;
        }

        // clear pending ack
        *this.ack_required = false;

        // start retransmit timer if needed
        if let Some(timeout) = this.retransmitter.next_retransmit_time()
            && this.retransmit_timer.is_none()
        {
            this.retransmit_timer.set(Some(sleep_until(timeout)));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct Closed;

impl PeerConnectionState for Closed {
    fn poll_next_action<'pin, IO: AsyncRead + AsyncWrite>(
        self,
        _this: ConnectionProj<'pin, IO>,
        _cx: &mut Context<'_>,
    ) -> Poll<Action> {
        Poll::Ready(Action::RemoveConnection)
    }

    fn process_ingress<'pin, IO: AsyncRead + AsyncWrite, R: DataReceiver>(
        self,
        this: ConnectionProj<'pin, IO>,
        receiver: R,
    ) -> io::Result<()> {
        let mut buffer = this.buffer_pool.get_buffer();
        receiver.try_recv_buf(&mut buffer)?;
        Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection is closed"))
    }

    fn produce_egress<'pin, IO: AsyncRead + AsyncWrite, S: DataSender>(
        self,
        _this: ConnectionProj<'pin, IO>,
        _sender: S,
    ) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection is closed"))
    }
}
