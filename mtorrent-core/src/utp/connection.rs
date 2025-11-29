use super::driver::{Action, DataReceiver, DataSender, PeerConnection};
use super::protocol::ConnectionState as ProtocolState;
use super::protocol::{ExtensionIter, Header, TypeVer};
use super::utils::{BufferPool, Retransmitter};
use bytes::{Buf, BytesMut};
use derive_more::Display;
use enum_dispatch::enum_dispatch;
use pin_project::pin_project;
use std::collections::VecDeque;
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use std::{fmt, io, mem};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{Sleep, sleep, sleep_until};

#[pin_project(project = ConnectionProj)]
pub struct Connection<IO: AsyncRead + AsyncWrite> {
    stage: Stage,

    remote_addr: SocketAddr,
    state: ProtocolState,
    retransmitter: Retransmitter,
    buffer_pool: BufferPool,

    pending_tx_packet: BytesMut, // packet to be sent once the window allows
    pending_rx_data_packets: VecDeque<(Header, BytesMut)>, // packets read from the socket but not forwarded yet

    #[pin]
    local_io: IO,
    #[pin]
    timer: Option<Sleep>, // used for retransmits and for connect timeout
    ack_required: bool,
}

impl<IO: AsyncRead + AsyncWrite> Connection<IO> {
    pub fn new_outbound(pipe: IO, remote_addr: SocketAddr, connect_timeout: Duration) -> Self {
        let state = ProtocolState::new_outbound();
        let mut buffer_pool = BufferPool::new(state.packet_size());
        Self {
            stage: Init.into(),
            remote_addr,
            state,
            retransmitter: Retransmitter::new(),
            pending_tx_packet: buffer_pool.get_buffer(),
            buffer_pool,
            pending_rx_data_packets: VecDeque::new(),
            local_io: pipe,
            timer: Some(sleep(connect_timeout)),
            ack_required: false,
        }
    }

    pub fn new_inbound(
        pipe: IO,
        remote_addr: SocketAddr,
        recv_syn: Header,
        connect_timeout: Duration,
    ) -> Self {
        let state = ProtocolState::new_inbound(&recv_syn);
        let mut buffer_pool = BufferPool::new(state.packet_size());
        Self {
            stage: SynRecv.into(),
            remote_addr,
            state,
            retransmitter: Retransmitter::new(),
            pending_tx_packet: buffer_pool.get_buffer(),
            buffer_pool,
            pending_rx_data_packets: VecDeque::new(),
            local_io: pipe,
            timer: Some(sleep(connect_timeout)),
            ack_required: false,
        }
    }
}

impl<'pin, IO: AsyncRead + AsyncWrite> ConnectionProj<'pin, IO> {
    fn can_flush_pending_tx_packet(&self) -> bool {
        self.pending_tx_packet.len() > Header::MIN_SIZE
            && (self.retransmitter.total_bytes_in_flight() == 0
                || self.pending_tx_packet.len() + self.retransmitter.total_bytes_in_flight()
                    <= self.state.max_window_size())
    }
}

impl<IO: AsyncRead + AsyncWrite> PeerConnection for Connection<IO> {
    type InboundInitData = Header;

    fn peer_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    fn current_stage(&self) -> impl fmt::Display {
        self.stage
    }

    fn poll_next_action(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Action> {
        let stage = self.stage;
        stage.poll_next_action(self.project(), cx)
    }

    fn process_ingress<R: DataReceiver>(self: Pin<&mut Self>, receiver: R) -> io::Result<()> {
        let stage = self.stage;
        stage.process_ingress(self.project(), receiver)
    }

    fn produce_egress<S: DataSender>(self: Pin<&mut Self>, sender: S) -> io::Result<()> {
        let stage = self.stage;
        stage.produce_egress(self.project(), sender)
    }

    fn process_unknown_source<R: DataReceiver>(receiver: R) -> io::Result<Self::InboundInitData> {
        let mut buffer = [MaybeUninit::<u8>::uninit(); Header::MIN_SIZE];
        let mut buf = ReadBuf::uninit(&mut buffer);
        receiver.try_recv_buf(&mut buf)?;
        let header = Header::decode_from(&mut buf.filled())?;
        match header.type_ver() {
            TypeVer::Syn => Ok(header),
            typever => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("expected SYN, got {typever:?}"),
            )),
        }
    }
}

#[enum_dispatch]
trait Actor {
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

/// # Outbound flow:
/// ```text
/// stage = Init
/// ----> SYN
/// stage = SynSent
/// <---- STATE
/// stage = Connected
/// ```
///
/// # Inbound flow:
/// ```text
/// stage = SynRecv
/// ----> STATE
/// stage = StateSent
/// <---- DATA
/// stage = Connected
/// ```
#[enum_dispatch(Actor)]
#[derive(Clone, Copy, Debug, Display)]
enum Stage {
    // outbound connect:
    Init(Init),
    SynSent(SynSent),

    // inbound connect:
    SynRecv(SynRecv),
    StateSent(StateSent),

    // common:
    Connected(Connected),
    Closing(Closing),
    Closed(Closed),
}

/// Newly created outbound connection, need to send SYN
#[derive(Clone, Copy, Debug, Display)]
struct Init;
impl Actor for Init {
    fn poll_next_action<'pin, IO: AsyncRead + AsyncWrite>(
        self,
        mut this: ConnectionProj<'pin, IO>,
        cx: &mut Context<'_>,
    ) -> Poll<Action> {
        // handle connect timeout
        if let Some(timer) = this.timer.as_mut().as_pin_mut()
            && timer.poll(cx).is_ready()
        {
            this.timer.set(None);
            log::warn!("Outbound connect to {} timed out", this.remote_addr);
            *this.stage = Closed.into();
            Poll::Ready(Action::RemoveConnection)
        } else {
            Poll::Ready(Action::FlushEgress)
        }
    }

    fn process_ingress<'pin, IO: AsyncRead + AsyncWrite, R: DataReceiver>(
        self,
        this: ConnectionProj<'pin, IO>,
        receiver: R,
    ) -> io::Result<()> {
        // unexpected
        let mut buffer = this.buffer_pool.get_buffer();
        receiver.try_recv_buf(&mut buffer)?;
        let header = Header::decode_from(&mut buffer)?;
        this.buffer_pool.reclaim_buffer(buffer);
        match header.type_ver() {
            TypeVer::Syn => {
                // at this point we haven't sent SYN yet, so we can turn outbound connect into inbound
                *this.state = ProtocolState::new_inbound(&header);
                *this.stage = SynRecv.into();
                Ok(())
            }
            typever => Err(io::Error::other(format!(
                "Unexpected first packet on new connection: {typever:?}",
            ))),
        }
    }

    fn produce_egress<'pin, IO: AsyncRead + AsyncWrite, S: DataSender>(
        self,
        this: ConnectionProj<'pin, IO>,
        sender: S,
    ) -> io::Result<()> {
        // send SYN
        let mut buffer = [MaybeUninit::<u8>::uninit(); Header::MIN_SIZE];
        let mut buf = ReadBuf::uninit(&mut buffer);
        this.state.generate_header(TypeVer::Syn).encode_to(&mut buf)?;
        sender.try_send(buf.filled())?;
        *this.stage = SynSent.into();
        Ok(())
    }
}

/// Outbound connection, waiting for inbound STATE after outbound SYN
#[derive(Clone, Copy, Debug, Display)]
struct SynSent;
impl Actor for SynSent {
    fn poll_next_action<'pin, IO: AsyncRead + AsyncWrite>(
        self,
        mut this: ConnectionProj<'pin, IO>,
        cx: &mut Context<'_>,
    ) -> Poll<Action> {
        // handle connect timeout
        if let Some(timer) = this.timer.as_mut().as_pin_mut()
            && timer.poll(cx).is_ready()
        {
            this.timer.set(None);
            log::warn!("Outbound connect to {} timed out", this.remote_addr);
            *this.stage = Closed.into();
            Poll::Ready(Action::RemoveConnection)
        } else {
            Poll::Pending
        }
    }

    fn process_ingress<'pin, IO: AsyncRead + AsyncWrite, R: DataReceiver>(
        self,
        mut this: ConnectionProj<'pin, IO>,
        receiver: R,
    ) -> io::Result<()> {
        // receive STATE
        let mut buffer = this.buffer_pool.get_buffer();
        receiver.try_recv_buf(&mut buffer)?;
        let header = Header::decode_from(&mut buffer)?;
        this.buffer_pool.reclaim_buffer(buffer);
        match header.type_ver() {
            TypeVer::State => {
                this.state.process_header(&header);
                *this.stage = Connected.into();
                this.timer.set(None);
                Ok(())
            }
            typever => Err(io::Error::other(format!(
                "Unexpected first packet on outbound connection: {typever:?}",
            ))),
        }
    }

    fn produce_egress<'pin, IO: AsyncRead + AsyncWrite, S: DataSender>(
        self,
        _this: ConnectionProj<'pin, IO>,
        _sender: S,
    ) -> io::Result<()> {
        // should never happen
        Ok(())
    }
}

/// Accepted inbound connection (just received SYN), need to send STATE
#[derive(Clone, Copy, Debug, Display)]
struct SynRecv;
impl Actor for SynRecv {
    fn poll_next_action<'pin, IO: AsyncRead + AsyncWrite>(
        self,
        mut this: ConnectionProj<'pin, IO>,
        cx: &mut Context<'_>,
    ) -> Poll<Action> {
        // handle connect timeout
        if let Some(timer) = this.timer.as_mut().as_pin_mut()
            && timer.poll(cx).is_ready()
        {
            this.timer.set(None);
            log::warn!("Inbound connect from {} timed out", this.remote_addr);
            *this.stage = Closed.into();
            Poll::Ready(Action::RemoveConnection)
        } else {
            Poll::Ready(Action::FlushEgress)
        }
    }

    fn process_ingress<'pin, IO: AsyncRead + AsyncWrite, R: DataReceiver>(
        self,
        this: ConnectionProj<'pin, IO>,
        receiver: R,
    ) -> io::Result<()> {
        // unexpected
        Init.process_ingress(this, receiver)
    }

    fn produce_egress<'pin, IO: AsyncRead + AsyncWrite, S: DataSender>(
        self,
        this: ConnectionProj<'pin, IO>,
        sender: S,
    ) -> io::Result<()> {
        // send STATE
        let mut buffer = [MaybeUninit::<u8>::uninit(); Header::MIN_SIZE];
        let mut buf = ReadBuf::uninit(&mut buffer);
        this.state.generate_header(TypeVer::State).encode_to(&mut buf)?;
        sender.try_send(buf.filled())?;
        *this.stage = StateSent.into(); // TODO: wait for data
        Ok(())
    }
}

/// Inbound connection, just sent STATE, waiting for DATA
#[derive(Clone, Copy, Debug, Display)]
struct StateSent;
impl Actor for StateSent {
    fn poll_next_action<'pin, IO: AsyncRead + AsyncWrite>(
        self,
        mut this: ConnectionProj<'pin, IO>,
        cx: &mut Context<'_>,
    ) -> Poll<Action> {
        // handle connect timeout
        if let Some(timer) = this.timer.as_mut().as_pin_mut()
            && timer.poll(cx).is_ready()
        {
            this.timer.set(None);
            log::warn!("Inbound connect from {} timed out", this.remote_addr);
            *this.stage = Closed.into();
            Poll::Ready(Action::RemoveConnection)
        } else {
            Poll::Pending
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

        match header.type_ver() {
            TypeVer::Data => {
                this.state.process_header(&header);
                this.pending_rx_data_packets.push_back((header, buffer));
                *this.ack_required = true;
                *this.stage = Connected.into();
                this.timer.set(None);
                Ok(())
            }
            typever => Err(io::Error::other(format!(
                "Unexpected second packet on outbound connection: {typever:?}",
            ))),
        }
    }

    fn produce_egress<'pin, IO: AsyncRead + AsyncWrite, S: DataSender>(
        self,
        _this: ConnectionProj<'pin, IO>,
        _sender: S,
    ) -> io::Result<()> {
        // should never happen
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Display)]
struct Connected;
impl Actor for Connected {
    fn poll_next_action<'pin, IO: AsyncRead + AsyncWrite>(
        self,
        mut this: ConnectionProj<'pin, IO>,
        cx: &mut Context<'_>,
    ) -> Poll<Action> {
        let mut made_progress = false;

        // poll local IO for egress data
        if this.pending_tx_packet.len() < this.state.packet_size() {
            if this.pending_tx_packet.is_empty() {
                // make space for header, to be written later before sending
                this.pending_tx_packet.extend_from_slice(&[0u8; Header::MIN_SIZE]);
            }
            let max_bytes_to_write = this.state.packet_size() - this.pending_tx_packet.len();
            if this.pending_tx_packet.capacity() < this.state.packet_size() {
                this.pending_tx_packet
                    .reserve(this.state.packet_size() - this.pending_tx_packet.capacity());
            }
            let buf = this.pending_tx_packet.spare_capacity_mut();
            let mut buf = ReadBuf::uninit(&mut buf[..max_bytes_to_write]);
            if let Poll::Ready(result) = this.local_io.as_mut().poll_read(cx, &mut buf) {
                match (result, buf.filled().len()) {
                    (_, 0) | (Err(_), _) => {
                        // local IO closed
                        *this.stage = Closing.into();
                        this.timer.set(None);
                        return Poll::Ready(Action::FlushEgress); // not strictly necessary
                    }
                    (Ok(()), bytes_read) => {
                        let filled = bytes_read + this.pending_tx_packet.len();
                        unsafe { this.pending_tx_packet.set_len(filled) }
                    }
                }
                made_progress = true;
            }
        }

        // submit received data to local IO
        if let Some((header, packet)) = this.pending_rx_data_packets.front_mut()
            && let Poll::Ready(result) = this.local_io.as_mut().poll_write(cx, &*packet)
        {
            match result {
                Ok(0) | Err(_) => {
                    // local IO closed
                    *this.stage = Closing.into();
                    this.timer.set(None);
                    return Poll::Ready(Action::FlushEgress); // not strictly necessary
                }
                Ok(written) => {
                    packet.advance(written);
                    if packet.is_empty() {
                        // update state if the packet is fully processed
                        this.state.process_header(&*header);
                        *this.ack_required = true;
                        this.buffer_pool.reclaim_buffer(mem::take(packet));
                        this.pending_rx_data_packets.pop_front();
                    }
                }
            }
            made_progress = true;
        }

        let mut pending_egress = this.can_flush_pending_tx_packet();

        // poll retransmit timer
        if let Some(timer) = this.timer.as_mut().as_pin_mut()
            && timer.poll(cx).is_ready()
        {
            this.timer.set(None);
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

        if this.state.validate_header(&header) {
            // delay or stop retransmit timer
            if this.retransmitter.discard_acked_packets(header.ack_nr(), this.buffer_pool) > 0
                && let Some(timer) = this.timer.as_mut().as_pin_mut()
            {
                if let Some(retransmit_at) = this.retransmitter.next_retransmit_time() {
                    timer.reset(retransmit_at)
                } else {
                    this.timer.set(None);
                }
            }
            // enqueue packet if data, else process immediately
            match header.type_ver() {
                TypeVer::Data => {
                    this.pending_rx_data_packets.push_back((header, buffer));
                }
                TypeVer::State => {
                    this.state.process_header(&header);
                    this.buffer_pool.reclaim_buffer(buffer);
                }
                TypeVer::Fin => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed by remote",
                    ));
                }
                TypeVer::Reset => {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "connection reset by remote",
                    ));
                }
                TypeVer::Syn => {
                    return Err(io::Error::other("received unexpected SYN"));
                }
            }
        } else {
            log::warn!("Header validation failed, typever: {:?}", header.type_ver());
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
            let header = this.state.generate_header(TypeVer::Data);
            header.encode_to(&mut this.pending_tx_packet.as_mut())?;
            let packet = mem::replace(this.pending_tx_packet, this.buffer_pool.get_buffer());
            sender.try_send(&packet)?;
            this.retransmitter.add_packet(packet, header.seq_nr())?;
        } else {
            // send ack
            let mut buffer = [MaybeUninit::<u8>::uninit(); Header::MIN_SIZE];
            let mut buf = ReadBuf::uninit(&mut buffer);
            this.state.generate_header(TypeVer::State).encode_to(&mut buf)?;
            sender.try_send(buf.filled())?;
        }

        // clear pending ack
        *this.ack_required = false;

        // start retransmit timer if needed
        if let Some(timeout) = this.retransmitter.next_retransmit_time()
            && this.timer.is_none()
        {
            this.timer.set(Some(sleep_until(timeout)));
        }
        Ok(())
    }
}

/// About to send FIN
#[derive(Clone, Copy, Debug, Display)]
struct Closing;
impl Actor for Closing {
    fn poll_next_action<'pin, IO: AsyncRead + AsyncWrite>(
        self,
        _this: ConnectionProj<'pin, IO>,
        _cx: &mut Context<'_>,
    ) -> Poll<Action> {
        Poll::Ready(Action::FlushEgress)
    }

    fn process_ingress<'pin, IO: AsyncRead + AsyncWrite, R: DataReceiver>(
        self,
        this: ConnectionProj<'pin, IO>,
        receiver: R,
    ) -> io::Result<()> {
        // process header (to update last recvd seq nr), drop the rest
        let mut buffer = this.buffer_pool.get_buffer();
        receiver.try_recv_buf(&mut buffer)?;
        let header = Header::decode_from(&mut buffer)?;
        this.state.process_header(&header);
        this.buffer_pool.reclaim_buffer(buffer);
        Ok(())
    }

    fn produce_egress<'pin, IO: AsyncRead + AsyncWrite, S: DataSender>(
        self,
        this: ConnectionProj<'pin, IO>,
        sender: S,
    ) -> io::Result<()> {
        // send fin
        let mut buffer = [MaybeUninit::<u8>::uninit(); Header::MIN_SIZE];
        let mut buf = ReadBuf::uninit(&mut buffer);
        this.state.generate_header(TypeVer::Fin).encode_to(&mut buf)?;
        sender.try_send(buf.filled())?;
        *this.stage = Closed.into();
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Display)]
struct Closed;
impl Actor for Closed {
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
