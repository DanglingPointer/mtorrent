use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use local_async_utils::prelude::*;
use log::log_enabled;
use mtorrent_utils::info_stopwatch;
use mtorrent_utils::loop_select::loop_select;
use std::collections::hash_map::{Entry, HashMap};
use std::io;
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::task::{Context, Poll, ready};
use tokio::io::ReadBuf;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

pub(super) enum Command {
    AddConnection((SocketAddr, ConnectionHandle)),
    ResetConnections,
}

pub(super) struct ConnectionHandle {
    pub(super) egress: local_bounded::Receiver<Bytes>,
    pub(super) ingress: local_bounded::Sender<Bytes>,
}

const MAX_UDP_PACKET_SIZE: usize = 65536;

fn is_transient_error(e: &io::ErrorKind) -> bool {
    use io::ErrorKind::*;
    matches!(
        e,
        ConnectionRefused
            | ConnectionReset
            | HostUnreachable
            | NetworkUnreachable
            | ConnectionAborted
    )
}

/// [`UdpDemux`] is responsible for sending and receiving UDP packets
/// and dispatching them to the appropriate connections.
pub struct UdpDemux {
    commands: mpsc::Receiver<Command>,
    socket: UdpSocket,
    connections: HashMap<SocketAddr, ConnectionHandle>,
    pending_tx: Vec<(SocketAddr, Bytes)>,
    rx_buffer: Box<[MaybeUninit<u8>]>,
    new_source_reporter: local_bounded::Sender<(SocketAddr, Bytes)>,
    reset_in_progress: bool,
}

impl UdpDemux {
    pub(super) fn new(
        command_receiver: mpsc::Receiver<Command>,
        socket: UdpSocket,
        new_source_reporter: local_bounded::Sender<(SocketAddr, Bytes)>,
    ) -> Self {
        Self {
            commands: command_receiver,
            socket,
            connections: HashMap::new(),
            pending_tx: Vec::new(),
            rx_buffer: Box::new_uninit_slice(MAX_UDP_PACKET_SIZE),
            new_source_reporter,
            reset_in_progress: false,
        }
    }

    /// Run the I/O (de-)multiplexing.
    pub async fn run(mut self) {
        log::info!("uTP Demux started");
        let _sw = info_stopwatch!("uTP Demux");
        loop_select(&mut self, [Self::poll_commands, Self::poll_egress, Self::poll_ingress]).await;
    }

    fn poll_commands(&mut self, cx: &mut Context<'_>) -> Poll<ControlFlow<()>> {
        if self.reset_in_progress {
            if self.connections.is_empty() {
                self.reset_in_progress = false;
                log::info!("uTP reset complete");
            } else {
                // wait for all connections to close
                return Poll::Pending;
            }
        }

        macro_rules! trigger_reset {
            ($reason:expr) => {{
                if !self.reset_in_progress {
                    log::info!("uTP Demux {} initiated", $reason);
                    self.pending_tx.clear();
                    for connection in self.connections.values_mut() {
                        // fake an empty inbound packet which will fail parsing and trigger an outbound RST and connection exit
                        connection.ingress.queue().clear();
                        _ = connection.ingress.try_send(Bytes::new());
                    }
                    self.reset_in_progress = true;
                }
            }};
        }

        match ready!(self.commands.poll_recv(cx)) {
            Some(Command::AddConnection((peer_addr, connection))) => {
                match self.connections.entry(peer_addr) {
                    Entry::Occupied(mut entry) => {
                        if entry.get().ingress.is_closed() || entry.get().egress.is_closed() {
                            // replace closed connection
                            entry.insert(connection);
                        } else {
                            log::error!("Not adding connection to {peer_addr}: already exists");
                        }
                    }
                    Entry::Vacant(e) => {
                        e.insert(connection);
                    }
                }
                Poll::Ready(ControlFlow::Continue(()))
            }
            Some(Command::ResetConnections) => {
                if self.connections.is_empty() {
                    Poll::Ready(ControlFlow::Continue(()))
                } else {
                    trigger_reset!("reset");
                    Poll::Pending // yield to allow connections to process the reset
                }
            }
            None => {
                // application exiting
                if self.connections.is_empty() {
                    Poll::Ready(ControlFlow::Break(()))
                } else {
                    trigger_reset!("shutdown");
                    Poll::Pending // yield to allow connections to process the reset
                }
            }
        }
    }

    fn poll_egress(&mut self, cx: &mut Context<'_>) -> Poll<ControlFlow<()>> {
        let mut made_progress = false;

        if let Some((remote_addr, packet)) = self.pending_tx.last() {
            // has pending egress, try sending it
            if let Poll::Ready(ret) = self.socket.poll_send_to(cx, packet, *remote_addr) {
                match ret {
                    Ok(bytes_sent) if bytes_sent != packet.len() => {
                        log::error!(
                            "Incomplete send to {remote_addr}: {bytes_sent}/{}",
                            packet.len()
                        );
                        self.connections.remove(remote_addr);
                    }
                    Err(e) => {
                        // `e` is guaranteed to never be WouldBlock here
                        log::error!("Send failed to {remote_addr}: {e}");
                        self.connections.remove(remote_addr);
                    }
                    Ok(_) => {
                        if log_enabled!(log::Level::Trace) {
                            log::trace!("TX-{remote_addr}: {}", String::from_utf8_lossy(packet));
                        }
                    }
                }
                self.pending_tx.pop();
                made_progress = true;
            }
        } else {
            // iterate through connections and fill the egress queue with at most 1 packet from each connection
            self.connections.retain(|remote_addr, connection| {
                match connection.egress.poll_next_unpin(cx) {
                    Poll::Ready(None) => {
                        made_progress = true;
                        false
                    }
                    Poll::Ready(Some(packet)) => {
                        made_progress = true;
                        self.pending_tx.push((*remote_addr, packet));
                        true
                    }
                    Poll::Pending => true,
                }
            });
        }

        if made_progress {
            Poll::Ready(ControlFlow::Continue(()))
        } else {
            Poll::Pending
        }
    }

    fn poll_ingress(&mut self, cx: &mut Context<'_>) -> Poll<ControlFlow<()>> {
        let mut buf = ReadBuf::uninit(&mut self.rx_buffer);

        let (source_addr, packet) = match ready!(self.socket.poll_recv_from(cx, &mut buf)) {
            Ok(addr) => {
                let packet = BytesMut::from(buf.filled());
                (addr, packet.freeze())
            }
            Err(e) => {
                log::error!("Receive failed: {e:?}");
                // the error might be caused by the last send()
                return Poll::Ready(if is_transient_error(&e.kind()) {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break(())
                });
            }
        };
        if log_enabled!(log::Level::Trace) {
            log::trace!("RX-{source_addr}: {}", String::from_utf8_lossy(&packet));
        }

        match self.connections.entry(source_addr) {
            Entry::Occupied(mut connection) => {
                match connection.get_mut().ingress.try_send(packet) {
                    Ok(_) => {}
                    Err(local_sync_error::TrySendError::Full(_buf)) => {
                        log::warn!("Dropping received packet from {source_addr}");
                    }
                    Err(local_sync_error::TrySendError::Closed(_buf)) => {
                        connection.remove();
                        // TODO: should we report this as unknown source?
                    }
                }
            }
            Entry::Vacant(_) => match self.new_source_reporter.try_send((source_addr, packet)) {
                Ok(_) => {}
                Err(e) => {
                    // don't exit even if the channel is closed
                    log::warn!("Dropping received packet from new source {source_addr}: {e}");
                }
            },
        }
        Poll::Ready(ControlFlow::Continue(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use std::net::Ipv4Addr;
    use tokio::task;

    struct Connection {
        egress: local_bounded::Sender<Bytes>,
        ingress: local_bounded::Receiver<Bytes>,
    }

    const INGRESS_CHANNEL_CAPACITY: usize = 10;

    fn new_connection() -> (Connection, ConnectionHandle) {
        let (egress_tx, egress_rx) = local_bounded::channel(1);
        let (ingress_tx, ingress_rx) = local_bounded::channel(INGRESS_CHANNEL_CAPACITY);
        (
            Connection {
                egress: egress_tx,
                ingress: ingress_rx,
            },
            ConnectionHandle {
                egress: egress_rx,
                ingress: ingress_tx,
            },
        )
    }

    #[tokio::test(flavor = "local")]
    async fn test_egress_to_multiple_connections() {
        let peer1_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let peer1_addr = peer1_socket.local_addr().unwrap();

        let peer2_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let peer2_addr = peer2_socket.local_addr().unwrap();

        let driver_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let driver_addr = driver_socket.local_addr().unwrap();

        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        let (unknown_tx, _unknown_rx) = local_bounded::channel(1);
        let driver = UdpDemux::new(cmd_rx, driver_socket, unknown_tx);
        task::spawn_local(driver.run());

        let (mut conn1, handle1) = new_connection();
        cmd_tx.send(Command::AddConnection((peer1_addr, handle1))).await.unwrap();

        let (mut conn2, handle2) = new_connection();
        cmd_tx.send(Command::AddConnection((peer2_addr, handle2))).await.unwrap();

        let msg1 = Bytes::from("Hello, Peer 1!");
        conn1.egress.send(msg1.clone()).await.unwrap();

        let mut buf1 = [0u8; 1024];
        let (len1, addr) = peer1_socket.recv_from(&mut buf1).await.unwrap();
        assert_eq!(addr, driver_addr);
        assert_eq!(&buf1[..len1], &msg1[..]);

        let msg2 = Bytes::from("Hello, Peer 2!");
        conn2.egress.send(msg2.clone()).await.unwrap();

        let mut buf2 = [0u8; 1024];
        let (len2, addr) = peer2_socket.recv_from(&mut buf2).await.unwrap();
        assert_eq!(addr, driver_addr);
        assert_eq!(&buf2[..len2], &msg2[..]);

        let msg2 = Bytes::from("Second message to Peer 2");
        conn2.egress.send(msg2.clone()).await.unwrap();

        let msg1 = Bytes::from("Second message to Peer 1");
        for _ in 0..5 {
            conn1.egress.send(msg1.clone()).await.unwrap();
        }

        let mut buf2 = [0u8; 1024];
        let (len2, addr) = peer2_socket.recv_from(&mut buf2).await.unwrap();
        assert_eq!(addr, driver_addr);
        assert_eq!(&buf2[..len2], &msg2[..]);

        for _ in 0..5 {
            let mut buf1 = [0u8; 1024];
            let (len1, addr) = peer1_socket.recv_from(&mut buf1).await.unwrap();
            assert_eq!(addr, driver_addr);
            assert_eq!(&buf1[..len1], &msg1[..]);
        }
    }

    #[tokio::test(flavor = "local")]
    async fn test_ingress_from_multiple_connections() {
        let peer1_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let peer1_addr = peer1_socket.local_addr().unwrap();

        let peer2_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let peer2_addr = peer2_socket.local_addr().unwrap();

        let driver_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let driver_addr = driver_socket.local_addr().unwrap();

        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        let (unknown_tx, _unknown_rx) = local_bounded::channel(1);
        let driver = UdpDemux::new(cmd_rx, driver_socket, unknown_tx);
        task::spawn_local(driver.run());

        let (mut conn1, handle1) = new_connection();
        cmd_tx.send(Command::AddConnection((peer1_addr, handle1))).await.unwrap();

        let (mut conn2, handle2) = new_connection();
        cmd_tx.send(Command::AddConnection((peer2_addr, handle2))).await.unwrap();

        let msg1 = Bytes::from("Hello from Peer 1!");
        peer1_socket.send_to(&msg1, driver_addr).await.unwrap();

        let msg2 = Bytes::from("Hello from Peer 2!");
        peer2_socket.send_to(&msg2, driver_addr).await.unwrap();

        let received1 = conn1.ingress.next().await.unwrap();
        assert_eq!(received1, msg1);

        let received2 = conn2.ingress.next().await.unwrap();
        assert_eq!(received2, msg2);

        assert!(conn1.ingress.next().now_or_never().is_none());
        assert!(conn2.ingress.next().now_or_never().is_none());
    }

    #[tokio::test(flavor = "local")]
    async fn test_unknown_source_reporting() {
        let peer_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let peer_addr = peer_socket.local_addr().unwrap();

        let driver_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let driver_addr = driver_socket.local_addr().unwrap();

        let (_cmd_tx, cmd_rx) = mpsc::channel(1);
        let (unknown_tx, mut unknown_rx) = local_bounded::channel(10);
        let driver = UdpDemux::new(cmd_rx, driver_socket, unknown_tx);
        task::spawn_local(driver.run());

        let msg = Bytes::from("Hello from unknown source!");
        peer_socket.send_to(&msg, driver_addr).await.unwrap();

        let (reported_addr, reported_msg) = unknown_rx.next().await.unwrap();
        assert_eq!(reported_addr, peer_addr);
        assert_eq!(reported_msg, msg);
    }

    #[tokio::test(flavor = "local")]
    async fn test_reset_all_connections() {
        let driver_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let driver_addr = driver_socket.local_addr().unwrap();

        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        let (unknown_tx, mut unknown_rx) = local_bounded::channel(1);
        let driver = UdpDemux::new(cmd_rx, driver_socket, unknown_tx);
        task::spawn_local(driver.run());

        let (mut conn1, handle1) = new_connection();
        cmd_tx
            .send(Command::AddConnection(((Ipv4Addr::LOCALHOST, 12345u16).into(), handle1)))
            .await
            .unwrap();

        let (mut conn2, handle2) = new_connection();
        cmd_tx
            .send(Command::AddConnection(((Ipv4Addr::LOCALHOST, 23456u16).into(), handle2)))
            .await
            .unwrap();

        cmd_tx.send(Command::ResetConnections).await.unwrap();

        // Connections should receive empty packets to trigger reset
        let received1 = conn1.ingress.next().await.unwrap();
        assert!(received1.is_empty());

        let received2 = conn2.ingress.next().await.unwrap();
        assert!(received2.is_empty());

        // try adding a new connection during reset
        let peer_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let peer_addr = peer_socket.local_addr().unwrap();

        let (mut conn, handle) = new_connection();
        cmd_tx.send(Command::AddConnection((peer_addr, handle))).await.unwrap();
        task::yield_now().await;

        // verify the connection hasn't been added yet
        let msg = Bytes::from("Hello during reset!");
        peer_socket.send_to(&msg, driver_addr).await.unwrap();

        let (reported_addr, reported_msg) = unknown_rx.next().await.unwrap();
        assert_eq!(reported_addr, peer_addr);
        assert_eq!(reported_msg, msg);

        assert!(conn.ingress.next().now_or_never().is_none());

        // finish reset by dropping connections
        drop(conn1);
        drop(conn2);
        task::yield_now().await;

        // now the pending command should be processed
        let msg = Bytes::from("Hello after reset complete!");
        peer_socket.send_to(&msg, driver_addr).await.unwrap();

        let received_msg = conn.ingress.next().await.unwrap();
        assert_eq!(received_msg, msg);
    }

    #[tokio::test(flavor = "local")]
    async fn test_initial_reset() {
        let peer_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let peer_addr = peer_socket.local_addr().unwrap();

        let driver_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let driver_addr = driver_socket.local_addr().unwrap();

        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        let (unknown_tx, _unknown_rx) = local_bounded::channel(1);
        let driver = UdpDemux::new(cmd_rx, driver_socket, unknown_tx);
        task::spawn_local(driver.run());

        cmd_tx.send(Command::ResetConnections).await.unwrap();

        let (mut conn, handle) = new_connection();
        cmd_tx.send(Command::AddConnection((peer_addr, handle))).await.unwrap();

        let msg = Bytes::from("Hello after initial reset!");
        conn.egress.send(msg.clone()).await.unwrap();

        let mut buf = [0u8; 1024];
        let (len, addr) = peer_socket.recv_from(&mut buf).await.unwrap();
        assert_eq!(addr, driver_addr);
        assert_eq!(&buf[..len], &msg[..]);

        let msg = Bytes::from("Hello from Peer after initial reset!");
        peer_socket.send_to(&msg, driver_addr).await.unwrap();

        let received_msg = conn.ingress.next().await.unwrap();
        assert_eq!(received_msg, msg);
    }

    #[tokio::test(flavor = "local")]
    async fn test_closed_connection_reported_as_unknown() {
        let peer_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let peer_addr = peer_socket.local_addr().unwrap();

        let driver_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let driver_addr = driver_socket.local_addr().unwrap();

        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        let (unknown_tx, mut unknown_rx) = local_bounded::channel(10);
        let driver = UdpDemux::new(cmd_rx, driver_socket, unknown_tx);
        task::spawn_local(driver.run());

        let (conn, handle) = new_connection();
        cmd_tx.send(Command::AddConnection((peer_addr, handle))).await.unwrap();

        // Close the connection
        drop(conn.ingress);
        drop(conn.egress);

        // Send a packet from the same peer
        let msg = Bytes::from("Hello after deletion!");
        peer_socket.send_to(&msg, driver_addr).await.unwrap();

        // It should be reported as unknown
        let (reported_addr, reported_msg) = unknown_rx.next().await.unwrap();
        assert_eq!(reported_addr, peer_addr);
        assert_eq!(reported_msg, msg);
    }

    #[cfg_attr(target_family = "windows", ignore)]
    #[tokio::test(flavor = "local")]
    async fn test_unreachable_connection_is_deleted() {
        let peer_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 0u16).into();

        let driver_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();

        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        let (unknown_tx, _unknown_rx) = local_bounded::channel(10);
        let driver = UdpDemux::new(cmd_rx, driver_socket, unknown_tx);
        task::spawn_local(driver.run());

        let (mut conn, handle) = new_connection();
        cmd_tx.send(Command::AddConnection((peer_addr, handle))).await.unwrap();

        let msg = Bytes::from("Hello to unreachable peer!");
        conn.egress.send(msg.clone()).await.unwrap();

        assert!(conn.ingress.next().await.is_none());
        assert!(conn.egress.send(msg).await.is_err());
    }

    #[tokio::test(flavor = "local")]
    async fn test_dont_exit_on_receive_error() {
        let _ = simple_logger::SimpleLogger::new()
            .with_level(log::LevelFilter::Off)
            .with_module_level("mtorrent_core::utp", log::LevelFilter::Trace)
            .init();
        let peer_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 0u16).into();

        let driver_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();

        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        let (unknown_tx, _unknown_rx) = local_bounded::channel(10);
        let driver = UdpDemux::new(cmd_rx, driver_socket, unknown_tx);
        let driver_handle = task::spawn_local(driver.run());

        {
            let (mut conn, handle) = new_connection();
            cmd_tx.send(Command::AddConnection((peer_addr, handle))).await.unwrap();

            let msg = Bytes::from("Hello to unreachable peer!");
            conn.egress.send(msg.clone()).await.unwrap();
        }

        // on windows the send error only shows up later during the next call to recv()
        tokio::time::sleep(millisec!(100)).await;
        assert!(!driver_handle.is_finished());

        {
            let (mut conn, handle) = new_connection();
            cmd_tx.send(Command::AddConnection((peer_addr, handle))).await.unwrap();

            let msg = Bytes::from("Retry hello to unreachable peer!");
            conn.egress.send(msg.clone()).await.unwrap();
        }
    }

    #[tokio::test(flavor = "local")]
    async fn test_send_and_receive_big_packets() {
        let peer_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let peer_addr = peer_socket.local_addr().unwrap();

        let driver_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let driver_addr = driver_socket.local_addr().unwrap();

        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        let (unknown_tx, mut unknown_rx) = local_bounded::channel(10);
        let driver = UdpDemux::new(cmd_rx, driver_socket, unknown_tx);
        task::spawn_local(driver.run());

        let long_msg = Bytes::from(vec![rand::random::<u8>(); 32 * 1024]);
        peer_socket.send_to(&long_msg, driver_addr).await.unwrap();

        let (reported_addr, reported_msg) = unknown_rx.next().await.unwrap();
        assert_eq!(reported_addr, peer_addr);
        assert_eq!(reported_msg.len(), long_msg.len());
        assert_eq!(reported_msg, long_msg);

        let (mut conn, handle) = new_connection();
        cmd_tx.send(Command::AddConnection((peer_addr, handle))).await.unwrap();

        conn.egress.send(long_msg.clone()).await.unwrap();
        let mut buf = vec![0u8; 32 * 1024];
        let (len, addr) = peer_socket.recv_from(&mut buf).await.unwrap();
        assert_eq!(addr, driver_addr);
        assert_eq!(len, long_msg.len());
        assert_eq!(&buf[..len], &long_msg[..]);

        let long_msg = Bytes::from(vec![rand::random::<u8>(); 32 * 1024]);
        peer_socket.send_to(&long_msg, driver_addr).await.unwrap();

        let received_msg = conn.ingress.next().await.unwrap();
        assert_eq!(received_msg.len(), long_msg.len());
        assert_eq!(received_msg, long_msg);
    }

    #[tokio::test(flavor = "local")]
    async fn test_driver_shutdown_on_command_channel_close() {
        let peer_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let peer_addr = peer_socket.local_addr().unwrap();

        let driver_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let driver_addr = driver_socket.local_addr().unwrap();

        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        let (unknown_tx, _unknown_rx) = local_bounded::channel(1);
        let driver = UdpDemux::new(cmd_rx, driver_socket, unknown_tx);
        let driver_handle = task::spawn_local(driver.run());

        // add a connection
        let (mut conn, handle) = new_connection();
        cmd_tx.send(Command::AddConnection((peer_addr, handle))).await.unwrap();

        // sending a message succeeds
        conn.egress.send(Bytes::from("hello")).await.unwrap();
        let mut buf = [0u8; 1024];
        let (len, addr) = peer_socket.recv_from(&mut buf).await.unwrap();
        assert_eq!(addr, driver_addr);
        assert_eq!(&buf[..len], b"hello");

        // Drop the command sender to close the channel
        drop(cmd_tx);
        task::yield_now().await;

        // the connection should receive an empty packet to signal shutdown
        let received = conn.ingress.next().await.unwrap();
        assert!(received.is_empty());

        // the connection should be able to send the final message
        conn.egress.send(Bytes::from("bye")).await.unwrap();
        drop(conn);
        let (len, addr) = peer_socket.recv_from(&mut buf).await.unwrap();
        assert_eq!(addr, driver_addr);
        assert_eq!(&buf[..len], b"bye");

        // The driver should've exited gracefully after sending the last message
        driver_handle.now_or_never().unwrap().unwrap();
    }

    #[tokio::test(flavor = "local")]
    async fn test_drop_received_packet_when_ingress_full() {
        // let _ = simple_logger::SimpleLogger::new()
        //     .with_level(log::LevelFilter::Off)
        //     .with_module_level("mtorrent_core::utp", log::LevelFilter::Trace)
        //     .init();
        let peer_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let peer_addr = peer_socket.local_addr().unwrap();

        let driver_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let driver_addr = driver_socket.local_addr().unwrap();

        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        let (unknown_tx, _unknown_rx) = local_bounded::channel(10);
        let driver = UdpDemux::new(cmd_rx, driver_socket, unknown_tx);
        task::spawn_local(driver.run());

        let (mut conn, handle) = new_connection();
        cmd_tx.send(Command::AddConnection((peer_addr, handle))).await.unwrap();

        // Fill the ingress channel
        let packets: Vec<_> = (0..=INGRESS_CHANNEL_CAPACITY)
            .map(|i| Bytes::from(format!("Packet {}", i)))
            .collect();

        for packet in &packets {
            peer_socket.send_to(packet, driver_addr).await.unwrap();
        }

        // Receive packets up to capacity
        for expected_packet in &packets[..INGRESS_CHANNEL_CAPACITY] {
            let received_packet = conn.ingress.next().await.unwrap();
            assert_eq!(&received_packet, expected_packet);
        }

        // The last packet should be dropped
        task::yield_now().await;
        assert!(conn.ingress.next().now_or_never().is_none());

        let msg = Bytes::from("This packet will be received instead of packet 10");
        peer_socket.send_to(&msg, driver_addr).await.unwrap();

        let received_packet = conn.ingress.next().await.unwrap();
        assert_eq!(received_packet, msg);
    }

    #[tokio::test(flavor = "local")]
    async fn test_dont_add_same_connection_twice() {
        let driver_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();

        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        let (unknown_tx, _unknown_rx) = local_bounded::channel(10);
        let driver = UdpDemux::new(cmd_rx, driver_socket, unknown_tx);
        task::spawn_local(driver.run());

        let peer_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 12345u16).into();

        let (mut conn1, handle1) = new_connection();
        cmd_tx.send(Command::AddConnection((peer_addr, handle1))).await.unwrap();

        let (mut conn2, handle2) = new_connection();
        cmd_tx.send(Command::AddConnection((peer_addr, handle2))).await.unwrap();

        task::yield_now().await;

        conn1.egress.send(Bytes::new()).await.unwrap();
        let e = conn2.egress.send(Bytes::new()).await.unwrap_err();
        assert!(matches!(e, local_sync_error::SendError::Closed(_)));
    }

    #[tokio::test(flavor = "local")]
    async fn test_replace_locally_closed_connection() {
        let peer_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let peer_addr = peer_socket.local_addr().unwrap();

        let driver_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
        let driver_addr = driver_socket.local_addr().unwrap();

        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        let (unknown_tx, _unknown_rx) = local_bounded::channel(10);
        let driver = UdpDemux::new(cmd_rx, driver_socket, unknown_tx);
        task::spawn_local(driver.run());

        let (mut conn1, handle1) = new_connection();
        cmd_tx.send(Command::AddConnection((peer_addr, handle1))).await.unwrap();

        drop(conn1.ingress);

        let (mut conn2, handle2) = new_connection();
        cmd_tx.send(Command::AddConnection((peer_addr, handle2))).await.unwrap();

        let msg1 = Bytes::from("Hello from new connection 1");
        conn1.egress.send(msg1.clone()).await.unwrap();

        let msg2 = Bytes::from("Hello from new connection 2");
        conn2.egress.send(msg2.clone()).await.unwrap();

        let mut buf = [0u8; 1024];
        let (len, addr) = peer_socket.recv_from(&mut buf).await.unwrap();
        assert_eq!(addr, driver_addr);
        assert_eq!(&buf[..len], &msg2[..]);

        let msg = Bytes::from("Hello from Peer to new connection!");
        peer_socket.send_to(&msg, driver_addr).await.unwrap();

        let received_msg = conn2.ingress.next().await.unwrap();
        assert_eq!(received_msg, msg);

        assert!(peer_socket.recv_from(&mut buf).now_or_never().is_none());
    }
}
