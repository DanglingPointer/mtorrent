use super::protocol::{Header, TypeVer};
use super::seq::seq;
use super::*;
use futures_util::StreamExt;
use std::net::Ipv4Addr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UdpSocket;
use tokio::{join, task};

#[tokio::test(flavor = "local")]
async fn test_exchange_data_between_2_peers() {
    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent_core::utp", log::LevelFilter::Trace)
        .init();

    let socket1 = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
    let addr1 = socket1.local_addr().unwrap();

    let socket2 = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
    let addr2 = socket2.local_addr().unwrap();

    let (spawner1, _reporter1, driver) = init(socket1);
    task::spawn_local(driver.run());

    let (spawner2, mut reporter2, driver) = init(socket2);
    task::spawn_local(driver.run());

    let outbound_fut = async move {
        let mut pipe = spawner1.connect_to(addr2).await.unwrap();

        pipe.write_all(b"hello from peer 1").await.unwrap();

        let mut buf = [0u8; 17];
        let n = pipe.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello from peer 2");

        pipe.write_all(b"Bye from peer 1").await.unwrap();

        let mut buf = [0u8; 128];
        let n = pipe.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"Bye from peer 2");

        task::yield_now().await; // let IoDriver finish sending packets
    };

    let inbound_fut = async move {
        let (remote_addr, data) = reporter2.next().await.unwrap();
        assert_eq!(remote_addr, addr1);
        let mut pipe = spawner2.accept_from(remote_addr, data).await.unwrap();

        pipe.write_all(b"hello from peer 2").await.unwrap();

        let mut buf = [0u8; 17];
        let n = pipe.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello from peer 1");

        pipe.write_all(b"Bye from peer 2").await.unwrap();

        let mut buf = [0u8; 128];
        let n = pipe.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"Bye from peer 1");

        task::yield_now().await; // let IoDriver finish sending packets
    };

    join!(outbound_fut, inbound_fut);
    task::yield_now().await; // let IoDriver finish sending packets
}

#[cfg_attr(not(target_os = "linux"), ignore)]
#[tokio::test(flavor = "local")]
async fn test_outbound_connection_timeout() {
    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent_core::utp", log::LevelFilter::Trace)
        .init();

    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();

    let (spawner, _, driver) = init(socket);
    task::spawn_local(driver.run());

    // connect to unreachable address
    let Err(error) = spawner.connect_to((Ipv4Addr::LOCALHOST, 0u16).into()).await else {
        panic!("expected connection to timeout");
    };
    assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);

    drop(spawner);
    task::yield_now().await; // let IoDriver exit gracefully
}

#[tokio::test(flavor = "local")]
async fn test_outbound_syn_doesnt_change_across_reconnects() {
    protocol::FAKE_CURRENT_TIMESTAMP_US.set(Some(42)); // constant timestamp to compare headers

    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
    let local_addr = socket.local_addr().unwrap();

    let peer_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
    let peer_addr = peer_socket.local_addr().unwrap();

    let (spawner, _reporter1, demux) = init(socket);
    task::spawn_local(demux.run());

    // start connecting
    let spawner_copy = spawner.clone();
    let connect_handle = task::spawn_local(async move { spawner_copy.connect_to(peer_addr).await });

    // receive SYN
    let mut buf = [0u8; 1024];
    let (len, addr) = peer_socket.recv_from(&mut buf).await.unwrap();
    assert_eq!(addr, local_addr);
    let syn_hdr = Header::decode_from(&mut &buf[..len]).unwrap();
    assert_eq!(syn_hdr.type_ver, TypeVer::Syn);

    // cancel and reconnect
    connect_handle.abort();
    task::yield_now().await;
    let spawner_copy = spawner.clone();
    let _connect_handle =
        task::spawn_local(async move { spawner_copy.connect_to(peer_addr).await });

    // receive new SYN
    let mut buf = [0u8; 1024];
    let (len, addr) = peer_socket.recv_from(&mut buf).await.unwrap();
    assert_eq!(addr, local_addr);
    let new_syn_hdr = Header::decode_from(&mut &buf[..len]).unwrap();
    assert_eq!(new_syn_hdr, syn_hdr);
}

#[tokio::test(flavor = "local")]
async fn test_pipe_data_from_one_peer_to_another() {
    // let _ = simple_logger::SimpleLogger::new()
    //     .with_level(log::LevelFilter::Off)
    //     .with_module_level("mtorrent_core::utp::connection", log::LevelFilter::Trace)
    //     .init();

    let socket1 = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
    let addr1 = socket1.local_addr().unwrap();

    let socket2 = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
    let addr2 = socket2.local_addr().unwrap();

    let (spawner1, _reporter1, driver1) = init(socket1);
    task::spawn_local(driver1.run());

    let (spawner2, mut reporter2, driver2) = init(socket2);
    task::spawn_local(driver2.run());

    const CHUNK_SIZE: usize = 8 * 1024;
    const CHUNK_COUNT: usize = 64 * 1024;

    let writer_fut = async {
        let mut pipe = spawner1.connect_to(addr2).await.unwrap();

        for _ in 0..CHUNK_COUNT {
            let data = [b'm'; CHUNK_SIZE];
            pipe.write_all(&data).await.unwrap();
        }

        // don't drop the pipe, let the reader read all data first
        pipe
    };

    let reader_fut = async move {
        let (remote_addr, data) = reporter2.next().await.unwrap();
        assert_eq!(remote_addr, addr1);
        let mut pipe = spawner2.accept_from(remote_addr, data).await.unwrap();

        let mut total_bytes = 0;
        let mut buf = [0u8; CHUNK_SIZE];
        while total_bytes < CHUNK_COUNT * CHUNK_SIZE {
            let n = pipe.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], &[b'm'; CHUNK_SIZE][..n]);
            total_bytes += n;
        }
    };

    let (_writer_pipe, ()) = join!(writer_fut, reader_fut);
    task::yield_now().await;
}

#[tokio::test(flavor = "local")]
async fn test_reconnect_after_local_disconnect() {
    let _ = simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Trace).init();

    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
    let local_addr = socket.local_addr().unwrap();

    let peer_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16)).await.unwrap();
    let peer_addr = peer_socket.local_addr().unwrap();

    let (spawner, _reporter1, demux) = init(socket);
    task::spawn_local(demux.run());

    let inbound = async move {
        // receive SYN
        let mut send_buf = [0u8; Header::MIN_SIZE];
        let mut recv_buf = [0u8; 1024 * 32];

        let (len, addr) = peer_socket.recv_from(&mut recv_buf).await.unwrap();
        assert_eq!(addr, local_addr);
        let syn_hdr = Header::decode_from(&mut &recv_buf[..len]).unwrap();
        assert_eq!(syn_hdr.type_ver, TypeVer::Syn);

        // send STATE
        let state_hdr = Header {
            type_ver: TypeVer::State,
            extension: 0,
            connection_id: syn_hdr.connection_id,
            timestamp_us: 42,
            timestamp_diff_us: 0,
            wnd_size: 0,
            seq_nr: seq(0),
            ack_nr: seq(0),
        };
        state_hdr.encode_to(&mut &mut send_buf[..]).unwrap();
        peer_socket.send_to(&send_buf, local_addr).await.unwrap();

        // receive 1 DATA packet
        let (len, addr) = peer_socket.recv_from(&mut recv_buf).await.unwrap();
        assert_eq!(addr, local_addr);
        assert!(len >= 1472);
        let data_hdr = Header::decode_from(&mut &recv_buf[..len]).unwrap();
        assert_eq!(data_hdr.type_ver, TypeVer::Data);

        // receive RESET
        let (len, addr) = peer_socket.recv_from(&mut recv_buf).await.unwrap();
        assert_eq!(addr, local_addr);
        let fin_hdr = Header::decode_from(&mut &recv_buf[..len]).unwrap();
        assert_eq!(fin_hdr.type_ver, TypeVer::Reset);

        // receive new SYN
        let (len, addr) = peer_socket.recv_from(&mut recv_buf).await.unwrap();
        assert_eq!(addr, local_addr);
        let syn_hdr = Header::decode_from(&mut &recv_buf[..len]).unwrap();
        assert_eq!(syn_hdr.type_ver, TypeVer::Syn);

        // send new STATE
        let state_hdr = Header {
            type_ver: TypeVer::State,
            extension: 0,
            connection_id: syn_hdr.connection_id,
            timestamp_us: 42,
            timestamp_diff_us: 0,
            wnd_size: 0,
            seq_nr: seq(0),
            ack_nr: seq(0),
        };
        state_hdr.encode_to(&mut &mut send_buf[..]).unwrap();
        peer_socket.send_to(&send_buf, local_addr).await.unwrap();
    };

    let outbound = async move {
        // connect
        let mut pipe = spawner.connect_to(peer_addr).await.unwrap();

        // write data bigger than window
        let data = [b'x'; 16 * 1024];
        pipe.write_all(&data).await.unwrap();

        // drop connection and reconnect
        drop(pipe);
        task::yield_now().await;
        let _pipe = spawner.connect_to(peer_addr).await.unwrap();
    };

    join!(inbound, outbound);
}
