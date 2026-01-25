use super::testutils::*;
use crate::ops::PeerConnector;
use crate::ops::PeerReporter;
use crate::ops::UtpHandle;
use crate::ops::ctx;
use crate::utils::startup;
use local_async_utils::prelude::*;
use mtorrent_core::msgs;
use mtorrent_core::pwp;
use mtorrent_core::pwp::{BlockInfo, MAX_BLOCK_SIZE};
use mtorrent_utils::peer_id::PeerId;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::rc::Rc;
use std::{fs, panic};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::time::Instant;
use tokio::{join, runtime, task, time, try_join};
use tokio_test::io::Builder as MockBuilder;

async fn connecting_peer_downloading_metadata(remote_ip: SocketAddr, metainfo_path: &'static str) {
    let metainfo = startup::read_metainfo(metainfo_path).unwrap();
    let metainfo_len = metainfo.size();
    let (storage, storage_server) = startup::create_metainfo_storage(metainfo_path).unwrap();

    runtime::Handle::current().spawn(async move {
        storage_server.run().await;
    });

    let mut local_id = [0u8; 20];
    local_id[..4].copy_from_slice("meta".as_bytes());

    let (mut download_chans, mut upload_chans, extended_chans) =
        super::tcp::new_outbound_connection(
            &local_id.into(),
            metainfo.info_hash(),
            true,
            remote_ip,
            0u16,
            &runtime::Handle::current(),
        )
        .await
        .unwrap();
    let extended_chans = extended_chans.unwrap();

    let download_fut = async {
        while let Ok(msg) = download_chans.1.receive_message().await {
            log::info!("Received downloader message: {msg}");
        }
    };
    let upload_fut = async {
        while let Ok(msg) = upload_chans.1.receive_message().await {
            log::info!("Received uploader message: {msg}");
        }
    };
    let meta_fut = async move {
        let pwp::ExtendedChannels(mut tx, mut rx) = extended_chans;
        let local_handshake = Box::new(pwp::ExtendedHandshake {
            extensions: super::ALL_SUPPORTED_EXTENSIONS
                .iter()
                .map(|e| (*e, e.local_id()))
                .collect(),
            ..Default::default()
        });
        tx.send_message((pwp::ExtendedMessage::Handshake(local_handshake), 0))
            .await
            .unwrap();
        let received = rx.receive_message().await.unwrap();
        log::info!("Received initial message {received}");
        match received {
            pwp::ExtendedMessage::Handshake(data) => {
                assert_eq!(
                    super::ALL_SUPPORTED_EXTENSIONS
                        .iter()
                        .map(|e| (*e, e.local_id()))
                        .collect::<HashMap<_, _>>(),
                    data.extensions
                );
                assert!(matches!(data.yourip, Some(IpAddr::V4(Ipv4Addr::LOCALHOST))));
                assert_eq!(metainfo_len, data.metadata_size.unwrap());
            }
            msg => panic!("Unexpected first extended message: {msg}"),
        };
        let received = rx.receive_message().await.unwrap();
        log::info!("Received second message {received}");
        match received {
            pwp::ExtendedMessage::PeerExchange(data) => {
                assert_eq!(1, data.added.len());
                assert_eq!(0, data.dropped.len());
            }
            msg => panic!("Unexpected second extended message: {msg}"),
        };
        let id = pwp::Extension::Metadata.local_id();
        for (piece, offset) in (0..metainfo_len).step_by(MAX_BLOCK_SIZE).enumerate() {
            log::info!("Requesting metadata piece {piece}");
            tx.send_message((pwp::ExtendedMessage::MetadataRequest { piece }, id))
                .await
                .unwrap();
            let received = rx.receive_message().await.unwrap();
            log::info!("Received message {received}");
            match received {
                pwp::ExtendedMessage::MetadataBlock {
                    piece: received_piece,
                    total_size,
                    data,
                } => {
                    assert_eq!(piece, received_piece);
                    assert_eq!(metainfo_len, total_size);
                    assert_eq!(std::cmp::min(MAX_BLOCK_SIZE, metainfo_len - offset), data.len());
                    let expected_data = storage.read_block(offset, data.len()).await.unwrap();
                    assert_eq!(expected_data, data);
                }
                msg => panic!("Unexpected extended message: {msg}"),
            }
        }
    };
    join!(download_fut, upload_fut, meta_fut);
}

async fn run_listening_seeder(
    listener_ip: SocketAddr,
    metainfo_path: &'static str,
    files_dir: &'static str,
) -> io::Result<()> {
    let metainfo = startup::read_metainfo(metainfo_path).unwrap();
    let (content_storage, content_storage_server) =
        startup::create_content_storage(&metainfo, files_dir).unwrap();
    let (meta_storage, meta_storage_server) =
        startup::create_metainfo_storage(metainfo_path).unwrap();
    runtime::Handle::current().spawn(async move {
        join!(content_storage_server.run(), meta_storage_server.run());
    });

    let piece_count = metainfo.pieces().unwrap().count();
    let mut local_id = [0u8; 20];
    local_id[..6].copy_from_slice("seeder".as_bytes());

    let mut handle =
        ctx::MainCtx::new(metainfo, PeerId::from(&local_id), listener_ip, listener_ip.port())
            .unwrap();
    handle.with(|ctx| {
        for piece_index in 0..piece_count {
            assert!(ctx.accountant.submit_piece(piece_index));
            ctx.piece_tracker.forget_piece(piece_index);
        }
    });

    let data = Rc::new(super::MainConnectionData {
        content_storage,
        metainfo_storage: meta_storage,
        ctx_handle: handle,
        pwp_worker_handle: runtime::Handle::current(),
        peer_reporter: PeerReporter::new_mock(),
        piece_downloaded_channel: Rc::new(broadcast::Sender::new(1024)),
        utp_handle: UtpHandle::new_mock(),
    });

    let listener = TcpListener::bind(listener_ip).await.unwrap();
    let (stream, addr) = listener.accept().await.unwrap();
    let deadline = Instant::now() + data.connect_retry_interval();
    let (dl_chan, ul_chan, ext_chan) =
        data.inbound_connect_and_handshake(addr, deadline, stream).await?;
    data.run_connection(pwp::PeerOrigin::Listener, (dl_chan, ul_chan, ext_chan))
        .await?;

    Ok(())
}

#[tokio::test(flavor = "local")]
async fn test_send_metainfo_file_to_peer() {
    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent::ops", log::LevelFilter::Debug)
        .init();

    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 43212));
    let metainfo = "../mtorrent-cli/tests/assets/big_metainfo_file.torrent";
    let data_dir = "test_send_metainfo_file_to_peer";

    task::spawn_local(async move {
        let _ = run_listening_seeder(addr, metainfo, data_dir).await;
    });
    time::timeout(sec!(30), async move {
        task::yield_now().await;
        connecting_peer_downloading_metadata(addr, metainfo).await;
    })
    .await
    .unwrap();

    fs::remove_dir_all(data_dir).unwrap();
}

// ------------------------------------------------------------------------------------------------

async fn pass_torrent_from_peer_to_peer(
    metainfo_filepath: &'static str,
    output_dir: &'static str,
    input_dir: &'static str,
) {
    let ip1 = SocketAddr::new([0, 0, 0, 0].into(), 6666);
    let ip2 = SocketAddr::new([0, 0, 0, 0].into(), 7777);

    let (downloader_sock, uploader_sock) = io::duplex(17 * 1024);

    let (mut downloader_ctx_handle, downloader_fut) = PeerBuilder::new()
        .with_socket(downloader_sock)
        .with_local_ip(ip1)
        .with_remote_ip(ip2)
        .with_metainfo_file(metainfo_filepath)
        .with_content_storage(output_dir)
        .build_main();

    let (_, uploader_fut) = PeerBuilder::new()
        .with_socket(uploader_sock)
        .with_local_ip(ip2)
        .with_remote_ip(ip1)
        .with_all_pieces()
        .with_metainfo_file(metainfo_filepath)
        .with_content_storage(input_dir)
        .build_main();

    task::spawn_local(async move {
        let _ = join!(downloader_fut, uploader_fut);
    });
    time::timeout(sec!(30), async move {
        loop {
            let finished = downloader_ctx_handle.with(|ctx| {
                ctx.accountant.missing_bytes() == 0
                    && ctx.piece_tracker.missing_pieces_rarest_first().count() == 0
            });
            if finished {
                break;
            } else {
                time::sleep(sec!(1)).await;
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test(start_paused = true, flavor = "local")]
async fn test_pass_multifile_torrent_from_peer_to_peer() {
    setup(false);
    let output_dir = "test_pass_multifile_torrent_from_peer_to_peer";
    let metainfo_filepath =
        "../mtorrent-cli/tests/assets/torrents_with_tracker/screenshots.torrent";
    let input_dir = "../mtorrent-cli/tests/assets/screenshots";

    pass_torrent_from_peer_to_peer(metainfo_filepath, output_dir, input_dir).await;
    compare_input_and_output(input_dir, output_dir);
    fs::remove_dir_all(output_dir).unwrap();
}

#[tokio::test(start_paused = true, flavor = "local")]
async fn test_pass_monofile_torrent_from_peer_to_peer() {
    setup(false);
    let output_dir = "test_pass_monofile_torrent_from_peer_to_peer";
    let metainfo_filepath = "../mtorrent-cli/tests/assets/torrents_with_tracker/pcap.torrent";
    let input_dir = "../mtorrent-cli/tests/assets/pcap";

    pass_torrent_from_peer_to_peer(metainfo_filepath, output_dir, input_dir).await;
    compare_input_and_output(input_dir, output_dir);
    fs::remove_dir_all(output_dir).unwrap();
}

// ------------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true, flavor = "local")]
async fn test_pass_metadata_from_peer_to_peer() {
    setup(false);
    let metainfo_filepath = "../mtorrent-cli/tests/assets/big_metainfo_file.torrent";
    let magnet = "magnet:?xt=urn:btih:77c09d63baf907ceeed366e2bdd687ca37cc4098&dn=Star.Trek.Voyager.S05.DVDRip.x264-MARS%5brartv%5d&tr=http%3a%2f%2ftracker.trackerfix.com%3a80%2fannounce&tr=udp%3a%2f%2f9.rarbg.me%3a2750%2fannounce&tr=udp%3a%2f%2f9.rarbg.to%3a2930%2fannounce";

    let metainfo_content = fs::read(metainfo_filepath).unwrap();

    let (downloader_sock, uploader_sock) = io::duplex(17 * 1024);

    let (mut downloader_ctx_handle, downloader_fut) = PeerBuilder::new()
        .with_socket(downloader_sock)
        .with_magnet_link(magnet)
        .build_preliminary();

    let (_, uploader_fut) = PeerBuilder::new()
        .with_socket(uploader_sock)
        .with_metainfo_file(metainfo_filepath)
        .with_extensions()
        .build_main();

    task::spawn_local(uploader_fut);
    time::timeout(sec!(30), async move {
        let _ = downloader_fut.await;
        downloader_ctx_handle.with(|ctx| {
            assert!(!ctx.metainfo_pieces.is_empty());
            assert!(ctx.metainfo_pieces.all());
            assert_eq!(metainfo_content, ctx.metainfo);
        });
    })
    .await
    .unwrap();
}

#[tokio::test(start_paused = true, flavor = "local")]
async fn test_send_extended_handshake_before_bitfield() {
    setup(true);
    let metainfo_filepath =
        "../mtorrent-cli/tests/assets/torrents_with_tracker/screenshots.torrent";
    let local_ip = SocketAddr::new([0, 0, 0, 0].into(), 6666);
    let remote_ip = SocketAddr::new([0, 0, 0, 0].into(), 7777);

    let socket = MockBuilder::new()
        .write(&msgs![pwp::ExtendedMessage::Handshake(Box::new(
            pwp::ExtendedHandshake {
                extensions: super::ALL_SUPPORTED_EXTENSIONS
                    .iter()
                    .map(|e| (*e, e.local_id()))
                    .collect(),
                listen_port: Some(local_ip.port()),
                client_type: Some(super::CLIENT_NAME.to_string()),
                yourip: Some(remote_ip.ip()),
                metadata_size: Some(1724),
                request_limit: Some(super::LOCAL_REQQ),
                ..Default::default()
            }
        ))])
        .write(b"\x00\x00\x00\x0A\x05\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x80") // bitfield
        .build();

    let (_, future) = PeerBuilder::new()
        .with_socket(socket)
        .with_local_ip(local_ip)
        .with_remote_ip(remote_ip)
        .with_metainfo_file(metainfo_filepath)
        .with_extensions()
        .with_all_pieces()
        .build_main();

    let _ = time::timeout(sec!(30), future).await.unwrap();
}

const KEEPALIVE: &[u8] = &[0u8; 4];

#[tokio::test(start_paused = true, flavor = "local")]
async fn test_disconnect_useless_peer_after_5_min() {
    setup(true);

    let mut socket_builder = MockBuilder::new();
    for _min in 0..10 {
        socket_builder.wait(sec!(30)).write(KEEPALIVE).read(KEEPALIVE);
    }
    let socket = socket_builder.wait(sec!(30)).build();

    let (_, future) = PeerBuilder::new().with_socket(socket).build_main();
    let result = time::timeout(min!(5) + sec!(1), future).await.unwrap();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
    assert_eq!(err.to_string(), "peer is useless");
}

#[tokio::test(start_paused = true, flavor = "local")]
async fn test_disconnect_parasite_after_5_min() {
    setup(true);

    let mut socket_builder = MockBuilder::new();
    socket_builder
        .read(&msgs![pwp::UploaderMessage::Have { piece_index: 0 }])
        .write(&msgs![pwp::DownloaderMessage::Interested]);
    for _retry in 0..4 {
        socket_builder
            .wait(sec!(30))
            .write(KEEPALIVE)
            .wait(sec!(30))
            .write(&msgs![
                pwp::DownloaderMessage::NotInterested,
                pwp::DownloaderMessage::Interested,
            ])
            .read(KEEPALIVE);
    }
    socket_builder
        .wait(sec!(30))
        .write(KEEPALIVE)
        .wait(sec!(30))
        .write(&msgs![pwp::DownloaderMessage::NotInterested])
        .read(KEEPALIVE);
    let socket = socket_builder.wait(sec!(30)).build();

    let (_, future) = PeerBuilder::new().with_socket(socket).build_main();
    let result = time::timeout(min!(5) + millisec!(1), future).await.unwrap();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
    assert_eq!(err.to_string(), "peer is useless");
}

#[tokio::test(start_paused = true, flavor = "local")]
async fn test_reevaluate_interest_every_min() {
    setup(true);

    let socket = MockBuilder::new()
        .read(&msgs![pwp::UploaderMessage::Have { piece_index: 0 }])
        .write(&msgs![pwp::DownloaderMessage::Interested])
        .wait(sec!(30))
        .write(KEEPALIVE)
        .wait(sec!(30))
        .write(&msgs![
            pwp::DownloaderMessage::NotInterested,
            pwp::DownloaderMessage::Interested,
        ])
        .read(KEEPALIVE)
        .wait(sec!(30))
        .write(KEEPALIVE)
        .wait(sec!(30))
        .write(&msgs![pwp::DownloaderMessage::NotInterested])
        .read(KEEPALIVE)
        .wait(sec!(30))
        .write(KEEPALIVE)
        .build();

    let (mut ctx, peer_future) = PeerBuilder::new().with_socket(socket).build_main();

    let _ = try_join!(time::timeout(min!(3), peer_future), async move {
        time::sleep(sec!(119)).await;
        ctx.with(|ctx| {
            ctx.piece_tracker.forget_piece(0);
        });
        Ok(())
    })
    .unwrap();
}

#[tokio::test(start_paused = true, flavor = "local")]
async fn test_block_request_timeout_not_affected_by_other_messages() {
    setup(true);
    let metainfo_filepath =
        "../mtorrent-cli/tests/assets/torrents_with_tracker/screenshots.torrent"; // piece length 16K

    let socket = MockBuilder::new()
        .read(&msgs![pwp::UploaderMessage::Have { piece_index: 0 }])
        .write(&msgs![pwp::DownloaderMessage::Interested])
        .read(&msgs![pwp::UploaderMessage::Unchoke])
        .write(&msgs![pwp::DownloaderMessage::Request(BlockInfo {
            piece_index: 0,
            in_piece_offset: 0,
            block_length: pwp::MAX_BLOCK_SIZE
        })])
        .wait(sec!(5))
        .read(&msgs![pwp::UploaderMessage::Have { piece_index: 1 }])
        .wait(sec!(20 - 5))
        .write(&msgs![pwp::DownloaderMessage::Request(BlockInfo {
            piece_index: 0,
            in_piece_offset: 0,
            block_length: pwp::MAX_BLOCK_SIZE
        })])
        .wait(sec!(5))
        .read(&msgs![pwp::UploaderMessage::Have { piece_index: 2 }])
        .wait(sec!(16))
        .build();

    let (_, future) = PeerBuilder::new()
        .with_socket(socket)
        .with_metainfo_file(metainfo_filepath)
        .build_main();

    let result = time::timeout(sec!(40), future).await.unwrap();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other); // i.e. we won't try to reconnect
    assert_eq!(err.to_string(), "peer failed to respond to requests within 20s");
}

#[tokio::test(start_paused = true, flavor = "local")]
async fn test_block_request_extra_retry_when_peer_has_seeded() {
    setup(true);
    let metainfo_filepath =
        "../mtorrent-cli/tests/assets/torrents_with_tracker/screenshots.torrent"; // piece length 16K

    let socket = MockBuilder::new()
        .read(&msgs![pwp::UploaderMessage::Have { piece_index: 0 }])
        .write(&msgs![pwp::DownloaderMessage::Interested])
        .read(&msgs![pwp::UploaderMessage::Unchoke])
        .write(&msgs![pwp::DownloaderMessage::Request(BlockInfo {
            piece_index: 0,
            in_piece_offset: 0,
            block_length: pwp::MAX_BLOCK_SIZE
        })])
        .read(&msgs![pwp::UploaderMessage::Block(
            BlockInfo {
                piece_index: 0,
                in_piece_offset: 0,
                block_length: pwp::MAX_BLOCK_SIZE
            },
            vec![0u8; pwp::MAX_BLOCK_SIZE]
        )])
        .write(&msgs![pwp::UploaderMessage::Have { piece_index: 0 }])
        .read(&msgs![pwp::UploaderMessage::Have { piece_index: 1 }])
        .write(&msgs![pwp::DownloaderMessage::Request(BlockInfo {
            piece_index: 1,
            in_piece_offset: 0,
            block_length: pwp::MAX_BLOCK_SIZE
        })])
        .wait(sec!(20))
        .write(&msgs![pwp::DownloaderMessage::Request(BlockInfo {
            piece_index: 1,
            in_piece_offset: 0,
            block_length: pwp::MAX_BLOCK_SIZE
        })])
        .wait(sec!(20))
        .write(&msgs![pwp::DownloaderMessage::Request(BlockInfo {
            piece_index: 1,
            in_piece_offset: 0,
            block_length: pwp::MAX_BLOCK_SIZE
        })])
        .wait(sec!(20))
        .build();

    let (_, future) = PeerBuilder::new()
        .with_socket(socket)
        .with_metainfo_file(metainfo_filepath)
        .build_main();

    let result = time::timeout(sec!(61), future).await.unwrap();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::TimedOut); // i.e. we will reconnect
    assert_eq!(err.to_string(), "peer failed to respond to requests within 20s");
}

#[tokio::test(start_paused = true, flavor = "local")]
async fn test_respect_peer_reqq() {
    setup(true);
    let metainfo_filepath = "../mtorrent-cli/tests/assets/torrents_with_tracker/pcap.torrent"; // piece size > 16K
    let local_ip = SocketAddr::new([0, 0, 0, 0].into(), 6666);
    let remote_ip = SocketAddr::new([0, 0, 0, 0].into(), 7777);

    for peer_reqq in [0, 1] {
        let (mut sock, farend_sock) = io::duplex(17 * 1024);
        let (_, peer_future) = PeerBuilder::new()
            .with_socket(farend_sock)
            .with_local_ip(local_ip)
            .with_remote_ip(remote_ip)
            .with_metainfo_file(metainfo_filepath)
            .with_extensions()
            .build_main();

        let _ = join!(peer_future, async move {
            let mut buf = vec![0u8; 17 * 1024];

            sock.write_all(&msgs![
                pwp::ExtendedMessage::Handshake(Box::new(pwp::ExtendedHandshake {
                    extensions: super::ALL_SUPPORTED_EXTENSIONS.iter().map(|e| (*e, 0)).collect(),
                    request_limit: Some(peer_reqq),
                    ..Default::default()
                })),
                pwp::UploaderMessage::Have { piece_index: 3 }
            ])
            .await
            .unwrap();

            let bytes_read = sock.read(&mut buf).await.unwrap();
            assert_eq!(
                &buf[..bytes_read],
                &msgs![pwp::ExtendedMessage::Handshake(Box::new(
                    pwp::ExtendedHandshake {
                        extensions: super::ALL_SUPPORTED_EXTENSIONS
                            .iter()
                            .map(|e| (*e, e.local_id()))
                            .collect(),
                        listen_port: Some(local_ip.port()),
                        client_type: Some(super::CLIENT_NAME.to_string()),
                        yourip: Some(remote_ip.ip()),
                        metadata_size: Some(7947),
                        request_limit: Some(super::LOCAL_REQQ),
                        ..Default::default()
                    }
                ))]
            );

            let bytes_read = sock.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..bytes_read], &msgs![pwp::DownloaderMessage::Interested]);

            sock.write_all(&msgs![pwp::UploaderMessage::Unchoke]).await.unwrap();

            let bytes_read = sock.read(&mut buf).await.unwrap();
            assert_eq!(
                &buf[..bytes_read],
                &msgs![pwp::DownloaderMessage::Request(BlockInfo {
                    piece_index: 3,
                    in_piece_offset: 0,
                    block_length: pwp::MAX_BLOCK_SIZE
                })]
            );

            let result = time::timeout(sec!(1), sock.read(&mut buf)).await;
            assert!(matches!(result, Err(_timeout)));
        });
    }
}

#[tokio::test(start_paused = true, flavor = "local")]
async fn test_always_keep_reqq_requests_in_flight() {
    setup(true);
    let metainfo_filepath = "../mtorrent-cli/tests/assets/torrents_with_tracker/pcap.torrent"; // piece size > 16K
    let local_ip = SocketAddr::new([0, 0, 0, 0].into(), 6666);
    let remote_ip = SocketAddr::new([0, 0, 0, 0].into(), 7777);

    let peer_reqq = 2;

    // using duplex as socket because of timeouts
    let (mut sock, farend_sock) = io::duplex(17 * 1024);
    let (_, peer_future) = PeerBuilder::new()
        .with_socket(farend_sock)
        .with_local_ip(local_ip)
        .with_remote_ip(remote_ip)
        .with_metainfo_file(metainfo_filepath)
        .with_extensions()
        .build_main();

    let _ = join!(peer_future, async move {
        let mut buf = vec![0u8; 17 * 1024];

        sock.write_all(&msgs![
            pwp::ExtendedMessage::Handshake(Box::new(pwp::ExtendedHandshake {
                extensions: super::ALL_SUPPORTED_EXTENSIONS.iter().map(|e| (*e, 0)).collect(),
                request_limit: Some(peer_reqq),
                ..Default::default()
            })),
            pwp::UploaderMessage::Have { piece_index: 3 }
        ])
        .await
        .unwrap();

        let bytes_read = time::timeout(sec!(1), sock.read(&mut buf))
            .await
            .expect("timeout")
            .expect("io error");
        assert_eq!(
            &buf[..bytes_read],
            &msgs![pwp::ExtendedMessage::Handshake(Box::new(
                pwp::ExtendedHandshake {
                    extensions: super::ALL_SUPPORTED_EXTENSIONS
                        .iter()
                        .map(|e| (*e, e.local_id()))
                        .collect(),
                    listen_port: Some(local_ip.port()),
                    client_type: Some(super::CLIENT_NAME.to_string()),
                    yourip: Some(remote_ip.ip()),
                    metadata_size: Some(7947),
                    request_limit: Some(super::LOCAL_REQQ),
                    ..Default::default()
                }
            ))]
        );

        let bytes_read = time::timeout(sec!(1), sock.read(&mut buf))
            .await
            .expect("timeout")
            .expect("io error");
        assert_eq!(&buf[..bytes_read], &msgs![pwp::DownloaderMessage::Interested]);

        sock.write_all(&msgs![pwp::UploaderMessage::Unchoke]).await.unwrap();

        // request 1
        let request_1 = BlockInfo {
            piece_index: 3,
            in_piece_offset: 0,
            block_length: pwp::MAX_BLOCK_SIZE,
        };
        let bytes_read = time::timeout(sec!(1), sock.read(&mut buf))
            .await
            .expect("timeout")
            .expect("io error");
        assert_eq!(&buf[..bytes_read], &msgs![pwp::DownloaderMessage::Request(request_1.clone())]);

        // request 2
        let request_2 = BlockInfo {
            piece_index: 3,
            in_piece_offset: pwp::MAX_BLOCK_SIZE,
            block_length: pwp::MAX_BLOCK_SIZE,
        };
        let bytes_read = time::timeout(sec!(1), sock.read(&mut buf))
            .await
            .expect("timeout")
            .expect("io error");
        assert_eq!(&buf[..bytes_read], &msgs![pwp::DownloaderMessage::Request(request_2.clone())]);

        time::timeout(sec!(19), sock.read(&mut buf)).await.expect_err("unexpected msg");

        // when: request 2 is served
        sock.write_all(&msgs![pwp::UploaderMessage::Block(
            request_2,
            vec![0u8; pwp::MAX_BLOCK_SIZE]
        )])
        .await
        .unwrap();

        // then: request 3
        let request_3 = BlockInfo {
            piece_index: 3,
            in_piece_offset: pwp::MAX_BLOCK_SIZE * 2,
            block_length: pwp::MAX_BLOCK_SIZE,
        };
        let bytes_read = time::timeout(sec!(1), sock.read(&mut buf))
            .await
            .expect("timeout")
            .expect("io error");
        assert_eq!(&buf[..bytes_read], &msgs![pwp::DownloaderMessage::Request(request_3.clone())]);

        // when: request 1 and 3 not served within timeout
        time::timeout(sec!(19), sock.read(&mut buf)).await.expect_err("unexpected msg");

        // then: retransmit request 1 and 3
        let mut expected_retransmissions: HashSet<_> = [
            msgs![pwp::DownloaderMessage::Request(request_1)],
            msgs![pwp::DownloaderMessage::Request(request_3)],
        ]
        .into_iter()
        .collect();
        let bytes_read = time::timeout(sec!(2), sock.read(&mut buf))
            .await
            .expect("timeout")
            .expect("io error");
        assert!(expected_retransmissions.remove(&buf[..bytes_read]));
        let bytes_read = time::timeout(sec!(1), sock.read(&mut buf))
            .await
            .expect("timeout")
            .expect("io error");
        assert!(expected_retransmissions.remove(&buf[..bytes_read]));

        time::timeout(sec!(19), sock.read(&mut buf)).await.expect_err("unexpected msg");
    });
}

#[tokio::test(start_paused = true, flavor = "local")]
async fn test_clear_pending_requests_when_peer_chokes() {
    setup(true);
    let metainfo_filepath =
        "../mtorrent-cli/tests/assets/torrents_with_tracker/screenshots.torrent"; // piece length 16K

    let socket = MockBuilder::new()
        .read(&msgs![pwp::UploaderMessage::Have { piece_index: 0 }])
        .write(&msgs![pwp::DownloaderMessage::Interested])
        .read(&msgs![pwp::UploaderMessage::Unchoke])
        .write(&msgs![pwp::DownloaderMessage::Request(BlockInfo {
            piece_index: 0,
            in_piece_offset: 0,
            block_length: pwp::MAX_BLOCK_SIZE
        })])
        .wait(sec!(15))
        .read(&msgs![pwp::UploaderMessage::Choke])
        .wait(sec!(15))
        .write(KEEPALIVE)
        .read(&msgs![pwp::UploaderMessage::Unchoke])
        .write(&msgs![pwp::DownloaderMessage::Request(BlockInfo {
            piece_index: 0,
            in_piece_offset: 0,
            block_length: pwp::MAX_BLOCK_SIZE
        })])
        .build();

    let (mut ctx, peer_future) = PeerBuilder::new()
        .with_socket(socket)
        .with_metainfo_file(metainfo_filepath)
        .build_main();

    let (peer_result, _) = try_join!(time::timeout(sec!(31), peer_future), async move {
        time::sleep(sec!(14)).await;
        ctx.with(|ctx| {
            assert!(ctx.pending_requests.is_piece_requested(0));
        });
        time::sleep(sec!(2)).await;
        ctx.with(|ctx| {
            assert!(!ctx.pending_requests.is_piece_requested(0));
        });
        Ok(())
    })
    .unwrap();
    let error = peer_result.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
}

#[tokio::test(start_paused = true, flavor = "local")]
async fn test_keep_reqq_requests_in_flight_when_rx_is_faster_than_tx() {
    setup(true);
    let metainfo_filepath = "../mtorrent-cli/tests/assets/torrents_with_tracker/pcap.torrent"; // piece size > 16K
    let local_ip = SocketAddr::new([0, 0, 0, 0].into(), 6666);
    let remote_ip = SocketAddr::new([0, 0, 0, 0].into(), 7777);

    let peer_reqq = 2;

    // using duplex as socket because of timeouts
    let (mut sock, farend_sock) = io::duplex(17 * 1024);
    let (_, peer_future) = PeerBuilder::new()
        .with_socket(farend_sock)
        .with_local_ip(local_ip)
        .with_remote_ip(remote_ip)
        .with_metainfo_file(metainfo_filepath)
        .with_extensions()
        .build_main();

    let _ = join!(peer_future, async move {
        let mut buf = vec![0u8; 17 * 1024];

        sock.write_all(&msgs![
            pwp::ExtendedMessage::Handshake(Box::new(pwp::ExtendedHandshake {
                extensions: super::ALL_SUPPORTED_EXTENSIONS.iter().map(|e| (*e, 0)).collect(),
                request_limit: Some(peer_reqq),
                ..Default::default()
            })),
            pwp::UploaderMessage::Have { piece_index: 3 }
        ])
        .await
        .unwrap();

        let bytes_read = time::timeout(sec!(1), sock.read(&mut buf))
            .await
            .expect("timeout")
            .expect("io error");
        assert_eq!(
            &buf[..bytes_read],
            &msgs![pwp::ExtendedMessage::Handshake(Box::new(
                pwp::ExtendedHandshake {
                    extensions: super::ALL_SUPPORTED_EXTENSIONS
                        .iter()
                        .map(|e| (*e, e.local_id()))
                        .collect(),
                    listen_port: Some(local_ip.port()),
                    client_type: Some(super::CLIENT_NAME.to_string()),
                    yourip: Some(remote_ip.ip()),
                    metadata_size: Some(7947),
                    request_limit: Some(super::LOCAL_REQQ),
                    ..Default::default()
                }
            ))]
        );

        let bytes_read = time::timeout(sec!(1), sock.read(&mut buf))
            .await
            .expect("timeout")
            .expect("io error");
        assert_eq!(&buf[..bytes_read], &msgs![pwp::DownloaderMessage::Interested]);

        sock.write_all(&msgs![pwp::UploaderMessage::Unchoke]).await.unwrap();

        // request 1
        let request_1 = BlockInfo {
            piece_index: 3,
            in_piece_offset: 0,
            block_length: pwp::MAX_BLOCK_SIZE,
        };
        let bytes_read = time::timeout(sec!(1), sock.read(&mut buf))
            .await
            .expect("timeout")
            .expect("io error");
        assert_eq!(&buf[..bytes_read], &msgs![pwp::DownloaderMessage::Request(request_1.clone())]);

        // when: request 1 is served
        sock.write_all(&msgs![pwp::UploaderMessage::Block(
            request_1,
            vec![0u8; pwp::MAX_BLOCK_SIZE]
        )])
        .await
        .unwrap();

        // then: request 2 and 3
        let request_2 = BlockInfo {
            piece_index: 3,
            in_piece_offset: pwp::MAX_BLOCK_SIZE,
            block_length: pwp::MAX_BLOCK_SIZE,
        };
        let bytes_read = time::timeout(sec!(1), sock.read(&mut buf))
            .await
            .expect("timeout")
            .expect("io error");
        assert_eq!(&buf[..bytes_read], &msgs![pwp::DownloaderMessage::Request(request_2.clone())]);

        let request_3 = BlockInfo {
            piece_index: 3,
            in_piece_offset: pwp::MAX_BLOCK_SIZE * 2,
            block_length: pwp::MAX_BLOCK_SIZE,
        };
        let bytes_read = time::timeout(sec!(1), sock.read(&mut buf))
            .await
            .expect("timeout")
            .expect("io error");
        assert_eq!(&buf[..bytes_read], &msgs![pwp::DownloaderMessage::Request(request_3.clone())]);

        // when: request 2 and 3 not served within timeout
        time::timeout(sec!(19), sock.read(&mut buf)).await.expect_err("unexpected msg");

        // then: retransmit request 2 and 3
        let mut expected_retransmissions: HashSet<_> = [
            msgs![pwp::DownloaderMessage::Request(request_2)],
            msgs![pwp::DownloaderMessage::Request(request_3)],
        ]
        .into_iter()
        .collect();
        let bytes_read = time::timeout(sec!(2), sock.read(&mut buf))
            .await
            .expect("timeout")
            .expect("io error");
        assert!(expected_retransmissions.remove(&buf[..bytes_read]));
        let bytes_read = time::timeout(sec!(1), sock.read(&mut buf))
            .await
            .expect("timeout")
            .expect("io error");
        assert!(expected_retransmissions.remove(&buf[..bytes_read]));

        time::timeout(sec!(19), sock.read(&mut buf)).await.expect_err("unexpected msg");
    });
}
