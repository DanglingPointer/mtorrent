use crate::pwp::MAX_BLOCK_SIZE;
use crate::utils::peer_id::PeerId;
use crate::utils::{fifo, magnet, startup};
use crate::{ops::ctx, pwp, sec};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::Path;
use std::{fs, iter};
use tokio::net::TcpStream;
use tokio::{io, task};
use tokio::{join, net::TcpListener, runtime, time};

async fn connecting_peer_downloading_metadata(remote_ip: SocketAddr, metainfo_path: &'static str) {
    let metainfo = startup::read_metainfo(metainfo_path).unwrap();
    let metainfo_len = metainfo.size();
    let (storage, storage_server) = startup::create_metainfo_storage(metainfo_path).unwrap();

    runtime::Handle::current().spawn(async move {
        storage_server.run().await;
    });

    let mut local_id = [0u8; 20];
    local_id[..4].copy_from_slice("meta".as_bytes());

    let stream = TcpStream::connect(remote_ip).await.unwrap();
    stream.set_nodelay(true).unwrap();
    let (mut download_chans, mut upload_chans, extended_chans, runner) =
        pwp::channels_from_outgoing(&local_id, metainfo.info_hash(), true, remote_ip, stream, None)
            .await
            .unwrap();
    let extended_chans = extended_chans.unwrap();

    runtime::Handle::current().spawn(async move {
        let _ = runner.await;
    });

    let download_fut = async {
        while let Ok(msg) = download_chans.1.receive_message().await {
            log::info!("Received downloader message: {}", msg);
        }
    };
    let upload_fut = async {
        while let Ok(msg) = upload_chans.1.receive_message().await {
            log::info!("Received uploader message: {}", msg);
        }
    };
    let meta_fut = async move {
        let pwp::ExtendedChannels(mut tx, mut rx) = extended_chans;
        let local_handshake = Box::new(pwp::HandshakeData {
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

    let mut handle = ctx::MainCtx::new(metainfo, PeerId::from(&local_id), listener_ip).unwrap();
    handle.with_ctx(|ctx| {
        for piece_index in 0..piece_count {
            assert!(ctx.accountant.submit_piece(piece_index));
            ctx.piece_tracker.forget_piece(piece_index);
        }
    });
    let (sink, _src) = fifo::channel();
    let (_outgoing_ctrl, mut incoming_ctrl) = super::super::connection_control(
        50,
        super::MainConnectionData {
            content_storage,
            metainfo_storage: meta_storage,
            ctx_handle: handle,
            pwp_worker_handle: runtime::Handle::current(),
            peer_discovered_channel: sink,
        },
    );

    let listener = TcpListener::bind(listener_ip).await.unwrap();
    let (stream, peer_ip) = listener.accept().await.unwrap();
    super::incoming_pwp_connection(stream, peer_ip, incoming_ctrl.issue_permit().unwrap()).await?;

    Ok(())
}

#[tokio::test]
async fn test_send_metainfo_file_to_peer() {
    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent::ops", log::LevelFilter::Debug)
        .init();

    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 43212));
    let metainfo = "tests/assets/big_metainfo_file.torrent";
    let data_dir = "test_send_metainfo_file_to_peer";

    let tasks = task::LocalSet::new();
    tasks.spawn_local(async move {
        let _ = run_listening_seeder(addr, metainfo, data_dir).await;
    });
    tasks
        .run_until(time::timeout(sec!(30), async move {
            task::yield_now().await;
            connecting_peer_downloading_metadata(addr, metainfo).await;
        }))
        .await
        .unwrap();

    fs::remove_dir_all(data_dir).unwrap();
}

// ------------------------------------------------------------------------------------------------

fn compare_input_and_output(
    input_dir: impl AsRef<Path> + Debug,
    output_dir: impl AsRef<Path> + Debug,
) {
    assert!(output_dir.as_ref().is_dir());
    assert!(input_dir.as_ref().is_dir());
    println!("comparing {input_dir:?} and {output_dir:?}");

    fn verify_files_identical(p1: impl AsRef<Path>, p2: impl AsRef<Path>) {
        let name1 = p1.as_ref().to_string_lossy().into_owned();
        let name2 = p2.as_ref().to_string_lossy().into_owned();
        let mut f1 = fs::File::open(p1).unwrap();
        let mut f2 = fs::File::open(p2).unwrap();
        let buff1: &mut [u8] = &mut [0; 1024];
        let buff2: &mut [u8] = &mut [0; 1024];
        loop {
            match (f1.read(buff1), f2.read(buff2)) {
                (Ok(f1_read_len), Ok(f2_read_len)) => {
                    if f1_read_len != f2_read_len {
                        panic!("{} and {} are different", name1, name2);
                    }
                    if f1_read_len == 0 {
                        break;
                    }
                    if buff1[0..f1_read_len] != buff2[0..f2_read_len] {
                        panic!("{} and {} are different", name1, name2);
                    }
                }
                _ => panic!(),
            }
        }
    }
    for (input, output) in
        iter::zip(fs::read_dir(input_dir).unwrap(), fs::read_dir(output_dir).unwrap())
    {
        let input_path = input.unwrap().path();
        let output_path = output.unwrap().path();

        if input_path.is_dir() && output_path.is_dir() {
            compare_input_and_output(input_path, output_path);
        } else if input_path.is_file() && output_path.is_file() {
            verify_files_identical(input_path, output_path);
        } else {
            panic!();
        }
    }
}

async fn pass_torrent_from_peer_to_peer(
    metainfo_filepath: &'static str,
    output_dir: &'static str,
    input_dir: &'static str,
) {
    let downloader_peer_id = [b'd'; 20];
    let downloader_ip = SocketAddr::new([0, 0, 0, 0].into(), 6666);
    let uploader_peer_id = [b'u'; 20];
    let uploader_ip = SocketAddr::new([0, 0, 0, 0].into(), 7777);

    let (downloader_sock, uploader_sock) = io::duplex(17 * 1024);

    let downloader_task = async move {
        let metainfo = startup::read_metainfo(metainfo_filepath).unwrap();
        let (content_storage, content_storage_server) =
            startup::create_content_storage(&metainfo, output_dir).unwrap();
        task::spawn(async move {
            content_storage_server.run().await;
        });

        let remote_hs = pwp::Handshake {
            peer_id: uploader_peer_id,
            info_hash: *metainfo.info_hash(),
            ..Default::default()
        };
        let (dlchans, ulchans, extchans) =
            pwp::channels_from_mock(uploader_ip, remote_hs, false, downloader_sock);
        let mut ctx_handle =
            ctx::MainCtx::new(metainfo, PeerId::from(&downloader_peer_id), downloader_ip).unwrap();
        let (sink, _src) = fifo::channel();
        let run_future = super::run_peer_connection(
            dlchans,
            ulchans,
            extchans,
            content_storage.clone(),
            content_storage, // metainfo storage (hack)
            ctx_handle.clone(),
            sink,
        );
        task::spawn_local(async move {
            let _ = run_future.await;
        });

        loop {
            let finished = ctx_handle.with_ctx(|ctx| {
                ctx.accountant.missing_bytes() == 0
                    && ctx.piece_tracker.get_rarest_pieces().count() == 0
            });
            if finished {
                break;
            } else {
                time::sleep(sec!(1)).await;
            }
        }
    };

    let uploader_task = async move {
        let metainfo = startup::read_metainfo(metainfo_filepath).unwrap();
        let (content_storage, content_storage_server) =
            startup::create_content_storage(&metainfo, input_dir).unwrap();
        task::spawn(async move {
            content_storage_server.run().await;
        });

        let remote_hs = pwp::Handshake {
            peer_id: downloader_peer_id,
            info_hash: *metainfo.info_hash(),
            ..Default::default()
        };
        let (dlchans, ulchans, extchans) =
            pwp::channels_from_mock(downloader_ip, remote_hs, false, uploader_sock);

        let mut ctx_handle =
            ctx::MainCtx::new(metainfo, PeerId::from(&downloader_peer_id), downloader_ip).unwrap();
        ctx_handle.with_ctx(|ctx| {
            let piece_count = ctx.metainfo.pieces().unwrap().count();
            for piece_index in 0..piece_count {
                assert!(ctx.accountant.submit_piece(piece_index));
                ctx.piece_tracker.forget_piece(piece_index);
            }
        });

        let (sink, _src) = fifo::channel();
        let _ = super::run_peer_connection(
            dlchans,
            ulchans,
            extchans,
            content_storage.clone(),
            content_storage, // metainfo storage (hack)
            ctx_handle,
            sink,
        )
        .await;
    };

    let tasks = task::LocalSet::new();
    tasks.spawn_local(uploader_task);
    tasks.run_until(time::timeout(sec!(30), downloader_task)).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn test_pass_multifile_torrent_from_peer_to_peer() {
    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent::ops", log::LevelFilter::Debug)
        .init();

    let output_dir = "test_pass_multifile_torrent_from_peer_to_peer";
    let metainfo_filepath = "tests/assets/screenshots.torrent";
    let input_dir = "tests/assets/screenshots";

    pass_torrent_from_peer_to_peer(metainfo_filepath, output_dir, input_dir).await;
    compare_input_and_output(input_dir, output_dir);
    fs::remove_dir_all(output_dir).unwrap();
}

#[tokio::test(start_paused = true)]
async fn test_pass_monofile_torrent_from_peer_to_peer() {
    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent::ops", log::LevelFilter::Debug)
        .init();

    let output_dir = "test_pass_monofile_torrent_from_peer_to_peer";
    let metainfo_filepath = "tests/assets/pcap.torrent";
    let input_dir = "tests/assets/pcap";

    pass_torrent_from_peer_to_peer(metainfo_filepath, output_dir, input_dir).await;
    compare_input_and_output(input_dir, output_dir);
    fs::remove_dir_all(output_dir).unwrap();
}

#[tokio::test(start_paused = true)]
async fn test_pass_metadata_from_peer_to_peer() {
    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent::ops", log::LevelFilter::Debug)
        .init();

    let metainfo_filepath = "tests/assets/big_metainfo_file.torrent";
    let magnet = "magnet:?xt=urn:btih:77c09d63baf907ceeed366e2bdd687ca37cc4098&dn=Star.Trek.Voyager.S05.DVDRip.x264-MARS%5brartv%5d&tr=http%3a%2f%2ftracker.trackerfix.com%3a80%2fannounce&tr=udp%3a%2f%2f9.rarbg.me%3a2750%2fannounce&tr=udp%3a%2f%2f9.rarbg.to%3a2930%2fannounce";

    let metainfo_content = fs::read(metainfo_filepath).unwrap();

    let downloader_peer_id = [b'd'; 20];
    let downloader_ip = SocketAddr::new([0, 0, 0, 0].into(), 6666);
    let uploader_peer_id = [b'u'; 20];
    let uploader_ip = SocketAddr::new([0, 0, 0, 0].into(), 7777);

    let (downloader_sock, uploader_sock) = io::duplex(17 * 1024);

    let downloader_task = async move {
        let magnet_link: magnet::MagnetLink = magnet.parse().unwrap();

        let remote_hs = pwp::Handshake {
            peer_id: uploader_peer_id,
            info_hash: *magnet_link.info_hash(),
            reserved: pwp::reserved_bits(true),
        };
        let (dlchans, ulchans, extchans) =
            pwp::channels_from_mock(uploader_ip, remote_hs, true, downloader_sock);
        let mut ctx_handle =
            ctx::PreliminaryCtx::new(magnet_link, PeerId::from(&downloader_peer_id), downloader_ip);
        let _ = super::run_metadata_download(dlchans, ulchans, extchans, ctx_handle.clone()).await;

        ctx_handle.with_ctx(|ctx| {
            assert!(!ctx.metainfo_pieces.is_empty());
            assert!(ctx.metainfo_pieces.all());
            assert_eq!(metainfo_content, ctx.metainfo);
        });
    };

    let uploader_task = async move {
        let metainfo = startup::read_metainfo(metainfo_filepath).unwrap();

        let (metainfo_storage, metainfo_storage_server) =
            startup::create_metainfo_storage(metainfo_filepath).unwrap();
        task::spawn(async move {
            metainfo_storage_server.run().await;
        });

        let remote_hs = pwp::Handshake {
            peer_id: downloader_peer_id,
            info_hash: *metainfo.info_hash(),
            reserved: pwp::reserved_bits(true),
        };
        let (dlchans, ulchans, extchans) =
            pwp::channels_from_mock(downloader_ip, remote_hs, true, uploader_sock);
        let ctx_handle =
            ctx::MainCtx::new(metainfo, PeerId::from(&uploader_peer_id), uploader_ip).unwrap();
        let (sink, _src) = fifo::channel();
        let _ = super::run_peer_connection(
            dlchans,
            ulchans,
            extchans,
            metainfo_storage.clone(), // content storage (hack)
            metainfo_storage,
            ctx_handle,
            sink,
        )
        .await;
    };

    let tasks = task::LocalSet::new();
    tasks.spawn_local(uploader_task);
    tasks.run_until(time::timeout(sec!(30), downloader_task)).await.unwrap();
}
