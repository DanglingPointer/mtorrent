use super::*;
use crate::ops::MAX_BLOCK_SIZE;
use crate::utils::peer_id::PeerId;
use crate::utils::{magnet, startup};
use crate::{ops::ctx, pwp, sec};
use std::collections::HashMap;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;
use tokio::{join, net::TcpListener, runtime, select, time};

const PIECE_LENGTH: usize = 2_097_152;
const LAST_PIECE_LENGTH: usize = 1_160_807;
const PIECE_COUNT: usize = 668;
const TOTAL_BYTES: usize = PIECE_LENGTH * (PIECE_COUNT - 1) + LAST_PIECE_LENGTH;

const START_PIECE_INDEX: usize = 600;
const EXPECTED_BYTES_PASSED: usize =
    PIECE_LENGTH * (PIECE_COUNT - START_PIECE_INDEX - 1) + LAST_PIECE_LENGTH;

type CtxHandle = ctx::Handle<ctx::MainCtx>;

async fn connecting_peer(
    peer_ip: SocketAddr,
    metainfo_path: &'static str,
    files_dir: &'static str,
) -> (download::IdlePeer, upload::IdlePeer, CtxHandle, SocketAddr) {
    let metainfo = startup::read_metainfo(metainfo_path).unwrap();
    let (storage, storage_server) = startup::create_content_storage(&metainfo, files_dir).unwrap();
    runtime::Handle::current().spawn(async move {
        storage_server.run().await;
    });

    let mut local_id = [0u8; 20];
    local_id[..5].copy_from_slice("leech".as_bytes());

    let handle = ctx::MainCtx::new(metainfo, PeerId::from(&local_id)).unwrap();

    let (download, upload, _) = from_outgoing_connection(
        peer_ip,
        storage.clone(),
        storage, // hack
        handle.clone(),
        runtime::Handle::current(),
        false, // extension protocol
    )
    .await
    .unwrap();
    (download, upload, handle, peer_ip)
}

async fn connecting_peer_downloading_metadata(remote_ip: SocketAddr, metainfo_path: &'static str) {
    let metainfo = startup::read_metainfo(metainfo_path).unwrap();
    let metainfo_len = metainfo.size();
    let (storage, storage_server) = startup::create_metainfo_storage(metainfo_path).unwrap();

    runtime::Handle::current().spawn(async move {
        storage_server.run().await;
    });

    let mut local_id = [0u8; 20];
    local_id[..4].copy_from_slice("meta".as_bytes());

    let (mut download_chans, mut upload_chans, extended_chans, runner) =
        pwp::channels_from_outgoing(&local_id, metainfo.info_hash(), true, remote_ip, None)
            .await
            .unwrap();
    let extended_chans = extended_chans.unwrap();

    runtime::Handle::current().spawn(async move {
        let _ = runner.run().await;
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
            extensions: ALL_SUPPORTED_EXTENSIONS.iter().map(|e| (*e, e.local_id())).collect(),
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
                    ALL_SUPPORTED_EXTENSIONS
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

async fn listening_seeder(
    listener_ip: SocketAddr,
    metainfo_path: &'static str,
    files_dir: &'static str,
) -> (download::IdlePeer, upload::IdlePeer, Option<extensions::Peer>, CtxHandle, SocketAddr) {
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

    let mut handle = ctx::MainCtx::new(metainfo, PeerId::from(&local_id)).unwrap();
    handle.with_ctx(|ctx| {
        for piece_index in 0..piece_count {
            assert!(ctx.accountant.submit_piece(piece_index));
            ctx.piece_tracker.forget_piece(piece_index);
        }
    });

    let listener = TcpListener::bind(listener_ip).await.unwrap();
    let (stream, peer_ip) = listener.accept().await.unwrap();
    let (download, upload, extensions) = from_incoming_connection(
        stream,
        content_storage,
        meta_storage,
        handle.clone(),
        runtime::Handle::current(),
        true, // extension protocol
    )
    .await
    .unwrap();
    (download, upload, extensions, handle, peer_ip)
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

    let mut handle = ctx::MainCtx::new(metainfo, PeerId::from(&local_id)).unwrap();
    handle.with_ctx(|ctx| {
        for piece_index in 0..piece_count {
            assert!(ctx.accountant.submit_piece(piece_index));
            ctx.piece_tracker.forget_piece(piece_index);
        }
    });

    let listener = TcpListener::bind(listener_ip).await.unwrap();
    let (stream, _peer_ip) = listener.accept().await.unwrap();
    incoming_pwp_connection(
        stream,
        content_storage,
        meta_storage,
        handle,
        runtime::Handle::current(),
        |_| (),
    )
    .await?;

    Ok(())
}

async fn run_leech(peer_ip: SocketAddr, metainfo_path: &'static str) {
    let files_dir = "test_output";
    let (download, upload, mut handle, _ip) =
        connecting_peer(peer_ip, metainfo_path, files_dir).await;
    define_with_ctx!(handle);

    let piece_count = with_ctx!(|ctx| ctx.metainfo.pieces().unwrap().count());

    handle.with_ctx(|ctx| {
        assert_eq!(PIECE_COUNT, ctx.piece_tracker.get_rarest_pieces().count());
        assert_eq!(TOTAL_BYTES, ctx.accountant.missing_bytes());
        assert_eq!(0, ctx.accountant.accounted_bytes());
        assert_eq!(PIECE_COUNT, ctx.piece_tracker.get_peer_pieces(&peer_ip).unwrap().count());
        assert_eq!(0, ctx.peer_states.leeches_count());
        assert_eq!(0, ctx.peer_states.seeders_count());
        let state = ctx.peer_states.get(&peer_ip).unwrap();
        assert_eq!(pwp::DownloadState::default(), state.download);
        assert_eq!(pwp::UploadState::default(), state.upload);
    });

    let mut uh = handle.clone();
    let upload_fut = async move {
        let mut peer = upload;
        while let upload::Peer::Idle(farend) = upload::linger(peer, Duration::MAX).await.unwrap() {
            peer = farend;
            let ustate = uh.with_ctx(|ctx| ctx.peer_states.get(&peer_ip).unwrap().upload.clone());
            assert_eq!(pwp::UploadState::default(), ustate);
        }
    };
    let mut dh = handle.clone();
    let download_fut = async move {
        let seeder = download::activate(download).await.unwrap();
        let dstate = dh.with_ctx(|ctx| ctx.peer_states.get(&peer_ip).unwrap().download.clone());
        assert!(dstate.am_interested);
        assert!(!dstate.peer_choking);
        assert_eq!(0, dstate.bytes_received);

        let peer = download::get_pieces(
            seeder,
            (START_PIECE_INDEX..piece_count).collect::<Vec<_>>().into_iter(),
        )
        .await
        .unwrap();
        if let download::Peer::Seeder(seeder) = peer {
            let dstate = dh.with_ctx(|ctx| ctx.peer_states.get(&peer_ip).unwrap().download.clone());
            assert!(dstate.am_interested);
            assert!(!dstate.peer_choking);
            assert_eq!(EXPECTED_BYTES_PASSED, dstate.bytes_received);
            dh.with_ctx(|ctx| {
                assert_eq!(EXPECTED_BYTES_PASSED, ctx.accountant.accounted_bytes());
                assert_eq!(TOTAL_BYTES - EXPECTED_BYTES_PASSED, ctx.accountant.missing_bytes());
            });

            let _peer = download::deactivate(seeder).await.unwrap();
            let dstate = dh.with_ctx(|ctx| ctx.peer_states.get(&peer_ip).unwrap().download.clone());
            assert!(!dstate.am_interested);
            assert!(!dstate.peer_choking);
            assert_eq!(EXPECTED_BYTES_PASSED, dstate.bytes_received);
        } else {
            panic!("Seeder choked prematurely");
        }
    };
    select! {
        biased;
        _ = download_fut => (),
        _ = upload_fut => (),
    }

    handle.with_ctx(|ctx| {
        assert_eq!(TOTAL_BYTES - EXPECTED_BYTES_PASSED, ctx.accountant.missing_bytes());
        assert_eq!(EXPECTED_BYTES_PASSED, ctx.accountant.accounted_bytes());
        assert_eq!(0, ctx.piece_tracker.get_rarest_pieces().count()); // peer disconnected
        assert!(ctx.piece_tracker.get_peer_pieces(&peer_ip).is_none());
        assert!(ctx.peer_states.get(&peer_ip).is_none());
        assert_eq!(0, ctx.peer_states.leeches_count());
        assert_eq!(0, ctx.peer_states.seeders_count());
    });

    std::fs::remove_dir_all(files_dir).unwrap();
}

async fn run_seeder(listener_ip: SocketAddr, metainfo_path: &'static str) {
    let files_dir = "test_input";
    let (download, upload, extensions, mut handle, peer_ip) =
        listening_seeder(listener_ip, metainfo_path, files_dir).await;
    assert!(extensions.is_none());

    handle.with_ctx(|ctx| {
        assert_eq!(0, ctx.piece_tracker.get_poorest_peers().count());
        assert_eq!(0, ctx.peer_states.leeches_count());
        assert_eq!(0, ctx.peer_states.seeders_count());
        let state = ctx.peer_states.get(&peer_ip).unwrap();
        assert_eq!(pwp::DownloadState::default(), state.download);
        assert_eq!(pwp::UploadState::default(), state.upload);
    });

    let mut dh = handle.clone();
    let download_fut = async move {
        let mut peer = download::Peer::Idle(download);
        while let Ok(farend) = download::linger(peer, Duration::MAX).await {
            peer = farend;
            let dstate = dh.with_ctx(|ctx| ctx.peer_states.get(&peer_ip).unwrap().download.clone());
            assert_eq!(pwp::DownloadState::default(), dstate);
        }
    };
    let mut uh = handle.clone();
    let upload_fut = async move {
        let downloader = upload::activate(upload).await.unwrap();
        let ustate = uh.with_ctx(|ctx| ctx.peer_states.get(&peer_ip).unwrap().upload.clone());
        assert!(ustate.peer_interested);
        assert!(!ustate.am_choking);
        assert_eq!(0, ustate.bytes_sent);
        let peer = upload::serve_pieces(downloader, Duration::MAX).await.unwrap();
        if let upload::Peer::Idle(_peer) = peer {
            let ustate = uh.with_ctx(|ctx| ctx.peer_states.get(&peer_ip).unwrap().upload.clone());
            assert!(!ustate.peer_interested);
            assert!(!ustate.am_choking);
            assert_eq!(EXPECTED_BYTES_PASSED, ustate.bytes_sent);
        } else {
            panic!("Upload stopped prematurely");
        }
    };
    select! {
        biased;
        _ = upload_fut => (),
        _ = download_fut => (),
    }

    handle.with_ctx(|ctx| {
        assert_eq!(0, ctx.piece_tracker.get_poorest_peers().count());
        assert!(ctx.peer_states.get(&peer_ip).is_none());
        assert_eq!(0, ctx.peer_states.leeches_count());
        assert_eq!(0, ctx.peer_states.seeders_count());
    });

    std::fs::remove_dir_all(files_dir).unwrap();
}

#[tokio::test]
async fn test_pass_partial_torrent_from_seeder_to_leech() {
    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent", log::LevelFilter::Info)
        .init();
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 43210));
    try_join!(
        time::timeout(sec!(30), run_seeder(addr, "tests/assets/zeroed_example.torrent")),
        time::timeout(sec!(30), run_leech(addr, "tests/assets/zeroed_example.torrent")),
    )
    .unwrap();
}

async fn run_peer(
    download: download::IdlePeer,
    upload: upload::IdlePeer,
    mut handle: CtxHandle,
    ip: SocketAddr,
) {
    let handle_copy = handle.clone();
    let upload_fut = async {
        let _ = run_upload(upload.into(), ip, handle_copy).await;
    };
    let download_fut = async {
        let _result = run_download(download.into(), ip, handle.clone()).await;
        handle.with_ctx(|ctx| {
            log::info!("Download from {} finished: {}", ip, ctx.accountant);
            assert_eq!(0, ctx.accountant.missing_bytes());
            assert_eq!(5364053, ctx.accountant.accounted_bytes());
        });
    };
    join!(download_fut, upload_fut);
}

#[allow(clippy::blocks_in_conditions)]
#[tokio::test]
async fn test_pass_full_torrent_from_peer_to_peer() {
    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent::ops::tests", log::LevelFilter::Info)
        .init();

    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 43211));
    let metainfo = "tests/assets/zeroed_test.torrent";

    let connecting_peer = time::timeout(sec!(60), async {
        let (download, upload, mut handle, peer_ip) =
            connecting_peer(addr, metainfo, "test_output2").await;
        let run_peer_fut = run_peer(download, upload, handle.clone(), peer_ip);
        let check_missing_bytes = async {
            while handle.with_ctx(|ctx| {
                log::info!("Downloading from {}: {}", peer_ip, ctx.accountant);
                ctx.accountant.missing_bytes() > 0
            }) {
                time::sleep(sec!(1)).await;
            }
        };
        select! {
        _ = run_peer_fut => (),
        _ = check_missing_bytes => (),
        }
    });
    let listening_peer = time::timeout(sec!(60), async {
        let (download, upload, extensions, handle, peer_ip) =
            listening_seeder(addr, metainfo, "test_input2").await;
        assert!(extensions.is_none());
        run_peer(download, upload, handle, peer_ip).await;
    });
    try_join!(listening_peer, connecting_peer).unwrap();

    std::fs::remove_dir_all("test_output2").unwrap();
    std::fs::remove_dir_all("test_input2").unwrap();
}

#[tokio::test]
async fn test_send_metainfo_file_to_peer() {
    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent::ops", log::LevelFilter::Debug)
        .init();

    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 43212));
    let metainfo = "tests/assets/big_metainfo_file.torrent";

    let sending_peer = time::timeout(sec!(30), async {
        let _ = run_listening_seeder(addr, metainfo, "test_input3").await;
    });
    let requesting_peer = time::timeout(sec!(30), async {
        connecting_peer_downloading_metadata(addr, metainfo).await;
    });
    try_join!(sending_peer, requesting_peer).unwrap();
    std::fs::remove_dir_all("test_input3").unwrap();
}

#[tokio::test]
async fn test_pass_metadata_from_peer_to_peer() {
    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent::ops", log::LevelFilter::Debug)
        .init();

    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 43213));

    let metainfo_filepath = "tests/assets/big_metainfo_file.torrent";
    let magnet = "magnet:?xt=urn:btih:77c09d63baf907ceeed366e2bdd687ca37cc4098&dn=Star.Trek.Voyager.S05.DVDRip.x264-MARS%5brartv%5d&tr=http%3a%2f%2ftracker.trackerfix.com%3a80%2fannounce&tr=udp%3a%2f%2f9.rarbg.me%3a2750%2fannounce&tr=udp%3a%2f%2f9.rarbg.to%3a2930%2fannounce";

    let magnet_link: magnet::MagnetLink = magnet.parse().unwrap();
    let metainfo_content = fs::read(metainfo_filepath).unwrap();

    let sending_peer = time::timeout(sec!(30), async move {
        let _ = run_listening_seeder(addr, metainfo_filepath, "test_input4").await;
    });
    let receiving_peer = time::timeout(sec!(30), async move {
        let mut ctx_handle = ctx::PreliminaryCtx::new(magnet_link, PeerId::generate_new());
        let _ =
            outgoing_preliminary_connection(addr, ctx_handle.clone(), runtime::Handle::current())
                .await;
        ctx_handle.with_ctx(|ctx| {
            assert!(!ctx.metainfo_pieces.is_empty());
            assert!(ctx.metainfo_pieces.all());
            assert_eq!(metainfo_content, ctx.metainfo);
        });
    });
    try_join!(sending_peer, receiving_peer).unwrap();
    std::fs::remove_dir_all("test_input4").unwrap();
}
