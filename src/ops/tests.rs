use super::{ctx, download, peer, upload};
use crate::utils::{startup, worker::simple};
use crate::{data, define_with_ctx, pwp, sec};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::{rc::Rc, time::Duration};
use tokio::{join, net::TcpListener, runtime, select, time};

const PIECE_LENGTH: usize = 2_097_152;
const LAST_PIECE_LENGTH: usize = 1_160_807;
const PIECE_COUNT: usize = 668;
const TOTAL_BYTES: usize = PIECE_LENGTH * (PIECE_COUNT - 1) + LAST_PIECE_LENGTH;

const START_PIECE_INDEX: usize = 600;
const EXPECTED_BYTES_PASSED: usize =
    PIECE_LENGTH * (PIECE_COUNT - START_PIECE_INDEX - 1) + LAST_PIECE_LENGTH;

async fn connecting_peer(
    peer_ip: SocketAddr,
    metainfo_path: &'static str,
    files_dir: &'static str,
) -> (download::IdlePeer, upload::IdlePeer, ctx::Handle, SocketAddr, simple::Handle) {
    let metainfo = startup::read_metainfo(metainfo_path).unwrap();
    let storage = data::Storage::new(files_dir, metainfo.files().unwrap()).unwrap();
    let (storage, storage_handle) = startup::start_storage(storage);

    let ctx = ctx::Owner::new(Rc::into_inner(metainfo).unwrap()).unwrap();
    let handle = ctx.create_handle();

    let mut local_id = [0u8; 20];
    local_id[..5].copy_from_slice("leech".as_bytes());

    let (download, upload) = peer::from_outgoing_connection(
        peer_ip,
        [b'l'; 20],
        storage,
        handle.clone(),
        runtime::Handle::current(),
    )
    .await
    .unwrap();
    (download, upload, handle, peer_ip, storage_handle)
}

async fn listening_peer(
    listener_ip: SocketAddr,
    metainfo_path: &'static str,
    files_dir: &'static str,
) -> (download::IdlePeer, upload::IdlePeer, ctx::Handle, SocketAddr, simple::Handle) {
    let metainfo = startup::read_metainfo(metainfo_path).unwrap();
    let storage = data::Storage::new(files_dir, metainfo.files().unwrap()).unwrap();
    let (storage, storage_handle) = startup::start_storage(storage);

    let piece_count = metainfo.pieces().unwrap().count();
    let ctx = ctx::Owner::new(Rc::into_inner(metainfo).unwrap()).unwrap();
    let mut handle = ctx.create_handle();
    handle.with_ctx(|ctx| {
        for piece_index in 0..piece_count {
            assert!(ctx.accountant.submit_piece(piece_index));
            ctx.piece_tracker.forget_piece(piece_index);
        }
    });

    let mut local_id = [0u8; 20];
    local_id[..6].copy_from_slice("seeder".as_bytes());

    let listener = TcpListener::bind(listener_ip).await.unwrap();
    let (stream, peer_ip) = listener.accept().await.unwrap();
    let (download, upload) = peer::from_incoming_connection(
        stream,
        local_id,
        storage,
        handle.clone(),
        runtime::Handle::current(),
    )
    .await
    .unwrap();
    (download, upload, handle, peer_ip, storage_handle)
}

async fn run_leech(peer_ip: SocketAddr, metainfo_path: &'static str) {
    let files_dir = "test_output";
    let (download, upload, mut handle, _ip, _storage) =
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
        let (dstate, ustate) = ctx.peer_states.get(&peer_ip).unwrap();
        assert_eq!(pwp::DownloadState::default(), *dstate);
        assert_eq!(pwp::UploadState::default(), *ustate);
    });

    let mut uh = handle.clone();
    let upload_fut = async move {
        let mut peer = upload;
        while let upload::Peer::Idle(farend) = upload::linger(peer, Duration::MAX).await.unwrap() {
            peer = farend;
            let ustate = uh.with_ctx(|ctx| ctx.peer_states.get(&peer_ip).unwrap().1.clone());
            assert_eq!(pwp::UploadState::default(), ustate);
        }
    };
    let mut dh = handle.clone();
    let download_fut = async move {
        let seeder = download::activate(download).await.unwrap();
        let dstate = dh.with_ctx(|ctx| ctx.peer_states.get(&peer_ip).unwrap().0.clone());
        assert!(dstate.am_interested);
        assert!(!dstate.peer_choking);
        assert_eq!(0, dstate.bytes_received);

        let peer =
            download::get_pieces(seeder, &(START_PIECE_INDEX..piece_count).collect::<Vec<_>>())
                .await
                .unwrap();
        if let download::Peer::Seeder(seeder) = peer {
            let dstate = dh.with_ctx(|ctx| ctx.peer_states.get(&peer_ip).unwrap().0.clone());
            assert!(dstate.am_interested);
            assert!(!dstate.peer_choking);
            assert_eq!(EXPECTED_BYTES_PASSED, dstate.bytes_received);
            dh.with_ctx(|ctx| {
                assert_eq!(EXPECTED_BYTES_PASSED, ctx.accountant.accounted_bytes());
                assert_eq!(TOTAL_BYTES - EXPECTED_BYTES_PASSED, ctx.accountant.missing_bytes());
            });

            let _peer = download::deactivate(seeder).await.unwrap();
            let dstate = dh.with_ctx(|ctx| ctx.peer_states.get(&peer_ip).unwrap().0.clone());
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
    let (download, upload, mut handle, peer_ip, _storage) =
        listening_peer(listener_ip, metainfo_path, files_dir).await;

    handle.with_ctx(|ctx| {
        assert_eq!(0, ctx.piece_tracker.get_poorest_peers().count());
        assert_eq!(0, ctx.peer_states.leeches_count());
        assert_eq!(0, ctx.peer_states.seeders_count());
        let (dstate, ustate) = ctx.peer_states.get(&peer_ip).unwrap();
        assert_eq!(pwp::DownloadState::default(), *dstate);
        assert_eq!(pwp::UploadState::default(), *ustate);
    });

    let mut dh = handle.clone();
    let download_fut = async move {
        let mut peer = download::Peer::Idle(download);
        while let Ok(farend) = download::linger(peer, Duration::MAX).await {
            peer = farend;
            let dstate = dh.with_ctx(|ctx| ctx.peer_states.get(&peer_ip).unwrap().0.clone());
            assert_eq!(pwp::DownloadState::default(), dstate);
        }
    };
    let mut uh = handle.clone();
    let upload_fut = async move {
        let downloader = upload::activate(upload).await.unwrap();
        let ustate = uh.with_ctx(|ctx| ctx.peer_states.get(&peer_ip).unwrap().1.clone());
        assert!(ustate.peer_interested);
        assert!(!ustate.am_choking);
        assert_eq!(0, ustate.bytes_sent);
        let peer = upload::serve_pieces(downloader, Duration::MAX).await.unwrap();
        if let upload::Peer::Idle(_peer) = peer {
            let ustate = uh.with_ctx(|ctx| ctx.peer_states.get(&peer_ip).unwrap().1.clone());
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
async fn test_pass_torrent_from_seeder_to_leech() {
    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent", log::LevelFilter::Info)
        .init();
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 43210));
    join!(run_seeder(addr, "tests/zeroed.torrent"), run_leech(addr, "tests/zeroed.torrent"));
}

async fn run_peer(
    download: download::IdlePeer,
    upload: upload::IdlePeer,
    mut handle: ctx::Handle,
    ip: SocketAddr,
    _storage: simple::Handle,
) {
    let handle_copy = handle.clone();
    let upload_fut = async {
        let _ = peer::run_upload(upload.into(), ip, handle_copy).await;
    };
    let download_fut = async {
        let _result = peer::run_download(download.into(), ip, handle.clone()).await;
        handle.with_ctx(|ctx| {
            log::info!("Download from {} finished: {}", ip, ctx.accountant);
            assert_eq!(0, ctx.accountant.missing_bytes());
            assert_eq!(TOTAL_BYTES, ctx.accountant.accounted_bytes());
        });
    };
    join!(download_fut, upload_fut);
}

#[allow(clippy::blocks_in_if_conditions)]
#[tokio::test]
async fn test_pass_torrent_from_peer_to_peer() {
    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent::ops::tests", log::LevelFilter::Info)
        .init();

    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 43211));
    let metainfo = "tests/zeroed.torrent";

    let connecting_peer = async {
        let (download, upload, mut handle, peer_ip, storage) =
            connecting_peer(addr, metainfo, "test_output2").await;
        let run_peer_fut = run_peer(download, upload, handle.clone(), peer_ip, storage);
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
    };
    let listening_peer = async {
        let (download, upload, handle, peer_ip, storage) =
            listening_peer(addr, metainfo, "test_input2").await;
        run_peer(download, upload, handle, peer_ip, storage).await;
    };
    join!(listening_peer, connecting_peer);

    std::fs::remove_dir_all("test_output2").unwrap();
    std::fs::remove_dir_all("test_input2").unwrap();
}
