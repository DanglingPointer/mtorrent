use super::{ctx, download, peer, upload};
use crate::sec;
use crate::{data, utils::startup};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::{rc::Rc, time::Duration};
use tokio::{join, net::TcpListener, runtime, select};

const PIECE_LENGTH: usize = 2_097_152;
const LAST_PIECE_LENGTH: usize = 1_160_807;
const PIECE_COUNT: usize = 668;
const TOTAL_BYTES: usize = PIECE_LENGTH * (PIECE_COUNT - 1) + LAST_PIECE_LENGTH;

const START_PIECE_INDEX: usize = 600;
const EXPECTED_BYTES_PASSED: usize =
    PIECE_LENGTH * (PIECE_COUNT - START_PIECE_INDEX - 1) + LAST_PIECE_LENGTH;

async fn run_leech(peer_ip: SocketAddr, metainfo_path: &'static str) {
    use crate::ops::download::State;

    let files_dir = "test_output";
    let metainfo = startup::read_metainfo(metainfo_path).unwrap();
    let storage = data::Storage::new(files_dir, metainfo.files().unwrap()).unwrap();
    let (storage, _handle) = startup::start_storage(storage);

    let piece_count = metainfo.pieces().unwrap().count();
    let ctx = ctx::Owner::new(Rc::into_inner(metainfo).unwrap()).unwrap();
    let mut handle = ctx.create_handle();

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

    handle.with_ctx(|ctx| {
        assert_eq!(PIECE_COUNT, ctx.piece_tracker.get_rarest_pieces().count());
        assert_eq!(TOTAL_BYTES, ctx.accountant.missing_bytes());
        assert_eq!(0, ctx.accountant.accounted_bytes());
        assert_eq!(PIECE_COUNT, ctx.piece_tracker.get_peer_pieces(&peer_ip).unwrap().count());
    });

    let upload_fut = async move {
        use crate::ops::upload::State;
        let mut peer = upload;
        while let upload::Peer::Idle(farend) = upload::linger(peer, Duration::MAX).await.unwrap() {
            peer = farend;
            assert_eq!(0, peer.state().bytes_sent());
        }
    };
    let download_fut = async move {
        let seeder = download::activate(download).await.unwrap();
        assert_eq!(0, seeder.state().bytes_received());

        let peer = download::get_pieces(seeder, START_PIECE_INDEX..piece_count).await.unwrap();
        if let download::Peer::Seeder(seeder) = peer {
            assert_eq!(EXPECTED_BYTES_PASSED, seeder.state().bytes_received())
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
    });

    std::fs::remove_dir_all(files_dir).unwrap();
}

async fn run_seeder(listener_ip: SocketAddr, metainfo_path: &'static str) {
    use crate::ops::upload::State;

    let files_dir = "test_input";
    let metainfo = startup::read_metainfo(metainfo_path).unwrap();
    let storage = data::Storage::new(files_dir, metainfo.files().unwrap()).unwrap();
    let (storage, _handle) = startup::start_storage(storage);

    let piece_count = metainfo.pieces().unwrap().count();
    let ctx = ctx::Owner::new(Rc::into_inner(metainfo).unwrap()).unwrap();
    let mut handle = ctx.create_handle();
    handle.with_ctx(|ctx| {
        for piece_index in 0..piece_count {
            assert!(ctx.accountant.submit_piece(piece_index));
        }
    });

    let mut local_id = [0u8; 20];
    local_id[..6].copy_from_slice("seeder".as_bytes());

    let listener = TcpListener::bind(listener_ip).await.unwrap();
    let (stream, _remote_addr) = listener.accept().await.unwrap();
    let (download, upload) = peer::from_incoming_connection(
        stream,
        local_id,
        storage,
        handle.clone(),
        runtime::Handle::current(),
    )
    .await
    .unwrap();

    handle.with_ctx(|ctx| {
        assert_eq!(0, ctx.piece_tracker.get_poorest_peers().count());
    });

    let download_fut = async move {
        use crate::ops::download::State;
        let mut peer = download::Peer::Idle(download);
        while let Ok(farend) = download::linger(peer, Duration::MAX).await {
            peer = farend;
            let bytes_downloaded = match &peer {
                download::Peer::Idle(p) => p.state().bytes_received(),
                download::Peer::Seeder(p) => p.state().bytes_received(),
            };
            assert_eq!(0, bytes_downloaded);
        }
    };
    let upload_fut = async move {
        let downloader = upload::activate(upload).await.unwrap();
        assert_eq!(0, downloader.state().bytes_sent());
        let peer = match upload::serve_pieces(downloader, sec!(1)).await {
            Ok(upload::Peer::Leech(leech)) => leech,
            _ => panic!(),
        };
        assert!(TOTAL_BYTES > peer.state().bytes_sent());
        assert!(0 < peer.state().bytes_sent());
        let _ = upload::serve_pieces(peer, Duration::MAX).await;
    };
    select! {
        biased;
        _ = upload_fut => (),
        _ = download_fut => (),
    }

    handle.with_ctx(|ctx| {
        assert_eq!(0, ctx.piece_tracker.get_poorest_peers().count());
    });

    std::fs::remove_dir_all(files_dir).unwrap();
}

#[tokio::test]
async fn test_pass_torrent_from_seeder_to_leech() {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent", log::LevelFilter::Info)
        .init()
        .unwrap();
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 43210));
    join!(run_seeder(addr, "tests/zeroed.torrent"), run_leech(addr, "tests/zeroed.torrent"));
}
