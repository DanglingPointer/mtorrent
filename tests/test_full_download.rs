use futures::future;
use mtorrent::utils::{self, startup};
use mtorrent::{data, pwp, sec};
use rand::random;
use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::process::Command;
use std::rc::Rc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::{task, time};

async fn run_one_seeder(
    index: u8,
    download_chans: pwp::DownloadChannels,
    upload_chans: pwp::UploadChannels,
    runner: pwp::ConnectionRunner,
    storage: data::StorageClient,
    info: Rc<data::PieceInfo>,
) {
    task::spawn_local(async move {
        let _ = runner.run().await;
    });

    let piece_count = info.piece_count();
    let bitfield = pwp::Bitfield::repeat(true, piece_count);
    let pwp::UploadChannels(mut tx, mut rx) = upload_chans;
    tx.send_message(pwp::UploaderMessage::Bitfield(bitfield)).await.unwrap();
    tx.send_message(pwp::UploaderMessage::Unchoke).await.unwrap();

    let (request_sender, mut request_receiver) = mpsc::unbounded_channel::<pwp::BlockInfo>();
    let (cancel_sender, mut cancel_receiver) = mpsc::unbounded_channel::<pwp::BlockInfo>();

    task::spawn_local(async move {
        let mut cancelled_blocks = HashSet::<pwp::BlockInfo>::new();
        loop {
            if let Ok(cancelled_block) = cancel_receiver.try_recv() {
                cancelled_blocks.insert(cancelled_block);
            }
            if let Some(requested_block) = request_receiver.recv().await {
                if !cancelled_blocks.remove(&requested_block) {
                    let global_offset = info
                        .global_offset(
                            requested_block.piece_index,
                            requested_block.in_piece_offset,
                            requested_block.block_length,
                        )
                        .unwrap_or_else(|_| {
                            panic!("Seeder received invalid request {requested_block}");
                        });
                    let data = storage
                        .read_block(global_offset, requested_block.block_length)
                        .await
                        .unwrap_or_else(|e| {
                            panic!(
                                "Couldn't read {} bytes at offset {}: {}",
                                requested_block.block_length, global_offset, e
                            )
                        });
                    if tx
                        .send_message(pwp::UploaderMessage::Block(requested_block, data))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            } else {
                return;
            }
        }
    });

    task::spawn_local(async move {
        while let Ok(msg) = rx.receive_message().await {
            match msg {
                pwp::DownloaderMessage::Request(block) => request_sender.send(block).unwrap(),
                pwp::DownloaderMessage::Cancel(block) => cancel_sender.send(block).unwrap(),
                msg => println!("Seeder {index} received {msg}"),
            }
        }
    });

    let mut remote_pieces = pwp::Bitfield::repeat(false, piece_count);
    let pwp::DownloadChannels(tx, mut rx) = download_chans;
    while let Ok(msg) = rx.receive_message().await {
        // println!("Seeder {index} received {msg}");
        match msg {
            pwp::UploaderMessage::Have { piece_index } => {
                if remote_pieces.get(piece_index).unwrap() == true {
                    panic!("Seeder {index} received double Have");
                }
                remote_pieces.set(piece_index, true);
            }
            pwp::UploaderMessage::Bitfield(mut bf) => {
                assert!(remote_pieces.len() <= bf.len());
                bf.resize(remote_pieces.len(), false);
                remote_pieces = bf;
            }
            _ => (),
        }
        if remote_pieces.iter().all(|has_piece| has_piece == true) {
            println!("Seeder {index} exiting: remote as all pieces");
            return;
        }
    }
    drop(tx);
}

async fn listening_seeder(
    index: u8,
    listening_addr: SocketAddr,
    storage: data::StorageClient,
    info: Rc<data::PieceInfo>,
) {
    let listener = TcpListener::bind(listening_addr).await.unwrap();
    let (stream, remote_addr) = listener.accept().await.unwrap();
    println!("Seeder {} on {} accepted connection from {}", index, listening_addr, remote_addr);
    let (download_chans, upload_chans, runner) =
        pwp::channels_from_incoming(&[index; 20], None, stream).await.unwrap();
    run_one_seeder(index, download_chans, upload_chans, runner, storage, info).await;
}

async fn connecting_seeder(
    index: u8,
    remote_ip: SocketAddr,
    info_hash: [u8; 20],
    storage: data::StorageClient,
    info: Rc<data::PieceInfo>,
) {
    println!("Seeder {} connecting to {}...", index, remote_ip);
    let start_time = time::Instant::now();
    let (download_chans, upload_chans, runner) = loop {
        let connect_result =
            pwp::channels_from_outgoing(&[index; 20], &info_hash, remote_ip, None).await;
        if let Ok(result) = connect_result {
            break result;
        }
        if start_time.elapsed() > sec!(10) {
            panic!("Seeder {index} failed to connect within 10 sec");
        }
        time::sleep(sec!(1)).await;
    };
    println!("Seeder {} connected to {}", index, remote_ip);
    run_one_seeder(index, download_chans, upload_chans, runner, storage, info).await;
}

enum ConnectionMode {
    Outgoing {
        num_seeders: usize,
    },
    Incoming {
        listen_addrs: Vec<SocketAddr>,
    },
}

async fn launch_seeders(metainfo_file: &str, mode: ConnectionMode) {
    let metainfo = startup::read_metainfo(metainfo_file).unwrap();

    let total_length = metainfo
        .length()
        .or_else(|| metainfo.files().map(|it| it.map(|(len, _path)| len).sum()))
        .unwrap();

    let pieces = Rc::new(data::PieceInfo::new(
        metainfo.pieces().unwrap(),
        metainfo.piece_length().unwrap(),
        total_length,
    ));

    let files_dir = format!("seeders_input_{:?}", random::<u32>());
    let (storage, storage_server) = startup::create_storage(&metainfo, &files_dir).unwrap();

    let task_set = task::LocalSet::new();
    task_set.spawn_local(async move {
        storage_server.run().await;
    });

    let info_hash = *metainfo.info_hash();
    let remote_ip = {
        let any = utils::ip::any_socketaddr_from_hash(&metainfo);
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, any.port()))
    };
    match mode {
        ConnectionMode::Outgoing { num_seeders } => {
            task_set
                .run_until(future::join_all((0..num_seeders).map(|seeder_index| {
                    connecting_seeder(
                        seeder_index as u8,
                        remote_ip,
                        info_hash,
                        storage.clone(),
                        pieces.clone(),
                    )
                })))
                .await;
        }
        ConnectionMode::Incoming { listen_addrs } => {
            task_set
                .run_until(future::join_all(listen_addrs.into_iter().enumerate().map(
                    |(index, listen_addr)| {
                        listening_seeder(index as u8, listen_addr, storage.clone(), pieces.clone())
                    },
                )))
                .await;
        }
    }

    std::fs::remove_dir_all(files_dir).unwrap();
}

#[ignore]
#[tokio::test]
async fn test_connect_to_50_seeders_and_download_multifile_torrent() {
    let metainfo_file = "tests/assets/zeroed_example.torrent";
    let output_dir = "test_connect_to_50_seeders_and_download_multifile_torrent";

    let seeder_ips = (50000u16..50050u16)
        .map(|port| SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)))
        .collect::<Vec<_>>();

    let mut mtorrent = {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mtorrentv2"));
        cmd.arg(metainfo_file).arg(output_dir);
        for ip in &seeder_ips {
            cmd.arg(ip.to_string());
        }
        cmd.spawn().expect("failed to execute 'mtorrentv2'")
    };

    launch_seeders(
        metainfo_file,
        ConnectionMode::Incoming {
            listen_addrs: seeder_ips,
        },
    )
    .await;

    let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrentv2'");
    assert!(mtorrent_ecode.success());

    std::fs::remove_dir_all(output_dir).unwrap();
}

#[tokio::test]
async fn test_accept_50_seeders_and_download_multifile_torrent() {
    let metainfo_file = "tests/assets/zeroed_example.torrent";
    let output_dir = "test_accept_50_seeders_and_download_multifile_torrent";

    let mut mtorrent = Command::new(env!("CARGO_BIN_EXE_mtorrentv2"))
        .arg(metainfo_file)
        .arg(output_dir)
        .spawn()
        .expect("failed to execute 'mtorrentv2'");

    launch_seeders(metainfo_file, ConnectionMode::Outgoing { num_seeders: 50 }).await;

    let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrentv2'");
    assert!(mtorrent_ecode.success());

    std::fs::remove_dir_all(output_dir).unwrap();
}

#[ignore]
#[tokio::test]
async fn test_accept_50_seeders_and_download_monofile_torrent() {
    let metainfo_file = "tests/assets/zeroed_ubuntu.torrent";
    let output_dir = "test_accept_50_seeders_and_download_monofile_torrent";

    let mut mtorrent = Command::new(env!("CARGO_BIN_EXE_mtorrentv2"))
        .arg(metainfo_file)
        .arg(output_dir)
        .spawn()
        .expect("failed to execute 'mtorrentv2'");

    launch_seeders(metainfo_file, ConnectionMode::Outgoing { num_seeders: 50 }).await;

    let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrentv2'");
    assert!(mtorrent_ecode.success());

    std::fs::remove_dir_all(output_dir).unwrap();
}

#[tokio::test]
async fn test_connect_to_50_seeders_and_download_monofile_torrent() {
    let metainfo_file = "tests/assets/zeroed_ubuntu.torrent";
    let output_dir = "test_connect_to_50_seeders_and_download_monofile_torrent";

    let seeder_ips = (50050u16..50100u16)
        .map(|port| SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)))
        .collect::<Vec<_>>();

    let mut mtorrent = {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mtorrentv2"));
        cmd.arg(metainfo_file).arg(output_dir);
        for ip in &seeder_ips {
            cmd.arg(ip.to_string());
        }
        cmd.spawn().expect("failed to execute 'mtorrentv2'")
    };

    launch_seeders(
        metainfo_file,
        ConnectionMode::Incoming {
            listen_addrs: seeder_ips,
        },
    )
    .await;

    let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrentv2'");
    assert!(mtorrent_ecode.success());

    std::fs::remove_dir_all(output_dir).unwrap();
}
