use futures::future;
use mtorrent::utils::{self, benc, startup};
use mtorrent::{data, pwp, sec};
use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use std::fs::File;
use std::future::Future;
use std::io::Read;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::{cmp, process};
use std::{fs, io, iter};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::{task, time};

trait Peer {
    const NEEDS_INPUT_DATA: bool;

    fn run(
        index: u8,
        download_chans: pwp::DownloadChannels,
        upload_chans: pwp::UploadChannels,
        runner: pwp::ConnectionRunner,
        storage: data::StorageClient,
        info: Rc<data::PieceInfo>,
    ) -> impl Future<Output = ()>;
}

struct Leech;

impl Peer for Leech {
    const NEEDS_INPUT_DATA: bool = false;

    async fn run(
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

        task::spawn_local(async move {
            let pwp::UploadChannels(_tx, mut rx) = upload_chans;
            while let Ok(msg) = rx.receive_message().await {
                match msg {
                    pwp::DownloaderMessage::Interested => {
                        eprintln!("Leech {index} received Interested")
                    }
                    pwp::DownloaderMessage::NotInterested => {
                        eprintln!("Leech {index} received NotInterested")
                    }
                    pwp::DownloaderMessage::Request(_) => {
                        panic!("Leech {index} received Request")
                    }
                    pwp::DownloaderMessage::Cancel(_) => {
                        panic!("Leech {index} received Cancel")
                    }
                }
            }
        });

        let piece_count = info.piece_count();
        let mut local_pieces = pwp::Bitfield::repeat(false, piece_count);
        let mut remote_pieces = pwp::Bitfield::repeat(false, piece_count);
        let pwp::DownloadChannels(mut tx, mut rx) = download_chans;

        tx.send_message(pwp::DownloaderMessage::Interested).await.unwrap();
        loop {
            match rx.receive_message_timed(sec!(30)).await.unwrap() {
                pwp::UploaderMessage::Choke => (),
                pwp::UploaderMessage::Unchoke => break,
                pwp::UploaderMessage::Have { piece_index } => {
                    remote_pieces.set(piece_index, true);
                }
                pwp::UploaderMessage::Bitfield(bf) => {
                    remote_pieces = bf;
                }
                pwp::UploaderMessage::Block(_, _) => {
                    panic!("Leech {index} received unsolicited block");
                }
            }
        }

        while !local_pieces.all() {
            // find piece to request
            if let Some(piece_index) = iter::zip(local_pieces.iter(), remote_pieces.iter())
                .position(|(local_bit, remote_bit)| local_bit == false && remote_bit == true)
            {
                const MAX_BLOCK_SIZE: usize = 16384;
                let piece_len = info.piece_len(piece_index);
                let mut requests = (0..piece_len)
                    .step_by(MAX_BLOCK_SIZE)
                    .map(move |in_piece_offset| pwp::BlockInfo {
                        piece_index,
                        in_piece_offset,
                        block_length: cmp::min(MAX_BLOCK_SIZE, piece_len - in_piece_offset),
                    })
                    .collect::<HashSet<_>>();
                for block in &requests {
                    tx.send_message(pwp::DownloaderMessage::Request(block.clone())).await.unwrap();
                }
                while !requests.is_empty() {
                    match rx
                        .receive_message_timed(sec!(10))
                        .await
                        .expect("requested block not received")
                    {
                        pwp::UploaderMessage::Choke => {
                            eprintln!("Received premature Choke")
                        }
                        pwp::UploaderMessage::Unchoke => (),
                        pwp::UploaderMessage::Have { piece_index } => {
                            remote_pieces.set(piece_index, true);
                        }
                        pwp::UploaderMessage::Bitfield(_) => {
                            panic!("Leech {index} received disallowed bitfield")
                        }
                        pwp::UploaderMessage::Block(block, data) => {
                            let global_offset = info
                                .global_offset(block.piece_index, block.in_piece_offset, data.len())
                                .unwrap();
                            storage.write_block(global_offset, data).await.unwrap();
                            requests.remove(&block);
                        }
                    }
                }
                local_pieces.set(piece_index, true);
            } else {
                // no pieces can be requested
                match rx.receive_message_timed(sec!(30)).await.unwrap() {
                    pwp::UploaderMessage::Choke => {
                        eprintln!("Received premature Choke")
                    }
                    pwp::UploaderMessage::Unchoke => (),
                    pwp::UploaderMessage::Have { piece_index } => {
                        remote_pieces.set(piece_index, true);
                    }
                    pwp::UploaderMessage::Bitfield(_) => {
                        panic!("Leech {index} received disallowed bitfield")
                    }
                    pwp::UploaderMessage::Block(_, _) => {
                        panic!("Leech {index} received unsolicited block");
                    }
                }
            }
        }
    }
}

struct Seeder;

impl Peer for Seeder {
    const NEEDS_INPUT_DATA: bool = true;

    async fn run(
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
                pwp::UploaderMessage::Block(_, _) => {
                    panic!("Seeder {index} received unsolicited Block");
                }
                _ => (),
            }
            if remote_pieces.iter().all(|has_piece| has_piece == true) {
                println!("Seeder {index} exiting: remote has all pieces");
                return;
            }
        }
        drop(tx);
    }
}

async fn listening_peer<P: Peer>(
    index: u8,
    listening_addr: SocketAddr,
    storage: data::StorageClient,
    info: Rc<data::PieceInfo>,
) {
    match TcpListener::bind(listening_addr).await {
        Ok(listener) => {
            let (stream, remote_addr) = listener.accept().await.unwrap();
            println!(
                "Peer {} on {} accepted connection from {}",
                index, listening_addr, remote_addr
            );
            let (download_chans, upload_chans, runner) =
                pwp::channels_from_incoming(&[index + b'0'; 20], None, stream).await.unwrap();
            P::run(index, download_chans, upload_chans, runner, storage, info).await;
        }
        Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
            eprintln!("Couldn't create listener on {listening_addr}")
        }
        Err(e) => panic!("{e}"),
    }
}

async fn connecting_peer<P: Peer>(
    index: u8,
    remote_ip: SocketAddr,
    info_hash: [u8; 20],
    storage: data::StorageClient,
    info: Rc<data::PieceInfo>,
) {
    println!("Peer {} connecting to {}...", index, remote_ip);
    let start_time = time::Instant::now();
    let (download_chans, upload_chans, runner) = loop {
        let connect_result =
            pwp::channels_from_outgoing(&[index + b'0'; 20], &info_hash, remote_ip, None).await;
        if let Ok(result) = connect_result {
            break result;
        }
        if start_time.elapsed() > sec!(10) {
            panic!("Peer {index} failed to connect within 10 sec");
        }
        time::sleep(sec!(1)).await;
    };
    println!("Peer {} connected to {}", index, remote_ip);
    P::run(index, download_chans, upload_chans, runner, storage, info).await;
}

enum ConnectionMode {
    Outgoing {
        num_peers: usize,
    },
    Incoming {
        listen_addrs: Vec<SocketAddr>,
    },
}

async fn launch_peers<P: Peer>(
    metainfo_file: impl AsRef<Path>,
    files_parentdir: impl AsRef<Path>,
    mode: ConnectionMode,
) {
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

    assert!(files_parentdir.as_ref().is_dir() == P::NEEDS_INPUT_DATA);
    let (storage, storage_server) = startup::create_storage(&metainfo, &files_parentdir).unwrap();

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
        ConnectionMode::Outgoing { num_peers } => {
            task_set
                .run_until(future::join_all((0..num_peers).map(|peer_index| {
                    connecting_peer::<P>(
                        peer_index as u8,
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
                        listening_peer::<P>(
                            index as u8,
                            listen_addr,
                            storage.clone(),
                            pieces.clone(),
                        )
                    },
                )))
                .await;
        }
    }
}

#[must_use]
fn start_tracker<'a>(
    dir: &str,
    port: u16,
    peers: impl Iterator<Item = &'a SocketAddr>,
) -> process::Child {
    fn get_peers_entry(addr: &SocketAddr) -> benc::Element {
        let ip_key = benc::Element::from("ip");
        let ip_value = benc::Element::from(addr.ip().to_string().as_str());
        let port_key = benc::Element::from("port");
        let port_value = benc::Element::Integer(addr.port() as i64);
        let mut d = BTreeMap::new();
        d.insert(ip_key, ip_value);
        d.insert(port_key, port_value);
        benc::Element::Dictionary(d)
    }

    let response = {
        let peers_key = benc::Element::from("peers");
        let peers_value = benc::Element::List(peers.map(get_peers_entry).collect());

        let interval_key = benc::Element::from("interval");
        let interval_value = benc::Element::Integer(1800);

        let mut root = BTreeMap::new();
        root.insert(peers_key, peers_value);
        root.insert(interval_key, interval_value);
        benc::Element::Dictionary(root)
    };

    let _ = fs::create_dir(dir);

    let announce_path: PathBuf = [dir, "announce"].into_iter().collect();
    fs::write(announce_path, response.to_bytes()).unwrap();

    process::Command::new("python3")
        .args(["-m", "http.server", &port.to_string()])
        .current_dir(dir)
        .spawn()
        .unwrap()
}

fn compare_input_and_output(
    input_dir: impl AsRef<Path> + Debug,
    output_dir: impl AsRef<Path>,
    name: &str,
) {
    let output_dir = output_dir.as_ref().join(name);
    assert!(output_dir.is_dir());
    assert!(input_dir.as_ref().is_dir());
    println!("comparing {input_dir:?} and {output_dir:?}");

    fn verify_files_identical(p1: impl AsRef<Path>, p2: impl AsRef<Path>) {
        let name1 = p1.as_ref().to_string_lossy().into_owned();
        let name2 = p2.as_ref().to_string_lossy().into_owned();
        let mut f1 = File::open(p1).unwrap();
        let mut f2 = File::open(p2).unwrap();
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
            compare_input_and_output(input_path, output_path, "");
        } else if input_path.is_file() && output_path.is_file() {
            verify_files_identical(input_path, output_path);
        } else {
            panic!();
        }
    }
}

#[tokio::test]
async fn test_download_multifile_torrent_from_50_seeders() {
    // these tests need to run sequentially because mtorrent
    // always listens on the same port for a given torrent
    let metainfo_file = "tests/assets/screenshots.torrent";
    let torrent_name = "screenshots";
    {
        let output_dir = "test_accept_50_seeders_and_download_multifile_torrent";
        let data_dir = "tests/assets/screenshots";

        let mut mtorrent = process::Command::new(env!("CARGO_BIN_EXE_mtorrentv2"))
            .arg(metainfo_file)
            .arg(output_dir)
            .spawn()
            .expect("failed to execute 'mtorrentv2'");

        launch_peers::<Seeder>(metainfo_file, data_dir, ConnectionMode::Outgoing { num_peers: 50 })
            .await;

        let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrentv2'");
        assert!(mtorrent_ecode.success());

        compare_input_and_output(data_dir, output_dir, torrent_name);
        std::fs::remove_dir_all(output_dir).unwrap();
    }
    {
        let output_dir = "test_connect_to_50_seeders_and_download_multifile_torrent";
        let data_dir = "tests/assets/screenshots";
        let tracker_port = 9000u16;

        let seeder_ips = (50000u16..50050u16)
            .map(|port| SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)))
            .collect::<Vec<_>>();

        let mut tracker = start_tracker(output_dir, tracker_port, seeder_ips.iter());

        let mut mtorrent = process::Command::new(env!("CARGO_BIN_EXE_mtorrentv2"))
            .arg(metainfo_file)
            .arg(output_dir)
            .spawn()
            .expect("failed to execute 'mtorrentv2'");

        launch_peers::<Seeder>(
            metainfo_file,
            data_dir,
            ConnectionMode::Incoming {
                listen_addrs: seeder_ips,
            },
        )
        .await;

        let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrentv2'");
        assert!(mtorrent_ecode.success());

        tracker.kill().unwrap();

        compare_input_and_output(data_dir, output_dir, torrent_name);
        std::fs::remove_dir_all(output_dir).unwrap();
    }
    {
        let output_dir = "test_accept_1_leech_and_upload_multifile_torrent";
        let data_dir = "tests/assets";

        let mut mtorrent = process::Command::new(env!("CARGO_BIN_EXE_mtorrentv2"))
            .arg(metainfo_file)
            .arg(data_dir)
            .spawn()
            .expect("failed to execute 'mtorrentv2'");

        launch_peers::<Leech>(metainfo_file, output_dir, ConnectionMode::Outgoing { num_peers: 1 })
            .await;

        let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrentv2'");
        assert!(mtorrent_ecode.success());

        compare_input_and_output(output_dir, data_dir, torrent_name);
        std::fs::remove_dir_all(output_dir).unwrap();
    }
}

#[tokio::test]
async fn test_download_and_upload_monofile_torrent() {
    // these tests need to run sequentially because mtorrent
    // always listens on the same port for a given torrent
    let metainfo_file = "tests/assets/pcap.torrent";
    let torrent_name = "pcap";
    {
        let output_dir = "test_accept_50_seeders_and_download_monofile_torrent";
        let data_dir = "tests/assets/pcap";

        let mut mtorrent = process::Command::new(env!("CARGO_BIN_EXE_mtorrentv2"))
            .arg(metainfo_file)
            .arg(output_dir)
            .spawn()
            .expect("failed to execute 'mtorrentv2'");

        launch_peers::<Seeder>(metainfo_file, data_dir, ConnectionMode::Outgoing { num_peers: 50 })
            .await;

        let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrentv2'");
        assert!(mtorrent_ecode.success());

        compare_input_and_output(data_dir, output_dir, torrent_name);
        std::fs::remove_dir_all(output_dir).unwrap();
    }
    {
        let output_dir = "test_connect_to_50_seeders_and_download_monofile_torrent";
        let data_dir = "tests/assets/pcap";
        let tracker_port = 8000u16;

        let seeder_ips = (50050u16..50100u16)
            .map(|port| SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)))
            .collect::<Vec<_>>();

        let mut tracker = start_tracker(output_dir, tracker_port, seeder_ips.iter());

        let mut mtorrent = process::Command::new(env!("CARGO_BIN_EXE_mtorrentv2"))
            .arg(metainfo_file)
            .arg(output_dir)
            .spawn()
            .expect("failed to execute 'mtorrentv2'");

        launch_peers::<Seeder>(
            metainfo_file,
            data_dir,
            ConnectionMode::Incoming {
                listen_addrs: seeder_ips,
            },
        )
        .await;

        let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrentv2'");
        assert!(mtorrent_ecode.success());

        tracker.kill().unwrap();

        compare_input_and_output(data_dir, output_dir, torrent_name);
        std::fs::remove_dir_all(output_dir).unwrap();
    }
    {
        let output_dir = "test_accept_1_leech_and_upload_monofile_torrent";
        let data_dir = "tests/assets";

        let mut mtorrent = process::Command::new(env!("CARGO_BIN_EXE_mtorrentv2"))
            .arg(metainfo_file)
            .arg(data_dir)
            .spawn()
            .expect("failed to execute 'mtorrentv2'");

        launch_peers::<Leech>(metainfo_file, output_dir, ConnectionMode::Outgoing { num_peers: 1 })
            .await;

        let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrentv2'");
        assert!(mtorrent_ecode.success());

        compare_input_and_output(output_dir, data_dir, torrent_name);
        std::fs::remove_dir_all(output_dir).unwrap();
    }
}
