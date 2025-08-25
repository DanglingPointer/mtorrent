use futures_util::future;
use local_async_utils::prelude::*;
use mtorrent::utils::startup;
use mtorrent_core::{data, pwp};
use mtorrent_utils::metainfo::Metainfo;
use mtorrent_utils::{benc, ip};
use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use std::fs::File;
use std::future::Future;
use std::io::Read;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::Path;
use std::rc::Rc;
use std::{cmp, process};
use std::{fs, io, iter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::{join, task, time};

trait Peer {
    const NEEDS_INPUT_DATA: bool;

    #[expect(clippy::too_many_arguments)]
    fn run(
        index: u8,
        download_chans: pwp::DownloadChannels,
        upload_chans: pwp::UploadChannels,
        ext_chans: Option<pwp::ExtendedChannels>,
        runner: pwp::ConnectionIoDriver,
        content_storage: data::StorageClient,
        metainfo_storage: data::StorageClient,
        metainfo: Rc<Metainfo>,
    ) -> impl Future<Output = ()>;
}

struct Leech;

impl Peer for Leech {
    const NEEDS_INPUT_DATA: bool = false;

    async fn run(
        index: u8,
        download_chans: pwp::DownloadChannels,
        upload_chans: pwp::UploadChannels,
        ext_chans: Option<pwp::ExtendedChannels>,
        runner: pwp::ConnectionIoDriver,
        storage: data::StorageClient,
        _metainfo_storage: data::StorageClient,
        metainfo: Rc<Metainfo>,
    ) {
        assert!(ext_chans.is_none());

        let info = get_piece_info(&metainfo);
        task::spawn_local(async move {
            let _ = runner.await;
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

impl Seeder {
    async fn seed_metainfo(
        index: u8,
        mut download_chans: pwp::DownloadChannels,
        mut upload_chans: pwp::UploadChannels,
        ext_chans: pwp::ExtendedChannels,
        meta_storage: data::StorageClient,
        metadata_len: usize,
    ) {
        task::spawn_local(async move {
            if let Ok(msg) = download_chans.1.receive_message().await {
                panic!("Received unexpected downloader msg: {msg}");
            }
        });
        task::spawn_local(async move {
            if let Ok(msg) = upload_chans.1.receive_message().await {
                panic!("Received unexpected uploader msg: {msg}");
            }
        });

        let pwp::ExtendedChannels(mut tx, mut rx) = ext_chans;
        tx.send_message((
            pwp::ExtendedMessage::Handshake(Box::new(pwp::ExtendedHandshake {
                extensions: [(pwp::Extension::Metadata, pwp::Extension::Metadata.local_id())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            })),
            0,
        ))
        .await
        .unwrap();
        while let Ok(msg) = rx.receive_message().await {
            if let pwp::ExtendedMessage::MetadataRequest { piece } = msg {
                if index % 2 == 0 {
                    tx.send_message((
                        pwp::ExtendedMessage::MetadataReject { piece },
                        pwp::Extension::Metadata.local_id(),
                    ))
                    .await
                    .unwrap();
                } else {
                    let max_block_size = 16 * 1024;
                    let global_offset = piece * max_block_size;
                    let length = cmp::min(max_block_size, metadata_len - global_offset);
                    let data = meta_storage.read_block(global_offset, length).await.unwrap();
                    tx.send_message((
                        pwp::ExtendedMessage::MetadataBlock {
                            piece,
                            total_size: metadata_len,
                            data,
                        },
                        pwp::Extension::Metadata.local_id(),
                    ))
                    .await
                    .unwrap();
                }
            }
        }
        println!("Seeder uploaded metainfo successfully");
    }

    async fn seed_content(
        index: u8,
        download_chans: pwp::DownloadChannels,
        upload_chans: pwp::UploadChannels,
        content_storage: data::StorageClient,
        info: data::PieceInfo,
    ) {
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
                        let data = content_storage
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
        let mut haves_count = 0usize;
        let mut bitfield_ones_count = 0usize;
        let pwp::DownloadChannels(tx, mut rx) = download_chans;
        while let Ok(msg) = rx.receive_message().await {
            // println!("Seeder {index} received {msg}");
            match msg {
                pwp::UploaderMessage::Have { piece_index } => {
                    if remote_pieces.get(piece_index).unwrap() == true {
                        panic!("Seeder {index} received double Have");
                    }
                    remote_pieces.set(piece_index, true);
                    haves_count += 1;
                }
                pwp::UploaderMessage::Bitfield(mut bf) => {
                    assert!(remote_pieces.len() <= bf.len());
                    assert_eq!(bitfield_ones_count, 0);
                    bitfield_ones_count = bf.count_ones();
                    bf.resize(remote_pieces.len(), false);
                    remote_pieces = bf;
                }
                pwp::UploaderMessage::Block(_, _) => {
                    panic!("Seeder {index} received unsolicited Block");
                }
                _ => (),
            }
            if remote_pieces.all() {
                println!("Seeder {index} exiting: remote has all pieces");
                break;
            }
        }
        assert!(remote_pieces.all());
        assert_eq!(haves_count + bitfield_ones_count, remote_pieces.len());
        drop(tx);
    }
}

fn get_piece_info(metainfo: &Metainfo) -> data::PieceInfo {
    let total_length = metainfo
        .length()
        .or_else(|| metainfo.files().map(|it| it.map(|(len, _path)| len).sum()))
        .unwrap();

    data::PieceInfo::new(metainfo.pieces().unwrap(), metainfo.piece_length().unwrap(), total_length)
}

impl Peer for Seeder {
    const NEEDS_INPUT_DATA: bool = true;

    async fn run(
        index: u8,
        download_chans: pwp::DownloadChannels,
        upload_chans: pwp::UploadChannels,
        ext_chans: Option<pwp::ExtendedChannels>,
        runner: pwp::ConnectionIoDriver,
        content_storage: data::StorageClient,
        meta_storage: data::StorageClient,
        metainfo: Rc<Metainfo>,
    ) {
        let pieces = get_piece_info(&metainfo);
        task::spawn_local(async move {
            let _ = runner.await;
        });
        if let Some(ext_chans) = ext_chans {
            Self::seed_metainfo(
                index,
                download_chans,
                upload_chans,
                ext_chans,
                meta_storage,
                metainfo.size(),
            )
            .await;
        } else {
            Self::seed_content(index, download_chans, upload_chans, content_storage, pieces).await;
        }
    }
}

async fn listening_peer<P: Peer>(
    index: u8,
    listening_addr: SocketAddr,
    expected_remote_addr: SocketAddr,
    content_storage: data::StorageClient,
    metainfo_storage: data::StorageClient,
    metainfo: Rc<Metainfo>,
    mut extensions_enabled: bool,
) {
    let verify_remote_addr = !extensions_enabled;
    macro_rules! accept_and_run {
        ($listener:expr) => {{
            let (stream, remote_addr) = $listener.accept().await.unwrap();
            stream.set_nodelay(true).unwrap();
            println!(
                "Peer {} on {} accepted connection from {}",
                index, listening_addr, remote_addr
            );
            if verify_remote_addr {
                assert_eq!(remote_addr, expected_remote_addr);
            }
            let (download_chans, upload_chans, ext_chans, runner) =
                pwp::channels_for_inbound_connection(
                    &[index + b'0'; 20],
                    None,
                    extensions_enabled,
                    remote_addr,
                    stream,
                )
                .await
                .unwrap();
            P::run(
                index,
                download_chans,
                upload_chans,
                ext_chans,
                runner,
                content_storage.clone(),
                metainfo_storage.clone(),
                metainfo.clone(),
            )
            .await;
        }};
    }
    match TcpListener::bind(listening_addr).await {
        Ok(listener) => {
            accept_and_run!(listener);
            if extensions_enabled {
                extensions_enabled = false;
                accept_and_run!(listener);
            }
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
    content_storage: data::StorageClient,
    metainfo_storage: data::StorageClient,
    metainfo: Rc<Metainfo>,
) {
    println!("Peer {index} connecting to {remote_ip}...");
    let start_time = time::Instant::now();
    let stream = loop {
        let connect_result = time::timeout(sec!(10), TcpStream::connect(remote_ip)).await;
        if let Ok(Ok(result)) = connect_result {
            break result;
        }
        if start_time.elapsed() > sec!(10) {
            panic!("Peer {index} failed to connect within 10 sec");
        }
        time::sleep(sec!(1)).await;
    };
    stream.set_nodelay(true).unwrap();
    let (download_chans, upload_chans, ext_chans, runner) = pwp::channels_for_outbound_connection(
        &[index + b'0'; 20],
        &info_hash,
        false,
        remote_ip,
        stream,
        None,
    )
    .await
    .expect("connecting peer failed to create channels");
    println!("Peer {index} connected to {remote_ip}");
    P::run(
        index,
        download_chans,
        upload_chans,
        ext_chans,
        runner,
        content_storage,
        metainfo_storage,
        metainfo,
    )
    .await;
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
    metainfo_file: &str,
    files_parentdir: impl AsRef<Path>,
    mode: ConnectionMode,
    extensions_enabled: bool,
) {
    let metainfo = Rc::new(startup::read_metainfo(metainfo_file).unwrap());

    assert!(files_parentdir.as_ref().is_dir() == P::NEEDS_INPUT_DATA);

    let (content_storage, content_storage_server) =
        startup::create_content_storage(&metainfo, &files_parentdir).unwrap();
    let (meta_storage, meta_storage_server) =
        startup::create_metainfo_storage(metainfo_file).unwrap();

    let task_set = task::LocalSet::new();
    task_set.spawn_local(async move {
        join!(content_storage_server.run(), meta_storage_server.run());
    });

    let info_hash = *metainfo.info_hash();
    let remote_ip = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), ip::port_from_hash(&metainfo_file));
    match mode {
        ConnectionMode::Outgoing { num_peers } => {
            task_set
                .run_until(future::join_all((0..num_peers).map(|peer_index| {
                    connecting_peer::<P>(
                        peer_index as u8,
                        remote_ip,
                        info_hash,
                        content_storage.clone(),
                        meta_storage.clone(),
                        metainfo.clone(),
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
                            remote_ip,
                            content_storage.clone(),
                            meta_storage.clone(),
                            metainfo.clone(),
                            extensions_enabled,
                        )
                    },
                )))
                .await;
        }
    }
}

async fn start_tracker<'a>(
    port: u16,
    peers: impl Iterator<Item = &'a SocketAddr>,
    info_hash: &str,
) -> (mockito::Server, mockito::Mock) {
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
        benc::Element::Dictionary(root).to_bytes()
    };

    let mut server = mockito::Server::new_with_opts_async(mockito::ServerOpts {
        port,
        ..Default::default()
    })
    .await;

    let mock = server
        .mock("GET", "/announce")
        .match_query(mockito::Matcher::Regex(format!(".*info_hash={info_hash}.*")))
        .with_status(200)
        .with_body(response)
        .create_async()
        .await;

    (server, mock)
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
                        panic!("{name1} and {name2} are different");
                    }
                    if f1_read_len == 0 {
                        break;
                    }
                    if buff1[0..f1_read_len] != buff2[0..f2_read_len] {
                        panic!("{name1} and {name2} are different");
                    }
                }
                _ => panic!(),
            }
        }
    }
    for (input, output) in iter::zip(
        fs::read_dir(input_dir).unwrap().filter(|item| {
            item.as_ref().is_ok_and(|dir_entry| dir_entry.file_name() != ".mtorrent")
        }),
        fs::read_dir(output_dir).unwrap().filter(|item| {
            item.as_ref().is_ok_and(|dir_entry| dir_entry.file_name() != ".mtorrent")
        }),
    ) {
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

macro_rules! with_30s_timeout {
    ($fut:expr, $proc:expr) => {
        if tokio::time::timeout(sec!(30), $fut).await.is_err() {
            let _ = $proc.kill();
            panic!("failed to finish in 60s");
        }
    };
}

#[tokio::test]
async fn test_download_and_upload_multifile_torrent() {
    // these tests need to run sequentially because mtorrent
    // always listens on the same port for a given torrent
    let metainfo_file = "tests/assets/screenshots.torrent";
    let torrent_name = "screenshots";
    {
        let output_dir = "test_accept_50_seeders_and_download_multifile_torrent";
        let data_dir = "tests/assets/screenshots";

        let mut mtorrent = process::Command::new(env!("CARGO_BIN_EXE_mtorrent"))
            .arg(metainfo_file)
            .arg("-o")
            .arg(output_dir)
            .arg("--no-upnp")
            .arg("--no-dht")
            .spawn()
            .expect("failed to execute 'mtorrent'");

        with_30s_timeout!(
            launch_peers::<Seeder>(
                metainfo_file,
                data_dir,
                ConnectionMode::Outgoing { num_peers: 50 },
                false,
            ),
            mtorrent
        );

        let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrent'");
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

        let (_server, tracker_mock) = start_tracker(
            tracker_port,
            seeder_ips.iter(),
            "%FA%A75%97%E7%99h%E1%94o%CB%22%3E%27J%A5%BB%7D.%DA",
        )
        .await;

        let mtorrent = task::spawn(async move {
            time::sleep(sec!(2)).await; // wait for listening peers to launch
            process::Command::new(env!("CARGO_BIN_EXE_mtorrent"))
                .arg(metainfo_file)
                .arg("-o")
                .arg(output_dir)
                .arg("--no-upnp")
                .arg("--no-dht")
                .spawn()
                .expect("failed to execute 'mtorrent'")
        });

        with_30s_timeout!(
            launch_peers::<Seeder>(
                metainfo_file,
                data_dir,
                ConnectionMode::Incoming {
                    listen_addrs: seeder_ips,
                },
                false,
            ),
            mtorrent.await.unwrap()
        );

        let mtorrent_ecode = mtorrent.await.unwrap().wait().expect("failed to wait on 'mtorrent'");
        assert!(mtorrent_ecode.success());

        tracker_mock.assert_async().await;

        compare_input_and_output(data_dir, output_dir, torrent_name);
        std::fs::remove_dir_all(output_dir).unwrap();
    }
    {
        let output_dir = "test_accept_1_leech_and_upload_multifile_torrent";
        let data_dir = "tests/assets";

        let mut mtorrent = process::Command::new(env!("CARGO_BIN_EXE_mtorrent"))
            .arg(metainfo_file)
            .arg("-o")
            .arg(data_dir)
            .arg("--no-upnp")
            .arg("--no-dht")
            .spawn()
            .expect("failed to execute 'mtorrent'");

        with_30s_timeout!(
            launch_peers::<Leech>(
                metainfo_file,
                output_dir,
                ConnectionMode::Outgoing { num_peers: 1 },
                false,
            ),
            mtorrent
        );

        let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrent'");
        assert!(mtorrent_ecode.success());

        compare_input_and_output(output_dir, data_dir, torrent_name);
        std::fs::remove_dir_all(output_dir).unwrap();
    }
    let _ = std::fs::remove_file("tests/assets/.mtorrent");
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

        let mut mtorrent = process::Command::new(env!("CARGO_BIN_EXE_mtorrent"))
            .arg(metainfo_file)
            .arg("-o")
            .arg(output_dir)
            .arg("--no-upnp")
            .arg("--no-dht")
            .spawn()
            .expect("failed to execute 'mtorrent'");

        with_30s_timeout!(
            launch_peers::<Seeder>(
                metainfo_file,
                data_dir,
                ConnectionMode::Outgoing { num_peers: 50 },
                false,
            ),
            mtorrent
        );

        let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrent'");
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

        let (_server, tracker_mock) = start_tracker(
            tracker_port,
            seeder_ips.iter(),
            "%8Ee%F2d%F5%C8%17%FE%D6G_%2F%A8%A9%1E%81%0DB1%A3",
        )
        .await;

        let mtorrent = task::spawn(async move {
            time::sleep(sec!(2)).await; // wait for listening peers to launch
            process::Command::new(env!("CARGO_BIN_EXE_mtorrent"))
                .arg(metainfo_file)
                .arg("-o")
                .arg(output_dir)
                .arg("--no-upnp")
                .arg("--no-dht")
                .spawn()
                .expect("failed to execute 'mtorrent'")
        });

        with_30s_timeout!(
            launch_peers::<Seeder>(
                metainfo_file,
                data_dir,
                ConnectionMode::Incoming {
                    listen_addrs: seeder_ips,
                },
                false,
            ),
            mtorrent.await.unwrap()
        );

        let mtorrent_ecode = mtorrent.await.unwrap().wait().expect("failed to wait on 'mtorrent'");
        assert!(mtorrent_ecode.success());

        tracker_mock.assert_async().await;

        compare_input_and_output(data_dir, output_dir, torrent_name);
        std::fs::remove_dir_all(output_dir).unwrap();
    }
    {
        let output_dir = "test_accept_1_leech_and_upload_monofile_torrent";
        let data_dir = "tests/assets";

        let mut mtorrent = process::Command::new(env!("CARGO_BIN_EXE_mtorrent"))
            .arg(metainfo_file)
            .arg("-o")
            .arg(data_dir)
            .arg("--no-upnp")
            .arg("--no-dht")
            .spawn()
            .expect("failed to execute 'mtorrent'");

        with_30s_timeout!(
            launch_peers::<Leech>(
                metainfo_file,
                output_dir,
                ConnectionMode::Outgoing { num_peers: 1 },
                false,
            ),
            mtorrent
        );

        let mtorrent_ecode = mtorrent.wait().expect("failed to wait on 'mtorrent'");
        assert!(mtorrent_ecode.success());

        compare_input_and_output(output_dir, data_dir, torrent_name);
        std::fs::remove_dir_all(output_dir).unwrap();
    }
    let _ = std::fs::remove_file("tests/assets/.mtorrent");
}

#[tokio::test]
async fn test_download_torrent_from_magnet_link() {
    let output_dir = "test_download_torrent_from_magnet_link";
    let data_dir = "tests/assets/screenshots";
    fs::create_dir_all(output_dir).unwrap();

    let metainfo_file = "tests/assets/screenshots_bad_tracker.torrent";
    let torrent_name = "screenshots";

    let peer_ips = (50100u16..50150u16)
        .map(|port| SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)))
        .collect::<Vec<_>>();

    let magnet_link = {
        let mut tmp = "magnet:?xt=urn:btih:faa73597e79968e1946fcb223e274aa5bb7d2eda&dn=screenshots"
            .to_string();
        for peer_addr in &peer_ips {
            tmp.push_str("&x.pe=");
            tmp.push_str(&peer_addr.to_string());
        }
        tmp
    };

    let mtorrent = task::spawn(async move {
        time::sleep(sec!(2)).await; // wait for listening peers to launch
        process::Command::new(env!("CARGO_BIN_EXE_mtorrent"))
            .arg(magnet_link)
            .arg("-o")
            .arg(output_dir)
            .arg("--no-upnp")
            .arg("--no-dht")
            .spawn()
            .expect("failed to execute 'mtorrent'")
    });

    with_30s_timeout!(
        launch_peers::<Seeder>(
            metainfo_file,
            data_dir,
            ConnectionMode::Incoming {
                listen_addrs: peer_ips
            },
            true
        ),
        mtorrent.await.unwrap()
    );

    let mtorrent_ecode = mtorrent.await.unwrap().wait().expect("failed to wait on 'mtorrent'");
    assert!(mtorrent_ecode.success());

    compare_input_and_output(data_dir, output_dir, torrent_name);
    fs::remove_dir_all(output_dir).unwrap();
}
