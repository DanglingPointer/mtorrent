use crate::pwp::MAX_BLOCK_SIZE;
use crate::utils::peer_id::PeerId;
use crate::utils::{fifo, magnet, startup};
use crate::{data, msgs};
use crate::{ops::ctx, pwp, sec};
use futures::future::LocalBoxFuture;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::{fs, iter, panic};
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::{join, runtime, task, time};
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

// ------------------------------------------------------------------------------------------------

trait PeerSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T> PeerSocket for T where T: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

#[derive(Default)]
struct PeerBuilder {
    socket: Option<Box<dyn PeerSocket>>,
    metainfo_uri: Option<String>,
    remote_ip: Option<SocketAddr>,
    local_ip: Option<SocketAddr>,
    remote_peer_id: Option<PeerId>,
    local_peer_id: Option<PeerId>,
    content_path: Option<PathBuf>,
    extensions_enabled: bool,
    has_all_pieces: bool,
}

#[allow(dead_code)]
impl PeerBuilder {
    fn new() -> Self {
        let orig_hook = panic::take_hook();
        panic::set_hook(Box::new(move |panic_info| {
            orig_hook(panic_info);
            std::process::exit(1);
        }));
        Self::default()
    }
    fn with_socket(mut self, socket: impl PeerSocket) -> Self {
        self.socket = Some(Box::new(socket));
        self
    }
    fn with_metainfo_file(mut self, metainfo_path: &str) -> Self {
        self.metainfo_uri = Some(metainfo_path.to_owned());
        self
    }
    fn with_magnet_link(mut self, magnet_link: &str) -> Self {
        self.metainfo_uri = Some(magnet_link.to_owned());
        self
    }
    fn with_remote_ip(mut self, ip: SocketAddr) -> Self {
        self.remote_ip = Some(ip);
        self
    }
    fn with_local_ip(mut self, ip: SocketAddr) -> Self {
        self.local_ip = Some(ip);
        self
    }
    fn with_remote_peer_id(mut self, id: impl Into<PeerId>) -> Self {
        self.remote_peer_id = Some(id.into());
        self
    }
    fn with_local_peer_id(mut self, id: impl Into<PeerId>) -> Self {
        self.local_peer_id = Some(id.into());
        self
    }
    fn with_content_storage(mut self, data_path: impl AsRef<Path>) -> Self {
        self.content_path = Some(data_path.as_ref().into());
        self
    }
    fn with_extensions(mut self) -> Self {
        self.extensions_enabled = true;
        self
    }
    fn with_all_pieces(mut self) -> Self {
        self.has_all_pieces = true;
        self
    }
    #[must_use]
    fn build_main(self) -> (ctx::Handle<ctx::MainCtx>, LocalBoxFuture<'static, io::Result<()>>) {
        let metainfo = startup::read_metainfo(
            self.metainfo_uri.as_deref().unwrap_or("tests/assets/zeroed_example.torrent"),
        )
        .unwrap();
        let content_storage = if let Some(content_path) = self.content_path {
            let (client, content_storage_server) =
                startup::create_content_storage(&metainfo, content_path).unwrap();
            task::spawn(async move {
                content_storage_server.run().await;
            });
            client
        } else {
            data::new_mock_storage(usize::MAX)
        };
        let metainfo_storage = match (self.extensions_enabled, self.metainfo_uri) {
            (true, Some(metainfo_path)) => {
                let (client, metainfo_storage_server) =
                    startup::create_metainfo_storage(metainfo_path).unwrap();
                task::spawn(async move {
                    metainfo_storage_server.run().await;
                });
                client
            }
            _ => data::new_mock_storage(usize::MAX),
        };
        let (dlchans, ulchans, extchans) = pwp::channels_from_mock(
            self.remote_ip.unwrap_or(SocketAddr::new([0, 0, 0, 0].into(), 6666)),
            pwp::Handshake {
                peer_id: *self.remote_peer_id.unwrap_or(PeerId::from(&[b'r'; 20])),
                info_hash: *metainfo.info_hash(),
                reserved: pwp::reserved_bits(self.extensions_enabled),
            },
            self.extensions_enabled,
            self.socket.unwrap_or_else(|| Box::new(io::empty())),
        );
        let mut ctx_handle = ctx::MainCtx::new(
            metainfo,
            self.local_peer_id.unwrap_or(PeerId::from(&[b'l'; 20])),
            self.local_ip.unwrap_or(SocketAddr::new([0, 0, 0, 0].into(), 7777)),
        )
        .unwrap();
        if self.has_all_pieces {
            ctx_handle.with_ctx(|ctx| {
                let piece_count = ctx.pieces.piece_count();
                for piece_index in 0..piece_count {
                    assert!(ctx.accountant.submit_piece(piece_index));
                    ctx.piece_tracker.forget_piece(piece_index);
                }
            });
        }
        let (sink, _src) = fifo::channel();
        let run_future = super::run_peer_connection(
            dlchans,
            ulchans,
            extchans,
            content_storage,
            metainfo_storage,
            ctx_handle.clone(),
            sink,
        );

        (ctx_handle, Box::pin(run_future))
    }
    #[must_use]
    fn build_preliminary(
        self,
    ) -> (ctx::Handle<ctx::PreliminaryCtx>, LocalBoxFuture<'static, io::Result<()>>) {
        let magnet_link = self.metainfo_uri.unwrap_or(
            "magnet:?xt=urn:btih:77c09d63baf907ceeed366e2bdd687ca37cc4098&dn=Star.Trek.Voyager.S05.DVDRip.x264-MARS%5brartv%5d&tr=http%3a%2f%2ftracker.trackerfix.com%3a80%2fannounce&tr=udp%3a%2f%2f9.rarbg.me%3a2750%2fannounce&tr=udp%3a%2f%2f9.rarbg.to%3a2930%2fannounce"
                .to_owned());

        let magnet_link: magnet::MagnetLink = magnet_link.parse().unwrap();

        let (dlchans, ulchans, extchans) = pwp::channels_from_mock(
            self.remote_ip.unwrap_or(SocketAddr::new([0, 0, 0, 0].into(), 6666)),
            pwp::Handshake {
                peer_id: *self.remote_peer_id.unwrap_or(PeerId::from(&[b'r'; 20])),
                info_hash: *magnet_link.info_hash(),
                reserved: pwp::reserved_bits(true),
            },
            true,
            self.socket.unwrap_or_else(|| Box::new(io::empty())),
        );

        let ctx_handle = ctx::PreliminaryCtx::new(
            magnet_link,
            self.local_peer_id.unwrap_or(PeerId::from(&[b'l'; 20])),
            self.local_ip.unwrap_or(SocketAddr::new([0, 0, 0, 0].into(), 7777)),
        );
        let run_future =
            super::run_metadata_download(dlchans, ulchans, extchans, ctx_handle.clone());

        (ctx_handle, Box::pin(run_future))
    }
}

// ------------------------------------------------------------------------------------------------

async fn pass_torrent_from_peer_to_peer(
    metainfo_filepath: &'static str,
    output_dir: &'static str,
    input_dir: &'static str,
) {
    let (downloader_sock, uploader_sock) = io::duplex(17 * 1024);

    let tasks = task::LocalSet::new();

    let (mut downloader_ctx_handle, downloader_fut) = PeerBuilder::new()
        .with_socket(downloader_sock)
        .with_metainfo_file(metainfo_filepath)
        .with_content_storage(output_dir)
        .build_main();

    let (_, uploader_fut) = PeerBuilder::new()
        .with_socket(uploader_sock)
        .with_all_pieces()
        .with_metainfo_file(metainfo_filepath)
        .with_content_storage(input_dir)
        .build_main();

    tasks.spawn_local(async move {
        let _ = join!(downloader_fut, uploader_fut);
    });
    tasks
        .run_until(time::timeout(sec!(30), async move {
            loop {
                let finished = downloader_ctx_handle.with_ctx(|ctx| {
                    ctx.accountant.missing_bytes() == 0
                        && ctx.piece_tracker.get_rarest_pieces().count() == 0
                });
                if finished {
                    break;
                } else {
                    time::sleep(sec!(1)).await;
                }
            }
        }))
        .await
        .unwrap();
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

    let (downloader_sock, uploader_sock) = io::duplex(17 * 1024);

    let tasks = task::LocalSet::new();

    let (mut downloader_ctx_handle, downloader_fut) = PeerBuilder::new()
        .with_socket(downloader_sock)
        .with_magnet_link(magnet)
        .build_preliminary();

    let (_, uploader_fut) = PeerBuilder::new()
        .with_socket(uploader_sock)
        .with_metainfo_file(metainfo_filepath)
        .with_extensions()
        .build_main();

    tasks.spawn_local(uploader_fut);
    tasks
        .run_until(time::timeout(sec!(30), async move {
            let _ = downloader_fut.await;
            downloader_ctx_handle.with_ctx(|ctx| {
                assert!(!ctx.metainfo_pieces.is_empty());
                assert!(ctx.metainfo_pieces.all());
                assert_eq!(metainfo_content, ctx.metainfo);
            });
        }))
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn test_send_extended_handshake_before_bitfield() {
    let _ = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent::ops", log::LevelFilter::Debug)
        .with_module_level("mtorrent::pwp::channels", log::LevelFilter::Trace)
        .init();

    let metainfo_filepath = "tests/assets/screenshots.torrent";
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
                request_limit: Some(super::MAX_PENDING_REQUESTS),
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
