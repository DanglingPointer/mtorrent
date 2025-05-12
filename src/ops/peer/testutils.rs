use crate::data;
use crate::utils::peer_id::PeerId;
use crate::utils::{magnet, startup};
use crate::{ops::ctx, pwp};
use futures::future::LocalBoxFuture;
use local_async_utils::{local_sync, shared::Shared};
use std::fmt::Debug;
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::{fs, iter, panic};
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::sync::broadcast;
use tokio::task;

pub fn compare_input_and_output(
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
    for (input, output) in iter::zip(
        fs::read_dir(input_dir).unwrap().filter(|item| {
            item.as_ref().is_ok_and(|dir_entry| dir_entry.file_name() != ".mtorrent")
        }),
        fs::read_dir(output_dir).unwrap(),
    ) {
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

pub trait PeerSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T> PeerSocket for T where T: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

#[derive(Default)]
pub struct PeerBuilder {
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

impl PeerBuilder {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_socket(mut self, socket: impl PeerSocket) -> Self {
        self.socket = Some(Box::new(socket));
        self
    }
    pub fn with_metainfo_file(mut self, metainfo_path: &str) -> Self {
        self.metainfo_uri = Some(metainfo_path.to_owned());
        self
    }
    pub fn with_magnet_link(mut self, magnet_link: &str) -> Self {
        self.metainfo_uri = Some(magnet_link.to_owned());
        self
    }
    pub fn with_remote_ip(mut self, ip: SocketAddr) -> Self {
        self.remote_ip = Some(ip);
        self
    }
    pub fn with_local_ip(mut self, ip: SocketAddr) -> Self {
        self.local_ip = Some(ip);
        self
    }
    #[expect(dead_code)]
    pub fn with_remote_peer_id(mut self, id: impl Into<PeerId>) -> Self {
        self.remote_peer_id = Some(id.into());
        self
    }
    #[expect(dead_code)]
    pub fn with_local_peer_id(mut self, id: impl Into<PeerId>) -> Self {
        self.local_peer_id = Some(id.into());
        self
    }
    pub fn with_content_storage(mut self, data_path: impl AsRef<Path>) -> Self {
        self.content_path = Some(data_path.as_ref().into());
        self
    }
    pub fn with_extensions(mut self) -> Self {
        self.extensions_enabled = true;
        self
    }
    pub fn with_all_pieces(mut self) -> Self {
        self.has_all_pieces = true;
        self
    }
    #[must_use]
    pub fn build_main(
        self,
    ) -> (ctx::Handle<ctx::MainCtx>, LocalBoxFuture<'static, io::Result<()>>) {
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
        let local_addr = self.local_ip.unwrap_or(SocketAddr::new([0, 0, 0, 0].into(), 7777));
        let mut ctx_handle = ctx::MainCtx::new(
            metainfo,
            self.local_peer_id.unwrap_or(PeerId::from(&[b'l'; 20])),
            local_addr,
            local_addr.port(),
        )
        .unwrap();
        if self.has_all_pieces {
            ctx_handle.with(|ctx| {
                let piece_count = ctx.pieces.piece_count();
                for piece_index in 0..piece_count {
                    assert!(ctx.accountant.submit_piece(piece_index));
                    ctx.piece_tracker.forget_piece(piece_index);
                }
            });
        }
        let (sink, _src) = local_sync::channel();

        let ctx_handle_clone = ctx_handle.clone();
        let run_future = async move {
            let data = super::MainConnectionData {
                content_storage,
                metainfo_storage,
                ctx_handle: ctx_handle.clone(),
                pwp_worker_handle: tokio::runtime::Handle::current(),
                peer_discovered_channel: sink,
                piece_downloaded_channel: Rc::new(broadcast::Sender::new(1024)),
            };
            super::run_peer_connection(dlchans, ulchans, extchans, &data).await
        };

        (ctx_handle_clone, Box::pin(run_future))
    }
    #[must_use]
    pub fn build_preliminary(
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

        let local_addr = self.local_ip.unwrap_or(SocketAddr::new([0, 0, 0, 0].into(), 7777));
        let ctx_handle = ctx::PreliminaryCtx::new(
            magnet_link,
            self.local_peer_id.unwrap_or(PeerId::from(&[b'l'; 20])),
            local_addr,
            local_addr.port(),
        );
        let run_future =
            super::run_metadata_download(dlchans, ulchans, extchans, ctx_handle.clone());

        (ctx_handle, Box::pin(run_future))
    }
}

// ------------------------------------------------------------------------------------------------

pub fn setup(log_all_msgs: bool) {
    // make sure test fails on panic inside tokio runtime
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        orig_hook(panic_info);
        std::process::exit(1);
    }));

    let mut logger = simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .with_module_level("mtorrent::ops", log::LevelFilter::Debug);
    if log_all_msgs {
        logger = logger.with_module_level("mtorrent::pwp::channels", log::LevelFilter::Trace);
    }
    let _ = logger.init();
}
