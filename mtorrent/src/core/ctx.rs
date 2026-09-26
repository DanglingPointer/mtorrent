use super::ctrl;
use crate::app::main::{DownloadStrategy, Mode, Outcome};
use crate::utils::disk;
use crate::utils::listener::{
    BytesSnapshot, MetainfoSnapshot, PiecesSnapshot, RequestsSnapshot, StateListener, StateSnapshot,
};
use local_async_utils::prelude::*;
use mtorrent_base::{data, input, pwp};
use mtorrent_utils::peer_id::PeerId;
use serde::Deserialize;
use std::collections::HashSet;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::time::Duration;
use std::{fs, io, mem};
use tokio::time::Instant;
use tokio::{select, time};

pub type Handle<C> = LocalShared<C>;

macro_rules! define_with_ctx {
    ($handle:expr) => {
        macro_rules! with_ctx {
            ($f:expr) => {{
                use local_async_utils::prelude::*;
                $handle.with(
                    #[inline(always)]
                    $f,
                )
            }};
        }
    };
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum PwpMode {
    TcpOnly,
    UtpOnly,
    Any,
}

fn get_outbound_pwp_mode() -> PwpMode {
    use serde::de::value::{Error, StringDeserializer};

    if let Ok(v) = std::env::var("MTORRENT_PWP_MODE")
        && let Ok(mode) = PwpMode::deserialize(StringDeserializer::<Error>::new(v))
    {
        mode
    } else {
        PwpMode::Any
    }
}

#[derive(Clone)]
pub(super) struct ConstData {
    local_peer_id: PeerId,
    pwp_external_port: u16,
    pwp_internal_port: u16,
    local_ip_v4: Ipv4Addr,
    local_ip_v6: Ipv6Addr,
    bind_interface: Option<String>,
    outbound_pwp_mode: PwpMode,
    download_strategy: DownloadStrategy,
    mode: Mode,
}

impl ConstData {
    pub(super) fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }
    pub(super) fn pwp_external_port(&self) -> u16 {
        self.pwp_external_port
    }
    pub(super) fn pwp_internal_port(&self) -> u16 {
        self.pwp_internal_port
    }
    pub(super) fn local_ip_v4(&self) -> Ipv4Addr {
        self.local_ip_v4
    }
    pub(super) fn local_ip_v6(&self) -> Ipv6Addr {
        self.local_ip_v6
    }
    pub(super) fn bind_interface(&self) -> Option<&str> {
        self.bind_interface.as_deref()
    }
    pub(super) fn pwp_outbound_tcp_allowed(&self) -> bool {
        matches!(self.outbound_pwp_mode, PwpMode::Any | PwpMode::TcpOnly)
    }
    pub(super) fn pwp_outbound_utp_allowed(&self) -> bool {
        matches!(self.outbound_pwp_mode, PwpMode::Any | PwpMode::UtpOnly)
    }
    pub(super) fn download_strategy(&self) -> DownloadStrategy {
        self.download_strategy
    }
    pub(super) fn mode(&self) -> Mode {
        self.mode
    }
}

pub struct PreliminaryCtx {
    pub(super) magnet: input::MagnetLink,
    pub(super) metainfo: Vec<u8>,
    pub(super) metainfo_pieces: pwp::Bitfield,
    pub(super) discovered_peers: HashSet<SocketAddr>,
    pub(super) peer_states: pwp::PeerStates,
    pub(super) const_data: ConstData,
}

impl PreliminaryCtx {
    pub fn new(
        magnet: input::MagnetLink,
        local_peer_id: PeerId,
        pwp_external_port: u16,
        pwp_internal_port: u16,
        local_ip_v4: Ipv4Addr,
        local_ip_v6: Ipv6Addr,
        bind_interface: Option<String>,
    ) -> Handle<Self> {
        Handle::new(Self {
            magnet,
            metainfo: Vec::new(),
            metainfo_pieces: pwp::Bitfield::new(),
            discovered_peers: Default::default(),
            peer_states: Default::default(),
            const_data: ConstData {
                local_peer_id,
                pwp_external_port,
                pwp_internal_port,
                local_ip_v4,
                local_ip_v6,
                bind_interface,
                outbound_pwp_mode: get_outbound_pwp_mode(),
                download_strategy: Default::default(), // unused
                mode: Default::default(),              // unused
            },
        })
    }
}

pub struct MainCtx {
    pub(super) pieces: Rc<data::PieceInfo>,
    pub(super) accountant: data::BlockAccountant,
    pub(super) piece_tracker: data::PieceTracker,
    pub(super) metainfo: input::Metainfo,
    pub(super) peer_states: pwp::PeerStates,
    pub(super) pending_requests: data::PendingRequests,
    pub(super) const_data: ConstData,
}

impl MainCtx {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        metainfo: input::Metainfo,
        local_peer_id: PeerId,
        pwp_external_port: u16,
        pwp_internal_port: u16,
        local_ip_v4: Ipv4Addr,
        local_ip_v6: Ipv6Addr,
        bind_interface: Option<String>,
        download_strategy: DownloadStrategy,
        mode: Mode,
    ) -> io::Result<Handle<Self>> {
        fn make_error(s: &'static str) -> impl FnOnce() -> io::Error {
            move || io::Error::new(io::ErrorKind::InvalidData, s)
        }
        let pieces = Rc::new(data::PieceInfo::new(
            metainfo.pieces().cloned(),
            metainfo.piece_length().ok_or_else(make_error("no piece length in metainfo"))?,
            metainfo
                .length()
                .or_else(|| metainfo.files().map(|it| it.map(|(len, _path)| len).sum()))
                .ok_or_else(make_error("no total length in metainfo"))?,
        )?);
        let accountant = data::BlockAccountant::new(pieces.clone());
        let piece_tracker = data::PieceTracker::new(pieces.piece_count());
        let ctx = Self {
            pieces,
            accountant,
            piece_tracker,
            metainfo,
            peer_states: Default::default(),
            pending_requests: Default::default(),
            const_data: ConstData {
                local_peer_id,
                pwp_external_port,
                pwp_internal_port,
                local_ip_v4,
                local_ip_v6,
                bind_interface,
                outbound_pwp_mode: get_outbound_pwp_mode(),
                download_strategy,
                mode,
            },
        };
        Ok(Handle::new(ctx))
    }
}

const COMPLETION_CHECK_INTERVAL: Duration = millisec!(500);

/// Returns `Ok(None)` if `cancel` resolved before the metadata was downloaded.
pub async fn supervise_metadata_download<L: StateListener>(
    ctx_handle: Handle<PreliminaryCtx>,
    metainfo_filepath: impl AsRef<Path>,
    state_listener: &mut L,
    mut cancel: Pin<&mut impl Future<Output = ()>>,
) -> io::Result<Option<impl IntoIterator<Item = SocketAddr> + 'static>> {
    define_with_ctx!(ctx_handle);

    fn save_metadata_if_complete(
        ctx: &mut PreliminaryCtx,
        metainfo_filepath: impl AsRef<Path>,
    ) -> io::Result<bool> {
        if ctrl::verify_metadata(ctx) {
            fs::write(metainfo_filepath, &ctx.metainfo)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    let mut snapshot_reporter =
        pin!(submit_snapshots_periodically(&ctx_handle, state_listener, preliminary_snapshot));

    let mut completion_check_timer = time::interval(COMPLETION_CHECK_INTERVAL);
    completion_check_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    loop {
        select! {
            biased;
            _ = &mut snapshot_reporter => unreachable!(),
            _ = completion_check_timer.tick() => {
                if with_ctx!(|ctx| save_metadata_if_complete(ctx, &metainfo_filepath))? {
                    return Ok(Some(with_ctx!(|ctx| mem::take(&mut ctx.discovered_peers))));
                }
            }
            _ = &mut cancel => {
                return Ok(None);
            }
        }
    }
}

pub async fn supervise_content_download<L: StateListener>(
    ctx_handle: Handle<MainCtx>,
    outputdir: impl AsRef<Path>,
    state_listener: &mut L,
    mut cancel: Pin<&mut impl Future<Output = ()>>,
) -> Outcome {
    define_with_ctx!(ctx_handle);

    let mut progress_file = disk::ProgressFile::open(&outputdir)
        .inspect_err(|e| log::error!("Failed to open progress file: {e}"))
        .ok();

    if let Some(file) = progress_file.as_mut() {
        with_ctx!(|ctx| match file.load_progress(ctx.metainfo.info_hash()) {
            Ok(mut state) => {
                state.resize(ctx.pieces.piece_count(), false);
                ctx.accountant.submit_bitfield(&state);
                for piece_index in state.iter_ones() {
                    ctx.piece_tracker.forget_piece(piece_index);
                }
            }
            Err(e) => {
                log::info!("Couldn't load saved progress: {e}");
            }
        });
    }

    let mut progress_persister = pin!(persist_progress_periodically(&ctx_handle, progress_file));

    let mut snapshot_reporter =
        pin!(submit_snapshots_periodically(&ctx_handle, state_listener, main_snapshot));

    let mut completion_check_timer = time::interval(COMPLETION_CHECK_INTERVAL);
    completion_check_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    loop {
        select! {
            biased;
            _ = &mut snapshot_reporter => unreachable!(),
            _ = completion_check_timer.tick() => {
                if with_ctx!(|ctx| ctrl::is_finished(ctx)) {
                    return Outcome::Finished;
                }
            }
            _ = &mut cancel => {
                return Outcome::Cancelled;
            }
            _ = &mut progress_persister => unreachable!(),
        }
    }
}

// ----------------------------------------------------------------------------

async fn persist_progress_periodically(
    ctx_handle: &Handle<MainCtx>,
    progress_file: Option<disk::ProgressFile>,
) -> ! {
    define_with_ctx!(ctx_handle);
    const PERSIST_INTERVAL: Duration = sec!(5);

    let Some(file) = progress_file else {
        loop {
            std::future::pending::<()>().await;
        }
    };

    let mut progress_saver = ProgressSaver {
        file,
        info_hash: with_ctx!(|ctx| *ctx.metainfo.info_hash()),
        get_bitfield: || with_ctx!(|ctx| ctx.accountant.generate_bitfield()),
        last_bitfield: with_ctx!(|ctx| ctx.accountant.generate_bitfield()),
    };

    let mut timer = time::interval_at(Instant::now() + PERSIST_INTERVAL, PERSIST_INTERVAL);
    timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    loop {
        timer.tick().await;
        progress_saver.persist_if_changed();
    }
}

struct ProgressSaver<F: Fn() -> pwp::Bitfield> {
    file: disk::ProgressFile,
    info_hash: [u8; 20],
    get_bitfield: F,
    last_bitfield: pwp::Bitfield,
}

impl<F: Fn() -> pwp::Bitfield> ProgressSaver<F> {
    fn persist_if_changed(&mut self) {
        let latest_bitfield = (self.get_bitfield)();
        if self.last_bitfield != latest_bitfield {
            self.last_bitfield = latest_bitfield;
            if let Err(e) = self.file.save_progress(&self.info_hash, self.last_bitfield.clone()) {
                log::error!("Failed to save progress to file: {e}");
            }
        }
    }
}

impl<F: Fn() -> pwp::Bitfield> Drop for ProgressSaver<F> {
    fn drop(&mut self) {
        self.persist_if_changed();
    }
}

// ----------------------------------------------------------------------------

async fn submit_snapshots_periodically<C, L: StateListener>(
    ctx_handle: &Handle<C>,
    state_listener: &mut L,
    generate_snapshot: fn(&C) -> StateSnapshot<'_>,
) -> ! {
    define_with_ctx!(ctx_handle);

    let mut timer = time::interval(L::INTERVAL);
    timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    loop {
        timer.tick().await;

        with_ctx!(|ctx| {
            let snapshot = generate_snapshot(ctx);
            state_listener.on_snapshot(snapshot);
        });
    }
}

fn preliminary_snapshot(ctx: &PreliminaryCtx) -> StateSnapshot<'_> {
    StateSnapshot {
        peers: ctx.peer_states.iter().map(|(addr, state)| (*addr, state)).collect(),
        metainfo: MetainfoSnapshot {
            total_pieces: ctx.metainfo_pieces.len(),
            downloaded_pieces: ctx.metainfo_pieces.count_ones(),
        },
        pieces: Default::default(),
        bytes: Default::default(),
        requests: Default::default(),
    }
}

fn main_snapshot(ctx: &MainCtx) -> StateSnapshot<'_> {
    let bitfield = ctx.accountant.generate_bitfield();
    let metadata_pieces = ctx.metainfo.size().div_ceil(pwp::MAX_BLOCK_SIZE);
    StateSnapshot {
        peers: ctx.peer_states.iter().map(|(addr, state)| (*addr, state)).collect(),
        pieces: PiecesSnapshot {
            total: bitfield.len(),
            downloaded: bitfield.count_ones(),
            bitfield,
        },
        bytes: BytesSnapshot {
            total: ctx.pieces.total_len(),
            downloaded: ctx.accountant.accounted_bytes(),
        },
        requests: RequestsSnapshot {
            in_flight: ctx.pending_requests.requests_in_flight(),
            distinct_pieces: ctx.pending_requests.pieces_requested(),
        },
        metainfo: MetainfoSnapshot {
            total_pieces: metadata_pieces,
            downloaded_pieces: metadata_pieces,
        },
    }
}

#[cfg(test)]
impl ConstData {
    pub(crate) fn new_stub() -> Self {
        Self {
            local_peer_id: PeerId::generate_new(),
            pwp_external_port: 12345,
            pwp_internal_port: 0,
            local_ip_v4: Ipv4Addr::LOCALHOST,
            local_ip_v6: Ipv6Addr::LOCALHOST,
            bind_interface: None,
            outbound_pwp_mode: PwpMode::Any,
            download_strategy: Default::default(),
            mode: Default::default(),
        }
    }
}
