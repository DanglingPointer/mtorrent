use super::{CriticalTask, ctrl};
use crate::app::main::{DownloadStrategy, Mode, Outcome};
use crate::utils::disk;
use crate::utils::listener::{
    BytesSnapshot, MetainfoSnapshot, PiecesSnapshot, RequestsSnapshot, StateListener, StateSnapshot,
};
use derive_more::{Deref, DerefMut};
use futures_util::{Stream, StreamExt};
use local_async_utils::prelude::*;
use mtorrent_base::{data, input, pwp};
use mtorrent_utils::info_stopwatch;
use mtorrent_utils::peer_id::PeerId;
use mtorrent_utils::task_watcher::Exit;
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
    critical_task_exits: impl Stream<Item = Exit<CriticalTask>> + Unpin,
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

    let mut critical_task_exits = critical_task_exits.fuse();
    let mut premature_exit = critical_task_exits.next();

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
                with_ctx!(|ctx| log::info!(
                    "Metadata download for torrent '{}' has been cancelled",
                    ctx.magnet.name().unwrap_or("unnamed")
                ));
                return Ok(None);
            }
            Some(exit) = &mut premature_exit => {
                return Err(premature_exit_error(exit));
            }
        }
    }
}

pub async fn supervise_content_download<L: StateListener>(
    ctx_handle: Handle<MainCtx>,
    state_listener: &mut L,
    mut cancel: Pin<&mut impl Future<Output = ()>>,
    critical_task_exits: impl Stream<Item = Exit<CriticalTask>> + Unpin,
) -> io::Result<Outcome> {
    define_with_ctx!(ctx_handle);

    let mut critical_task_exits = critical_task_exits.fuse();
    let mut premature_exit = critical_task_exits.next();

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
                    return Ok(Outcome::Finished);
                }
            }
            _ = &mut cancel => {
                with_ctx!(|ctx| log::info!(
                    "Content download for torrent '{}' has been cancelled",
                    ctx.metainfo.name().unwrap_or("unnamed")
                ));
                return Ok(Outcome::Cancelled);
            }
            Some(exit) = &mut premature_exit => {
                return Err(premature_exit_error(exit));
            }
        }
    }
}

pub async fn restore_and_persist_progress(
    ctx_handle: Handle<MainCtx>,
    outputdir: impl AsRef<Path>,
    content_storage: data::StorageClient,
) {
    define_with_ctx!(ctx_handle);

    let Ok(mut file) = disk::ProgressFile::open(&outputdir)
        .inspect_err(|e| log::error!("Failed to open progress file: {e}"))
    else {
        return;
    };

    let info_hash = with_ctx!(|ctx| *ctx.metainfo.info_hash());
    match file.load_progress(&info_hash) {
        Ok(state) => {
            apply_loaded_progress(&ctx_handle, state, &content_storage).await;
            let verified_pieces = with_ctx!(|ctx| ctrl::verified_pieces_bitfield(ctx));
            if let Err(e) = file.save_progress(&info_hash, verified_pieces) {
                log::error!("Failed to save progress to file: {e}");
            }
        }
        Err(e) => {
            log::info!("Couldn't load saved progress: {e}");
        }
    }

    const PERSIST_INTERVAL: Duration = sec!(5);

    let mut last_bitfield = with_ctx!(|ctx| ctrl::verified_pieces_bitfield(ctx));

    let mut persist_progress = CallOnDrop(move || {
        let latest_bitfield = with_ctx!(|ctx| ctrl::verified_pieces_bitfield(ctx));
        if last_bitfield != latest_bitfield {
            last_bitfield = latest_bitfield;
            if let Err(e) = file.save_progress(&info_hash, last_bitfield.clone()) {
                log::error!("Failed to save progress to file: {e}");
            }
        }
    });

    let mut timer = time::interval_at(Instant::now() + PERSIST_INTERVAL, PERSIST_INTERVAL);
    timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    loop {
        timer.tick().await;
        persist_progress();
    }
}

// ----------------------------------------------------------------------------

async fn apply_loaded_progress(
    ctx_handle: &Handle<MainCtx>,
    mut loaded_state: pwp::Bitfield,
    content_storage: &data::StorageClient,
) {
    async fn verify_piece(
        piece_index: usize,
        pieces: &data::PieceInfo,
        storage: &data::StorageClient,
    ) -> io::Result<()> {
        let length = pieces.piece_len(piece_index);
        let global_offset = pieces.global_offset(piece_index, 0, length)?;
        let expected_sha1 = pieces.hash_of_piece(piece_index)?;
        let sha1_matches = storage.verify_block(global_offset, length, *expected_sha1).await?;
        if sha1_matches {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("sha-1 mismatch for piece {piece_index}"),
            ))
        }
    }
    define_with_ctx!(ctx_handle);
    let pieces = with_ctx!(|ctx| ctx.pieces.clone());

    loaded_state.resize(pieces.piece_count(), false);
    let _sw = info_stopwatch!("Rechecking {} saved pieces", loaded_state.count_ones());

    // mark all loaded pieces as downloaded but not verified
    for piece_index in loaded_state.iter_ones() {
        with_ctx!(|ctx| ctx.accountant.submit_piece(piece_index));
    }

    // verify SHA-1 of every loaded piece
    for piece_index in loaded_state.iter_ones() {
        match verify_piece(piece_index, &pieces, content_storage).await {
            Ok(()) => with_ctx!(|ctx| {
                ctx.accountant.submit_piece(piece_index);
                ctx.piece_tracker.forget_piece(piece_index);
            }),
            Err(e) => {
                log::warn!("Verification of piece {piece_index} failed: {e}");
                with_ctx!(|ctx| ctx.accountant.remove_piece(piece_index));
            }
        }
    }
}

fn premature_exit_error(exit: Exit<CriticalTask>) -> io::Error {
    match exit {
        Exit::Completed(tag) => io::Error::other(format!("task {tag:?} completed prematurely")),
        Exit::Dropped(tag) => {
            io::Error::other(format!("task {tag:?} was dropped prematurely (panicked or aborted)"))
        }
    }
}

#[derive(Deref, DerefMut)]
struct CallOnDrop<F: FnMut()>(
    #[deref]
    #[deref_mut]
    F,
);

impl<F: FnMut()> Drop for CallOnDrop<F> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            (self.0)();
        }
    }
}

async fn submit_snapshots_periodically<C, L: StateListener>(
    ctx_handle: &Handle<C>,
    state_listener: &mut L,
    generate_snapshot: fn(&C) -> StateSnapshot<'_>,
) -> ! {
    define_with_ctx!(ctx_handle);

    let mut submit_snapshot = CallOnDrop(|| {
        with_ctx!(|ctx| {
            let snapshot = generate_snapshot(ctx);
            state_listener.on_snapshot(snapshot);
        })
    });

    let mut timer = time::interval(L::INTERVAL);
    timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    loop {
        timer.tick().await;
        submit_snapshot();
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
    let bitfield = ctx.accountant.downloaded_pieces_bitfield();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::startup;
    use tokio::task;

    /// Creates a directory and removes it on drop, even if the test panics.
    struct TestDir(&'static str);

    impl TestDir {
        fn create(path: &'static str) -> Self {
            fs::create_dir_all(path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(self.0);
        }
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_persist_only_verified_pieces() {
        let dir = "test_persist_only_verified_pieces";
        let _dir_guard = TestDir::create(dir);

        let metainfo =
            startup::read_metainfo("../mtorrent-cli/tests/assets/example.torrent").unwrap();
        let info_hash = *metainfo.info_hash();
        let handle = MainCtx::new(
            metainfo,
            PeerId::generate_new(),
            1234,
            12345,
            Ipv4Addr::LOCALHOST,
            Ipv6Addr::LOCALHOST,
            None,
            Default::default(),
            Default::default(),
        )
        .unwrap();
        let piece_count = handle.with(|ctx| ctx.pieces.piece_count());
        assert!(piece_count > 4);

        let mut persister = pin!(restore_and_persist_progress(
            handle.clone(),
            dir,
            data::new_mock_storage(usize::MAX)
        ));
        let _ = time::timeout(sec!(1), &mut persister).await;

        // pieces 0..4 downloaded, but only 0 and 2 verified
        handle.with(|ctx| {
            for piece_index in 0..4 {
                assert!(ctx.accountant.submit_piece(piece_index));
            }
            ctx.piece_tracker.forget_piece(0);
            ctx.piece_tracker.forget_piece(2);
        });
        let _ = time::timeout(sec!(5), &mut persister).await;

        let load_progress = || {
            let mut state =
                disk::ProgressFile::open(dir).unwrap().load_progress(&info_hash).unwrap();
            state.resize(piece_count, false);
            state
        };
        assert_eq!(load_progress().iter_ones().collect::<Vec<_>>(), vec![0, 2]);

        // verify piece 1
        handle.with(|ctx| ctx.piece_tracker.forget_piece(1));
        let _ = time::timeout(sec!(5), &mut persister).await;
        assert_eq!(load_progress().iter_ones().collect::<Vec<_>>(), vec![0, 1, 2]);
    }

    fn save_progress_with(dir: &str, handle: &Handle<MainCtx>, pieces: &[usize]) {
        let (info_hash, piece_count) =
            handle.with(|ctx| (*ctx.metainfo.info_hash(), ctx.pieces.piece_count()));
        let mut state = pwp::Bitfield::repeat(false, piece_count);
        for &piece_index in pieces {
            state.set(piece_index, true);
        }
        disk::ProgressFile::open(dir).unwrap().save_progress(&info_hash, state).unwrap();
    }

    fn downloaded_pieces(handle: &Handle<MainCtx>) -> Vec<usize> {
        handle.with(|ctx| ctx.accountant.downloaded_pieces_bitfield().iter_ones().collect())
    }

    fn verified_pieces(handle: &Handle<MainCtx>) -> Vec<usize> {
        handle.with(|ctx| ctrl::verified_pieces_bitfield(ctx).iter_ones().collect())
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_loaded_progress_accepts_valid_pieces() {
        let dir = "test_loaded_progress_accepts_valid_pieces";
        let _dir_guard = TestDir::create(dir);
        let handle = main_ctx();
        save_progress_with(dir, &handle, &[0, 2, 3]);

        let total_len = handle.with(|ctx| ctx.pieces.total_len());
        let storage = data::new_mock_storage(total_len);
        let mut task = pin!(restore_and_persist_progress(handle.clone(), dir, storage));
        let _ = time::timeout(sec!(1), &mut task).await;

        assert_eq!(downloaded_pieces(&handle), vec![0, 2, 3]);
        assert_eq!(verified_pieces(&handle), vec![0, 2, 3]);
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_loaded_pieces_marked_downloaded_before_verification() {
        let dir = "test_loaded_pieces_marked_downloaded_before_verification";
        let _dir_guard = TestDir::create(dir);
        let handle = main_ctx();
        save_progress_with(dir, &handle, &[0, 2, 3]);

        let total_len = handle.with(|ctx| ctx.pieces.total_len());
        let storage = data::new_mock_storage(total_len);
        let mut restorer =
            tokio_test::task::spawn(restore_and_persist_progress(handle.clone(), dir, storage));

        // first poll stops at the first verification request, before the storage replies
        tokio_test::assert_pending!(restorer.poll());
        assert_eq!(downloaded_pieces(&handle), vec![0, 2, 3]);
        assert!(verified_pieces(&handle).is_empty());

        // let the storage reply to all verification requests
        for _ in 0..3 {
            task::yield_now().await;
            tokio_test::assert_pending!(restorer.poll());
        }
        assert_eq!(downloaded_pieces(&handle), vec![0, 2, 3]);
        assert_eq!(verified_pieces(&handle), vec![0, 2, 3]);

        drop(restorer);
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_download_not_finished_until_loaded_pieces_verified() {
        let dir = "test_download_not_finished_until_loaded_pieces_verified";
        let _dir_guard = TestDir::create(dir);
        let handle = main_ctx();
        let (total_len, piece_count) =
            handle.with(|ctx| (ctx.pieces.total_len(), ctx.pieces.piece_count()));
        let all_pieces: Vec<usize> = (0..piece_count).collect();
        save_progress_with(dir, &handle, &all_pieces);

        let storage = data::new_mock_storage(total_len);
        let mut restorer =
            tokio_test::task::spawn(restore_and_persist_progress(handle.clone(), dir, storage));

        // all pieces are downloaded, but none verified yet
        tokio_test::assert_pending!(restorer.poll());
        assert_eq!(downloaded_pieces(&handle), all_pieces);
        assert!(!handle.with(|ctx| ctrl::is_finished(ctx)));

        // let the storage reply to all verification requests
        for _ in 0..piece_count {
            assert!(!handle.with(|ctx| ctrl::is_finished(ctx)));
            tokio::task::yield_now().await;
            tokio_test::assert_pending!(restorer.poll());
        }
        assert_eq!(verified_pieces(&handle), all_pieces);
        assert!(handle.with(|ctx| ctrl::is_finished(ctx)));

        drop(restorer);
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_loaded_pieces_not_requested_during_verification() {
        let dir = "test_loaded_pieces_not_requested_during_verification";
        let _dir_guard = TestDir::create(dir);
        let handle = main_ctx();
        save_progress_with(dir, &handle, &[0, 2]);

        let peer = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 6666);
        handle.with(|ctx| {
            for piece_index in 0..3 {
                ctx.piece_tracker.add_single_record(&peer, piece_index);
            }
        });

        let total_len = handle.with(|ctx| ctx.pieces.total_len());
        let storage = data::new_mock_storage(total_len);
        let mut restorer =
            tokio_test::task::spawn(restore_and_persist_progress(handle.clone(), dir, storage));

        // verification is pending, but the loaded pieces are not requested
        tokio_test::assert_pending!(restorer.poll());
        assert!(verified_pieces(&handle).is_empty());
        assert_eq!(handle.with(|ctx| ctrl::next_piece_to_request(&peer, ctx)), Some(1));

        drop(restorer);
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_corrupted_pieces_removed_from_progress_file() {
        let dir = "test_corrupted_pieces_removed_from_progress_file";
        let _dir_guard = TestDir::create(dir);
        let handle = main_ctx();
        save_progress_with(dir, &handle, &[0, 2, 3]);

        let (info_hash, piece_count, total_len, piece_len) = handle.with(|ctx| {
            (
                *ctx.metainfo.info_hash(),
                ctx.pieces.piece_count(),
                ctx.pieces.total_len(),
                ctx.pieces.piece_len(0),
            )
        });
        let corrupted_offset = 2 * piece_len;
        let storage = data::new_mock_storage_with_verifier(total_len, move |offset, _len| {
            offset != corrupted_offset
        });
        let mut task = pin!(restore_and_persist_progress(handle.clone(), dir, storage));
        // shorter than the periodic save interval
        let _ = time::timeout(sec!(1), &mut task).await;

        let mut state = disk::ProgressFile::open(dir).unwrap().load_progress(&info_hash).unwrap();
        state.resize(piece_count, false);
        assert_eq!(state.iter_ones().collect::<Vec<_>>(), vec![0, 3]);
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_loaded_progress_discards_corrupted_pieces() {
        let dir = "test_loaded_progress_discards_corrupted_pieces";
        let _dir_guard = TestDir::create(dir);
        let handle = main_ctx();
        save_progress_with(dir, &handle, &[0, 2, 3]);

        let (total_len, piece_len) =
            handle.with(|ctx| (ctx.pieces.total_len(), ctx.pieces.piece_len(0)));
        let corrupted_offset = 2 * piece_len;
        let storage = data::new_mock_storage_with_verifier(total_len, move |offset, _len| {
            offset != corrupted_offset
        });
        let mut task = pin!(restore_and_persist_progress(handle.clone(), dir, storage));
        let _ = time::timeout(sec!(1), &mut task).await;

        assert_eq!(downloaded_pieces(&handle), vec![0, 3]);
        assert_eq!(verified_pieces(&handle), vec![0, 3]);
        let piece_2_missing = handle.with(|ctx| ctx.piece_tracker.missing_pieces_bitfield()[2]);
        assert!(piece_2_missing);
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_loaded_progress_verifies_last_piece_with_correct_length() {
        let dir = "test_loaded_progress_verifies_last_piece_with_correct_length";
        let _dir_guard = TestDir::create(dir);
        let handle = main_ctx();
        let (total_len, piece_count) =
            handle.with(|ctx| (ctx.pieces.total_len(), ctx.pieces.piece_count()));
        let last_piece = piece_count - 1;
        assert!(handle.with(|ctx| ctx.pieces.piece_len(last_piece) < ctx.pieces.piece_len(0)));
        save_progress_with(dir, &handle, &[0, last_piece]);

        let requests = Rc::new(std::cell::RefCell::new(Vec::new()));
        let storage = data::new_mock_storage_with_verifier(total_len, {
            let requests = requests.clone();
            move |offset, len| {
                requests.borrow_mut().push((offset, len));
                true
            }
        });
        let mut task = pin!(restore_and_persist_progress(handle.clone(), dir, storage));
        let _ = time::timeout(sec!(1), &mut task).await;

        let expected: Vec<_> = handle.with(|ctx| {
            [0, last_piece]
                .into_iter()
                .map(|i| (ctx.pieces.global_offset(i, 0, 0).unwrap(), ctx.pieces.piece_len(i)))
                .collect()
        });
        assert_eq!(*requests.borrow(), expected);
        assert_eq!(downloaded_pieces(&handle), vec![0, last_piece]);
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_progress_saved_when_task_dropped() {
        let dir = "test_progress_saved_when_task_dropped";
        let _dir_guard = TestDir::create(dir);
        let handle = main_ctx();
        let (info_hash, piece_count, total_len) = handle.with(|ctx| {
            (*ctx.metainfo.info_hash(), ctx.pieces.piece_count(), ctx.pieces.total_len())
        });

        {
            let storage = data::new_mock_storage(total_len);
            let mut task = pin!(restore_and_persist_progress(handle.clone(), dir, storage));
            let _ = time::timeout(sec!(1), &mut task).await;

            handle.with(|ctx| {
                assert!(ctx.accountant.submit_piece(1));
                ctx.piece_tracker.forget_piece(1);
            });
            // dropped before the next periodic save is due
        }

        let mut state = disk::ProgressFile::open(dir).unwrap().load_progress(&info_hash).unwrap();
        state.resize(piece_count, false);
        assert_eq!(state.iter_ones().collect::<Vec<_>>(), vec![1]);
    }

    struct NoopListener;

    impl StateListener for NoopListener {
        const INTERVAL: Duration = sec!(1);
        fn on_snapshot(&mut self, _snapshot: StateSnapshot<'_>) {}
    }

    fn main_ctx() -> Handle<MainCtx> {
        let metainfo =
            startup::read_metainfo("../mtorrent-cli/tests/assets/example.torrent").unwrap();
        MainCtx::new(
            metainfo,
            PeerId::generate_new(),
            1234,
            12345,
            Ipv4Addr::LOCALHOST,
            Ipv6Addr::LOCALHOST,
            None,
            Default::default(),
            Default::default(),
        )
        .unwrap()
    }

    fn preliminary_ctx() -> Handle<PreliminaryCtx> {
        let magnet = "magnet:?xt=urn:btih:1EBD3DBFBB25C1333F51C99C7EE670FC2A1727C9"
            .parse::<input::MagnetLink>()
            .unwrap();
        PreliminaryCtx::new(
            magnet,
            PeerId::generate_new(),
            1234,
            12345,
            Ipv4Addr::LOCALHOST,
            Ipv6Addr::LOCALHOST,
            None,
        )
    }

    /// Stream that ends immediately and panics if polled again after that.
    fn stream_panicking_after_end() -> impl Stream<Item = Exit<CriticalTask>> + Unpin {
        let mut ended = false;
        futures_util::stream::poll_fn(move |_cx| {
            assert!(!ended, "stream polled again after it ended");
            ended = true;
            std::task::Poll::Ready(None)
        })
    }

    // Neither directory exists, so nothing is written to disk.
    const NONEXISTENT_METAINFO: &str = "test_supervise_nonexistent_dir/example.torrent";

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_content_download_fails_when_critical_task_stops() {
        let cancel = pin!(std::future::pending::<()>());
        let result = time::timeout(
            sec!(10),
            supervise_content_download(
                main_ctx(),
                &mut NoopListener,
                cancel,
                futures_util::stream::iter([Exit::Dropped(CriticalTask::PieceVerifier)]),
            ),
        )
        .await
        .expect("supervisor did not react to stopped critical task");
        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "task PieceVerifier was dropped prematurely (panicked or aborted)"
        );
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_content_download_fails_when_critical_task_stops_later() {
        let cancel = pin!(time::sleep(sec!(10)));
        let stopped = pin!(futures_util::stream::once(async {
            time::sleep(sec!(5)).await;
            Exit::Completed(CriticalTask::ConnectControl)
        }));
        let result =
            supervise_content_download(main_ctx(), &mut NoopListener, cancel, stopped).await;
        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "task ConnectControl completed prematurely");
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_content_download_continues_after_critical_task_stream_ends() {
        let cancel = pin!(time::sleep(sec!(5)));
        let result = supervise_content_download(
            main_ctx(),
            &mut NoopListener,
            cancel,
            stream_panicking_after_end(),
        )
        .await;
        assert_eq!(result.unwrap(), Outcome::Cancelled);
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_metadata_download_fails_when_critical_task_stops() {
        let cancel = pin!(std::future::pending::<()>());
        let result = time::timeout(
            sec!(10),
            supervise_metadata_download(
                preliminary_ctx(),
                NONEXISTENT_METAINFO,
                &mut NoopListener,
                cancel,
                futures_util::stream::iter([Exit::Completed(CriticalTask::ConnectControl)]),
            ),
        )
        .await
        .expect("supervisor did not react to stopped critical task");
        let error = result.err().unwrap();
        assert_eq!(error.to_string(), "task ConnectControl completed prematurely");
    }

    #[tokio::test(start_paused = true, flavor = "local")]
    async fn test_metadata_download_continues_after_critical_task_stream_ends() {
        let cancel = pin!(time::sleep(sec!(5)));
        let result = supervise_metadata_download(
            preliminary_ctx(),
            NONEXISTENT_METAINFO,
            &mut NoopListener,
            cancel,
            stream_panicking_after_end(),
        )
        .await;
        assert!(result.unwrap().is_none());
    }
}
