use super::ctrl;
use crate::utils::config;
use crate::utils::listener::{
    BytesSnapshot, MetainfoSnapshot, PiecesSnapshot, RequestsSnapshot, StateListener, StateSnapshot,
};
use local_async_utils::prelude::*;
use mtorrent_core::{data, input, pwp};
use mtorrent_utils::peer_id::PeerId;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;
use std::rc::Rc;
use std::{cmp, fs, io, mem};
use tokio::time;
use tokio_util::sync::DropGuard;

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

pub(super) struct ConstData {
    local_peer_id: PeerId,
    pwp_listener_public_addr: SocketAddr,
    pwp_local_tcp_port: u16,
}

impl ConstData {
    pub(super) fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }
    pub(super) fn pwp_listener_public_addr(&self) -> &SocketAddr {
        &self.pwp_listener_public_addr
    }
    pub(super) fn pwp_local_tcp_port(&self) -> u16 {
        self.pwp_local_tcp_port
    }
}

pub struct PreliminaryCtx {
    pub(super) magnet: input::MagnetLink,
    pub(super) metainfo: Vec<u8>,
    pub(super) metainfo_pieces: pwp::Bitfield,
    pub(super) reachable_peers: HashSet<SocketAddr>,
    pub(super) peer_states: pwp::PeerStates,
    pub(super) const_data: ConstData,
}

impl PreliminaryCtx {
    pub fn new(
        magnet: input::MagnetLink,
        local_peer_id: PeerId,
        pwp_listener_public_addr: SocketAddr,
        pwp_local_tcp_port: u16,
    ) -> Handle<Self> {
        Handle::new(Self {
            magnet,
            metainfo: Vec::new(),
            metainfo_pieces: pwp::Bitfield::new(),
            reachable_peers: Default::default(),
            peer_states: Default::default(),
            const_data: ConstData {
                local_peer_id,
                pwp_listener_public_addr,
                pwp_local_tcp_port,
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
    pub fn new(
        metainfo: input::Metainfo,
        local_peer_id: PeerId,
        pwp_listener_public_addr: SocketAddr,
        pwp_local_tcp_port: u16,
    ) -> io::Result<Handle<Self>> {
        fn make_error(s: &'static str) -> impl FnOnce() -> io::Error {
            move || io::Error::new(io::ErrorKind::InvalidData, s)
        }
        let pieces = Rc::new(data::PieceInfo::new(
            metainfo.pieces().ok_or_else(make_error("no pieces in metainfo"))?,
            metainfo.piece_length().ok_or_else(make_error("no piece length in metainfo"))?,
            metainfo
                .length()
                .or_else(|| metainfo.files().map(|it| it.map(|(len, _path)| len).sum()))
                .ok_or_else(make_error("no total length in metainfo"))?,
        ));
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
                pwp_listener_public_addr,
                pwp_local_tcp_port,
            },
        };
        Ok(Handle::new(ctx))
    }
}

pub async fn periodic_metadata_check<L: StateListener>(
    mut ctx_handle: Handle<PreliminaryCtx>,
    metainfo_filepath: impl AsRef<Path>,
    state_listener: &mut L,
    _canceller: DropGuard,
) -> io::Result<impl IntoIterator<Item = SocketAddr> + 'static> {
    define_with_ctx!(ctx_handle);

    let mut check_finished = || {
        with_ctx!(|ctx| {
            state_listener.on_snapshot(preliminary_snapshot(ctx)).is_break()
                || ctrl::verify_metadata(ctx)
        })
    };

    let mut timer = time::interval(cmp::min(sec!(1), L::INTERVAL));
    while !check_finished() {
        timer.tick().await;
    }

    with_ctx!(|ctx| fs::write(metainfo_filepath, &ctx.metainfo))?;

    Ok(with_ctx!(|ctx| mem::take(&mut ctx.reachable_peers)))
}

pub async fn periodic_state_dump<L: StateListener>(
    mut ctx_handle: Handle<MainCtx>,
    outputdir: impl AsRef<Path>,
    state_listener: &mut L,
    _canceller: DropGuard,
) {
    define_with_ctx!(ctx_handle);

    with_ctx!(|ctx| {
        match config::load_state(&outputdir, ctx.metainfo.info_hash()) {
            Ok(mut state) => {
                state.resize(ctx.pieces.piece_count(), false);
                ctx.accountant.submit_bitfield(&state);
                for (piece_index, is_present) in state.iter().enumerate() {
                    if *is_present {
                        ctx.piece_tracker.forget_piece(piece_index);
                    }
                }
            }
            Err(e) => {
                log::warn!("Failed to load saved state: {e}");
            }
        }
    });

    // sleep for 5s because of integration tests
    #[cfg(debug_assertions)]
    time::sleep(sec!(5)).await;

    let mut check_finished = || {
        with_ctx!(|ctx| {
            if let Err(e) = config::save_state(
                &outputdir,
                ctx.metainfo.info_hash(),
                ctx.accountant.generate_bitfield(),
            ) {
                log::warn!("Failed to save state to file: {e}");
            }
            state_listener.on_snapshot(main_snapshot(ctx)).is_break() || ctrl::is_finished(ctx)
        })
    };

    let mut timer = time::interval(L::INTERVAL);
    while !check_finished() {
        timer.tick().await;
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
