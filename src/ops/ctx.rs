use super::ctrl;
use crate::pwp::Bitfield;
use crate::utils::peer_id::PeerId;
use crate::utils::{config, magnet, metainfo};
use crate::{data, pwp};
use core::fmt;
use local_async_utils::local_sync::LocalShared;
use local_async_utils::sec;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;
use std::rc::Rc;
use std::{fs, io, mem};
use tokio::time;

pub type Handle<C> = LocalShared<C>;

macro_rules! define_with_ctx {
    ($handle:expr) => {
        macro_rules! with_ctx {
            ($f:expr) => {{
                use local_async_utils::shared::Shared;
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
    pub(super) magnet: magnet::MagnetLink,
    pub(super) metainfo: Vec<u8>,
    pub(super) metainfo_pieces: pwp::Bitfield,
    pub(super) known_peers: HashSet<SocketAddr>,
    pub(super) connected_peers: HashSet<SocketAddr>,
    pub(super) const_data: ConstData,
}

impl PreliminaryCtx {
    pub fn new(
        magnet: magnet::MagnetLink,
        local_peer_id: PeerId,
        pwp_listener_public_addr: SocketAddr,
        pwp_local_tcp_port: u16,
    ) -> Handle<Self> {
        Handle::new(Self {
            magnet,
            metainfo: Vec::new(),
            metainfo_pieces: Bitfield::new(),
            known_peers: Default::default(),
            connected_peers: Default::default(),
            const_data: ConstData {
                local_peer_id,
                pwp_listener_public_addr,
                pwp_local_tcp_port,
            },
        })
    }
}

impl fmt::Display for PreliminaryCtx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Metainfo pieces: {}/{}\nConnected peers: {:?}",
            self.metainfo_pieces.count_ones(),
            self.metainfo_pieces.len(),
            self.connected_peers,
        )
    }
}

pub struct MainCtx {
    pub(super) pieces: Rc<data::PieceInfo>,
    pub(super) accountant: data::BlockAccountant,
    pub(super) piece_tracker: data::PieceTracker,
    pub(super) metainfo: metainfo::Metainfo,
    pub(super) peer_states: pwp::PeerStates,
    pub(super) pending_requests: data::PendingRequests,
    pub(super) const_data: ConstData,
}

impl MainCtx {
    pub fn new(
        metainfo: metainfo::Metainfo,
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

impl fmt::Display for MainCtx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Local availability: {}\nOutstanding requests: {}\n{}",
            self.accountant, self.pending_requests, self.peer_states
        )
    }
}

impl Drop for MainCtx {
    fn drop(&mut self) {
        log::info!("Final state dump:\n{}", self);
    }
}

pub async fn periodic_metadata_check(
    mut ctx_handle: Handle<PreliminaryCtx>,
    metainfo_filepath: impl AsRef<Path>,
) -> io::Result<impl IntoIterator<Item = SocketAddr>> {
    define_with_ctx!(ctx_handle);

    let mut interval = time::interval(sec!(1));
    while with_ctx!(|ctx| !ctrl::verify_metadata(ctx)) {
        interval.tick().await;
        with_ctx!(|ctx| log::info!("Periodic state dump:\n{}", ctx));
    }

    with_ctx!(|ctx| fs::write(metainfo_filepath, &ctx.metainfo))?;

    Ok(with_ctx!(|ctx| mem::take(&mut ctx.known_peers)))
}

pub async fn periodic_state_dump(mut ctx_handle: Handle<MainCtx>, outputdir: impl AsRef<Path>) {
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

    // first tick after 5s because of integration tests
    let mut interval = time::interval_at(time::Instant::now() + sec!(5), sec!(10));

    loop {
        interval.tick().await;
        let finished = with_ctx!(|ctx| {
            if let Err(e) = config::save_state(
                &outputdir,
                ctx.metainfo.info_hash(),
                ctx.accountant.generate_bitfield(),
            ) {
                log::warn!("Failed to save state to file: {e}");
            }
            log::info!("Periodic state dump:\n{}", ctx);
            ctrl::is_finished(ctx)
        });
        if finished {
            break;
        }
    }
}
