use super::ctrl;
use crate::sec;
use crate::utils::config;
use crate::utils::peer_id::PeerId;
use crate::{data, pwp, utils::meta};
use core::fmt;
use std::io;
use std::path::Path;
use std::time::Duration;
use std::{cell::RefCell, rc::Rc};
use tokio::time;

pub struct Handle<C> {
    ctx: Rc<RefCell<C>>,
}

impl<C> Clone for Handle<C> {
    fn clone(&self) -> Self {
        Self {
            ctx: self.ctx.clone(),
        }
    }
}

impl<C> Handle<C> {
    #[inline(always)]
    pub(super) fn with_ctx<R, F>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut C) -> R,
    {
        let mut borrowed = self.ctx.borrow_mut();
        f(&mut borrowed)
    }
}

macro_rules! define_with_ctx {
    ($handle:expr) => {
        macro_rules! with_ctx {
            ($f:expr) => {
                $handle.with_ctx(
                    #[inline(always)]
                    $f,
                )
            };
        }
    };
}

pub struct MainCtx {
    pub(super) pieces: Rc<data::PieceInfo>,
    pub(super) accountant: data::BlockAccountant,
    pub(super) piece_tracker: data::PieceTracker,
    pub(super) metainfo: meta::Metainfo,
    pub(super) peer_states: pwp::PeerStates,
    pub(super) pending_requests: pwp::PendingRequests,
    pub(super) local_peer_id: PeerId,
}

impl MainCtx {
    pub fn new(metainfo: meta::Metainfo, local_peer_id: PeerId) -> io::Result<Handle<Self>> {
        fn make_error(s: &'static str) -> impl FnOnce() -> io::Error {
            || io::Error::new(io::ErrorKind::InvalidData, s.to_owned())
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
        let ctx = Rc::new(RefCell::new(Self {
            pieces,
            accountant,
            piece_tracker,
            metainfo,
            peer_states: Default::default(),
            pending_requests: Default::default(),
            local_peer_id,
        }));
        Ok(Handle { ctx })
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

    #[cfg(debug_assertions)]
    const INTERVAL: Duration = sec!(5);

    #[cfg(not(debug_assertions))]
    const INTERVAL: Duration = sec!(10);

    loop {
        time::sleep(INTERVAL).await;
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
