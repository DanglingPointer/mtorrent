use super::ctrl;
use crate::sec;
use crate::utils::peer_id::PeerId;
use crate::{data, pwp, utils::meta};
use core::fmt;
use std::io;
use std::time::Duration;
use std::{cell::RefCell, rc::Rc};
use tokio::time;

pub(super) struct Ctx {
    pub(super) pieces: Rc<data::PieceInfo>,
    pub(super) accountant: data::BlockAccountant,
    pub(super) piece_tracker: data::PieceTracker,
    pub(super) metainfo: meta::Metainfo,
    pub(super) peer_states: pwp::PeerStates,
    pub(super) pending_requests: pwp::PendingRequests,
    pub(super) local_peer_id: PeerId,
}

impl fmt::Display for Ctx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Local availability: {}\nOutstanding requests: {}\n{}",
            self.accountant, self.pending_requests, self.peer_states
        )
    }
}

impl Drop for Ctx {
    fn drop(&mut self) {
        log::info!("Final state dump:\n{}", self);
    }
}

#[derive(Clone)]
pub struct Handle {
    ctx: Rc<RefCell<Ctx>>,
}

impl Handle {
    #[inline(always)]
    pub(super) fn with_ctx<R, F>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut Ctx) -> R,
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

pub fn new_ctx(metainfo: meta::Metainfo, local_peer_id: PeerId) -> io::Result<Handle> {
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
    let ctx = Rc::new(RefCell::new(Ctx {
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

pub async fn periodic_state_dump(mut ctx_handle: Handle) {
    define_with_ctx!(ctx_handle);
    const INTERVAL: Duration = sec!(10);

    loop {
        time::sleep(INTERVAL).await;
        let finished = with_ctx!(|ctx| {
            log::info!("Periodic state dump:\n{}", ctx);
            ctrl::is_finished(ctx)
        });
        if finished {
            break;
        }
    }
}

// struct ControlBlock {
//     ctx: UnsafeCell<Ctx>,
//     borrowed: Cell<bool>,
// }

// struct BorrowGuard<'a> {
//     borrowed: &'a Cell<bool>,
// }
// impl<'a> BorrowGuard<'a> {
//     #[inline(always)]
//     fn new(cb: &'a ControlBlock) -> Self {
//         if cb.borrowed.replace(true) {
//             panic!("Already borrowed");
//         }
//         Self {
//             borrowed: &cb.borrowed,
//         }
//     }
// }
// impl<'a> Drop for BorrowGuard<'a> {
//     #[inline(always)]
//     fn drop(&mut self) {
//         self.borrowed.set(false)
//     }
// }

// pub(super) struct UnsafeHandle {
//     ctx: Rc<ControlBlock>,
// }

// impl UnsafeHandle {
//     pub(super) fn with_ctx<R, F>(&mut self, f: F) -> R
//     where
//         F: FnOnce(&mut Ctx) -> R,
//     {
//         let _guard = BorrowGuard::new(&self.ctx);
//         f(unsafe { &mut *self.ctx.ctx.get() })
//     }
// }
