use crate::{data, pwp, utils::meta};
use std::io;
use std::rc::Rc;
use std::{cell::RefCell, ops::Deref};

pub(super) struct PeerId([u8; 20]); // immutable wrapper

impl Deref for PeerId {
    type Target = [u8; 20];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(super) struct Ctx {
    pub(super) pieces: Rc<data::PieceInfo>,
    pub(super) accountant: data::BlockAccountant,
    pub(super) piece_tracker: data::PieceTracker,
    pub(super) metainfo: meta::Metainfo,
    pub(super) peer_states: pwp::PeerStates,
    pub(super) pending_requests: pwp::PendingRequests,
    pub(super) local_peer_id: PeerId,
}

#[derive(Clone)]
pub struct Handle {
    ctx: Rc<RefCell<Ctx>>,
}

impl Handle {
    pub(super) fn with_ctx<R, F>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut Ctx) -> R,
    {
        let mut borrowed = self.ctx.borrow_mut();
        f(&mut borrowed)
    }
}

#[macro_export]
macro_rules! define_with_ctx {
    ($handle:expr) => {
        macro_rules! with_ctx {
            ($f:expr) => {
                $handle.with_ctx($f)
            };
        }
    };
}

pub struct Owner {
    ctx_handle: Handle,
}

impl Owner {
    pub fn new(metainfo: meta::Metainfo, local_peer_id: &[u8; 20]) -> io::Result<Self> {
        fn io_error(s: &'static str) -> impl FnOnce() -> io::Error {
            || io::Error::new(io::ErrorKind::InvalidData, s.to_owned())
        }
        let pieces = Rc::new(data::PieceInfo::new(
            metainfo.pieces().ok_or_else(io_error("no pieces in metainfo"))?,
            metainfo.piece_length().ok_or_else(io_error("no piece length in metainfo"))?,
            metainfo
                .length()
                .or_else(|| metainfo.files().map(|it| it.map(|(len, _path)| len).sum()))
                .ok_or_else(io_error("no total length in metainfo"))?,
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
            local_peer_id: PeerId(*local_peer_id),
        }));
        Ok(Self {
            ctx_handle: Handle { ctx },
        })
    }

    pub fn create_handle(&self) -> Handle {
        Handle {
            ctx: self.ctx_handle.ctx.clone(),
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
