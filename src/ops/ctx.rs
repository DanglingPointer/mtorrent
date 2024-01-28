use std::{
    cell::{Cell, RefCell, UnsafeCell},
    rc::Rc,
};

use crate::{data, utils::meta};

pub struct Ctx {
    pub pieces: data::PieceInfo,
    pub accountant: data::BlockAccountant,
    pub piece_tracker: data::PieceTracker,
    pub metainfo: meta::Metainfo,
}

pub(super) struct Handle {
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

struct ControlBlock {
    ctx: UnsafeCell<Ctx>,
    borrowed: Cell<bool>,
}

struct BorrowGuard<'a> {
    borrowed: &'a Cell<bool>,
}
impl<'a> BorrowGuard<'a> {
    #[inline(always)]
    fn new(cb: &'a ControlBlock) -> Self {
        if cb.borrowed.replace(true) {
            panic!("Already borrowed");
        }
        Self {
            borrowed: &cb.borrowed,
        }
    }
}
impl<'a> Drop for BorrowGuard<'a> {
    #[inline(always)]
    fn drop(&mut self) {
        self.borrowed.set(false)
    }
}

pub(super) struct UnsafeHandle {
    ctx: Rc<ControlBlock>,
}

impl UnsafeHandle {
    pub(super) fn with_ctx<R, F>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut Ctx) -> R,
    {
        let _guard = BorrowGuard::new(&self.ctx);
        f(unsafe { &mut *self.ctx.ctx.get() })
    }
}
