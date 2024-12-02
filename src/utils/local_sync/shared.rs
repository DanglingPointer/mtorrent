use crate::utils::shared::Shared;
use std::cell::RefCell;
use std::rc::Rc;

pub struct LocalShared<T> {
    inner: Rc<RefCell<T>>,
}

impl<T> LocalShared<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: Rc::new(RefCell::new(inner)),
        }
    }
}

impl<T> Shared for LocalShared<T> {
    type Target = T;

    #[inline(always)]
    fn with<R, F>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        f(&mut self.inner.borrow_mut())
    }
}

impl<T> Clone for LocalShared<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
