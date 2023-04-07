use std::{cell::UnsafeCell, collections::VecDeque};

pub struct Queue<T>(UnsafeCell<VecDeque<T>>);

impl<T> Queue<T> {
    pub fn new() -> Self {
        Self(UnsafeCell::new(VecDeque::new()))
    }

    pub fn push(&self, item: T) {
        let inner = unsafe { &mut *self.0.get() };
        inner.push_back(item);
    }

    pub fn pop(&self) -> Option<T> {
        let inner = unsafe { &mut *self.0.get() };
        inner.pop_front()
    }

    pub fn contains(&self, item: &T) -> bool
    where
        T: PartialEq<T>,
    {
        let inner = unsafe { &*self.0.get() };
        inner.contains(item)
    }

    pub fn remove_all(&self, item: &T) -> bool
    where
        T: PartialEq<T>,
    {
        let inner = unsafe { &mut *self.0.get() };
        let initial_len = inner.len();
        inner.retain(|e| e != item);
        inner.len() != initial_len
    }

    pub fn remove_if<F>(&mut self, mut pred: F) -> bool
    where
        F: FnMut(&T) -> bool,
    {
        let inner = self.0.get_mut();
        let initial_len = inner.len();
        inner.retain(|e| !pred(e));
        inner.len() != initial_len
    }

    pub fn len(&self) -> usize {
        let inner = unsafe { &*self.0.get() };
        inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::*;
    use std::{rc::Rc, sync::Arc};

    #[test]
    fn test_queue_is_send_but_not_sync() {
        assert_impl_all!(Queue<usize>: Send);
        assert_not_impl_any!(Queue<Rc<usize>>: Send);
        assert_not_impl_any!(Queue<Arc<usize>>: Sync);
        assert_not_impl_any!(Arc<Queue<usize>>: Send, Sync);
    }
}
