use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::collections::{hash_set, HashSet, VecDeque};
use std::hash::Hash;

/// FIFO queue that never leaks references to its content
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

/// Unordered set that never leaks references to its content
pub struct Set<T>(UnsafeCell<HashSet<T>>);

impl<T: Eq + Hash> Set<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(UnsafeCell::new(HashSet::with_capacity(capacity)))
    }

    pub fn contains<Q>(&self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        let inner = unsafe { &*self.0.get() };
        inner.contains(value)
    }

    pub fn insert(&self, value: T) -> bool {
        let inner = unsafe { &mut *self.0.get() };
        inner.insert(value)
    }

    pub fn remove<Q>(&self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        let inner = unsafe { &mut *self.0.get() };
        inner.remove(value)
    }

    pub fn clear(&self) {
        let inner = unsafe { &mut *self.0.get() };
        inner.clear();
    }

    pub fn len(&self) -> usize {
        let inner = unsafe { &*self.0.get() };
        inner.len()
    }

    pub fn is_empty(&self) -> bool {
        let inner = unsafe { &*self.0.get() };
        inner.is_empty()
    }
}

impl<T> Default for Set<T> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<T: Clone> Clone for Set<T> {
    fn clone(&self) -> Self {
        let inner = unsafe { &*self.0.get() };
        Self(UnsafeCell::new(inner.clone()))
    }
}

impl<T> IntoIterator for Set<T> {
    type Item = T;
    type IntoIter = hash_set::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_inner().into_iter()
    }
}

#[cfg(test)]
mod tests {
    use std::{rc::Rc, sync::Arc};

    use super::*;
    use static_assertions::{assert_impl_all, assert_not_impl_any};

    #[test]
    fn test_queue_is_send_but_not_sync() {
        assert_impl_all!(Queue<usize>: std::marker::Send);
        assert_not_impl_any!(Queue<Rc<usize>>: std::marker::Send);
        assert_not_impl_any!(Queue<Arc<usize>>: Sync);
        assert_not_impl_any!(Arc<Queue<usize>>: std::marker::Send, Sync);
    }
}
