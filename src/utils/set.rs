use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::collections::HashSet;
use std::hash::Hash;

/// Unordered set that never leaks references to its content
pub struct Set<T>(UnsafeCell<HashSet<T>>);

impl<T: Eq + Hash> Set<T> {
    pub fn new() -> Self {
        Self::default()
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
}

impl<T> Default for Set<T> {
    fn default() -> Self {
        Self(Default::default())
    }
}
