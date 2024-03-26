use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::collections::HashSet;
use std::hash::Hash;

pub struct Set<T>(UnsafeCell<HashSet<T>>);

impl<T> Default for Set<T> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<T: Eq + Hash> Set<T> {
    pub fn contains<Q: ?Sized>(&self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq,
    {
        let inner = unsafe { &*self.0.get() };
        inner.contains(value)
    }

    pub fn insert(&self, value: T) -> bool {
        let inner = unsafe { &mut *self.0.get() };
        inner.insert(value)
    }

    pub fn remove<Q: ?Sized>(&self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Hash + Eq,
    {
        let inner = unsafe { &mut *self.0.get() };
        inner.remove(value)
    }
}
