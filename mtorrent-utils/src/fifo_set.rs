use std::collections::vec_deque::{IntoIter, Iter};
use std::collections::{HashSet, VecDeque};
use std::hash::Hash;

pub trait Policy: Clone {
    fn capacity(&self) -> usize;
}

#[derive(Clone, Copy)]
pub struct Bounded(usize);
impl Policy for Bounded {
    fn capacity(&self) -> usize {
        self.0
    }
}

#[derive(Clone, Copy)]
pub struct Unbounded;
impl Policy for Unbounded {
    fn capacity(&self) -> usize {
        usize::MAX
    }
}

#[derive(Debug)]
pub struct FifoSet<T, P: Policy> {
    ringbuf: VecDeque<T>,
    set: HashSet<T>, // for performance
    policy: P,
}

pub type UnboundedFifoSet<T> = FifoSet<T, Unbounded>;
pub type BoundedFifoSet<T> = FifoSet<T, Bounded>;

impl<T> FifoSet<T, Bounded> {
    /// New [`BoundedFifoSet`] that can hold up to `capacity` elements.
    pub fn new(capacity: usize) -> Self {
        assert_ne!(capacity, 0, "zero capacity is not supported");
        Self {
            ringbuf: VecDeque::with_capacity(capacity),
            set: HashSet::with_capacity(capacity + 1),
            policy: Bounded(capacity),
        }
    }
}

impl<T> FifoSet<T, Unbounded> {
    pub fn new() -> Self {
        Self {
            ringbuf: VecDeque::new(),
            set: HashSet::new(),
            policy: Unbounded,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            ringbuf: VecDeque::with_capacity(capacity),
            set: HashSet::with_capacity(capacity),
            policy: Unbounded,
        }
    }
}

impl<T, P: Policy> FifoSet<T, P> {
    pub fn iter(&self) -> Iter<'_, T> {
        self.ringbuf.iter()
    }

    pub fn len(&self) -> usize {
        debug_assert!(self.ringbuf.len() == self.set.len());
        self.ringbuf.len()
    }

    pub fn is_empty(&self) -> bool {
        debug_assert!(self.ringbuf.len() == self.set.len());
        self.ringbuf.is_empty()
    }
}

impl<T: Eq + Hash + Clone, P: Policy> FifoSet<T, P> {
    /// Remove oldest element.
    pub fn remove_first(&mut self) -> Option<T> {
        self.ringbuf.pop_front().inspect(|item| {
            self.set.remove(item);
        })
    }
}

impl<T: Eq + Hash + Clone> FifoSet<T, Bounded> {
    /// Add a new element unless it already exists.
    /// Removes the oldest element if the maximum capacity has been reached.
    ///
    /// # Returns
    /// `true` if the element has been inserted, `false` if it was already present.
    pub fn insert_or_replace(&mut self, item: T) -> bool {
        if self.set.insert(item.clone()) {
            if self.ringbuf.len() == self.policy.capacity() {
                let popped = self.ringbuf.pop_front().unwrap_or_else(|| unreachable!());
                self.set.remove(&popped);
            }
            self.ringbuf.push_back(item);
            true
        } else {
            false
        }
    }
}

impl<T: Eq + Hash + Clone> FifoSet<T, Unbounded> {
    /// Add a new element ignoring capacity. Does nothing if the element already exists.
    ///
    /// # Returns
    /// `true` if the element has been inserted, `false` if it was already present.
    pub fn insert(&mut self, item: T) -> bool {
        if self.set.insert(item.clone()) {
            self.ringbuf.push_back(item);
            true
        } else {
            false
        }
    }
}

impl<T> Default for FifoSet<T, Unbounded> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone, P: Policy> Clone for FifoSet<T, P> {
    fn clone(&self) -> Self {
        Self {
            ringbuf: self.ringbuf.clone(),
            set: self.set.clone(),
            policy: self.policy.clone(),
        }
    }
}

impl<T, P: Policy> IntoIterator for FifoSet<T, P> {
    type Item = T;

    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.ringbuf.into_iter()
    }
}

impl<'a, T, P: Policy> IntoIterator for &'a FifoSet<T, P> {
    type Item = &'a T;

    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fifo_rotation() {
        const MAX_CAPACITY: usize = 128;

        let mut pt = BoundedFifoSet::new(MAX_CAPACITY);
        for port in 0..MAX_CAPACITY + 10 {
            pt.insert_or_replace(port);
        }
        assert_eq!(pt.ringbuf.len(), MAX_CAPACITY);
        assert_eq!(pt.set.len(), MAX_CAPACITY);
        assert_eq!(*pt.ringbuf.front().unwrap(), 10);
        assert_eq!(*pt.ringbuf.back().unwrap(), MAX_CAPACITY + 9);

        // insert the same element multiple times
        assert!(pt.insert_or_replace(12345));
        assert!(!pt.insert_or_replace(12345));
        assert!(!pt.insert_or_replace(12345));

        assert_eq!(pt.ringbuf.len(), MAX_CAPACITY);
        assert_eq!(pt.set.len(), MAX_CAPACITY);
        assert_eq!(*pt.ringbuf.front().unwrap(), 11);
        assert_eq!(*pt.ringbuf.back().unwrap(), 12345);

        // reinsert element that has been removed
        pt.insert_or_replace(0);

        assert_eq!(pt.ringbuf.len(), MAX_CAPACITY);
        assert_eq!(pt.set.len(), MAX_CAPACITY);
        assert_eq!(*pt.ringbuf.front().unwrap(), 12);
        assert_eq!(*pt.ringbuf.back().unwrap(), 0);
    }
}
