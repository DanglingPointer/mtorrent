use std::collections::vec_deque::{IntoIter, Iter};
use std::collections::{HashSet, VecDeque};
use std::hash::Hash;

pub struct BoundedFifoSet<T> {
    ringbuf: VecDeque<T>,
    set: HashSet<T>, // for performance
    max_capacity: usize,
}

impl<T> BoundedFifoSet<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            ringbuf: VecDeque::with_capacity(capacity),
            set: HashSet::with_capacity(capacity + 1),
            max_capacity: capacity,
        }
    }

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

impl<T: Eq + Hash + Clone> BoundedFifoSet<T> {
    pub fn insert_or_replace(&mut self, item: T) -> bool {
        if self.set.insert(item.clone()) {
            if self.ringbuf.len() == self.max_capacity {
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

impl<T> IntoIterator for BoundedFifoSet<T> {
    type Item = T;

    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.ringbuf.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a BoundedFifoSet<T> {
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
