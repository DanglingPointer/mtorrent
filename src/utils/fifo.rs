use std::cell::{Cell, UnsafeCell};
use std::task::{Poll, Waker};
use std::{collections::VecDeque, rc::Rc};

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

struct ChannelData<T> {
    queue: Queue<T>,
    waker: Cell<Option<Waker>>,
    sender_count: Cell<usize>,
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let state = Rc::new(ChannelData {
        queue: Default::default(),
        waker: Cell::new(None),
        sender_count: Cell::new(1),
    });
    let sender = Sender(state.clone());
    let receiver = Receiver(state);
    (sender, receiver)
}

pub struct Sender<T>(Rc<ChannelData<T>>);

impl<T> Sender<T> {
    pub fn send(&self, item: T) {
        self.0.queue.push(item);
        if let Some(waker) = self.0.waker.take() {
            waker.wake();
        }
    }

    pub fn try_send(&self, len_threshold: usize, item: T) -> bool {
        if self.0.queue.len() < len_threshold {
            self.send(item);
            true
        } else {
            false
        }
    }

    pub fn remove_all(&self, item: &T) -> bool
    where
        T: PartialEq<T>,
    {
        self.0.queue.remove_all(item)
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let prev_count = self.0.sender_count.get();
        self.0.sender_count.set(prev_count - 1);
        if let Some(waker) = self.0.waker.take() {
            waker.wake();
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let prev_count = self.0.sender_count.get();
        self.0.sender_count.set(prev_count + 1);
        Self(self.0.clone())
    }
}

pub struct Receiver<T>(Rc<ChannelData<T>>);

impl<T> futures::Stream for Receiver<T> {
    type Item = T;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(item) = self.0.queue.pop() {
            Poll::Ready(Some(item))
        } else if self.0.sender_count.get() == 0 {
            Poll::Ready(None)
        } else {
            self.0.waker.set(Some(cx.waker().clone()));
            Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.0.queue.len(), Some(self.0.queue.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::prelude::*;
    use static_assertions::*;
    use std::{rc::Rc, sync::Arc};

    #[test]
    fn test_queue_is_send_but_not_sync() {
        assert_impl_all!(Queue<usize>: std::marker::Send);
        assert_not_impl_any!(Queue<Rc<usize>>: std::marker::Send);
        assert_not_impl_any!(Queue<Arc<usize>>: Sync);
        assert_not_impl_any!(Arc<Queue<usize>>: std::marker::Send, Sync);
    }

    #[test]
    fn test_channel_static_properties() {
        assert_not_impl_any!(Arc<Sender<usize>>: std::marker::Send, Sync);
        assert_not_impl_any!(Arc<Receiver<usize>>: std::marker::Send, Sync);
        assert_not_impl_any!(Sender<usize>: std::marker::Send, Sync);
        assert_not_impl_any!(Receiver<usize>: std::marker::Send, Sync);
    }

    #[tokio::test]
    async fn test_channel_passes_values() {
        let (sender1, mut receiver1) = channel::<i32>();
        let (sender2, mut receiver2) = channel::<i32>();

        let fut2 = async move {
            for i in 0..42 {
                sender1.send(i);
            }
            drop(sender1); // deadlock otherwise
            let mut ret = Vec::new();
            while let Some(value) = receiver2.next().await {
                ret.push(value);
            }
            ret
        };

        let fut1 = async move {
            let mut ret = Vec::new();
            while let Some(value) = receiver1.next().await {
                ret.push(value);
            }
            for i in 42..84 {
                sender2.send(i);
            }
            drop(sender2);
            ret
        };

        let (result1, result2) = tokio::join!(fut1, fut2);
        assert_eq!((0..42).collect::<Vec<i32>>(), result1);
        assert_eq!((42..84).collect::<Vec<i32>>(), result2);
    }
}
