use super::sealed::Queue;
use futures::FutureExt;
use std::cell::Cell;
use std::future::{poll_fn, Future};
use std::ops::Deref;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

trait Source {
    type Item;
    fn closed(&self) -> bool;
    fn extract_item(&self) -> Option<Self::Item>;
}

struct SharedState<T> {
    waker: Cell<Option<Waker>>,
    inner: T,
}

impl<T: Source> SharedState<T> {
    fn new(inner: T) -> Rc<Self> {
        Rc::new(Self {
            waker: Cell::new(None),
            inner,
        })
    }

    fn notify(&self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    // This should NEVER be called concurrently from different futures/tasks,
    // because we store only 1 waker
    fn poll_wait(&self, cx: &mut Context<'_>) -> Poll<Option<T::Item>> {
        if let Some(item) = self.inner.extract_item() {
            Poll::Ready(Some(item))
        } else if self.inner.closed() {
            Poll::Ready(None)
        } else {
            let new_waker = match self.waker.replace(None) {
                Some(waker) if waker.will_wake(cx.waker()) => waker,
                _ => cx.waker().clone(),
            };
            self.waker.set(Some(new_waker));
            Poll::Pending
        }
    }
}

impl<T> Deref for SharedState<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

// ------------------------------------------------------------------------------------------------

struct ChannelData<T> {
    queue: Queue<T>,
    sender_count: Cell<usize>,
    #[cfg(debug_assertions)]
    has_receiver: Cell<bool>,
}

impl<T> Source for ChannelData<T> {
    type Item = T;

    fn closed(&self) -> bool {
        self.sender_count.get() == 0
    }

    fn extract_item(&self) -> Option<Self::Item> {
        self.queue.pop()
    }
}

type ChannelStateRc<T> = Rc<SharedState<ChannelData<T>>>;

pub struct Sender<T>(ChannelStateRc<T>);

pub struct Receiver<T>(ChannelStateRc<T>);

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let state = SharedState::new(ChannelData {
        queue: Default::default(),
        sender_count: Cell::new(1),
        #[cfg(debug_assertions)]
        has_receiver: Cell::new(true),
    });
    (Sender(state.clone()), Receiver(state))
}

impl<T> Sender<T> {
    pub fn send(&self, item: T) {
        #[cfg(debug_assertions)]
        debug_assert!(self.0.has_receiver.get());
        self.0.queue.push(item);
        self.0.notify();
    }

    #[must_use]
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
        self.0.notify();
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let prev_count = self.0.sender_count.get();
        self.0.sender_count.set(prev_count + 1);
        Self(self.0.clone())
    }
}

impl<T> Receiver<T> {
    pub fn has_pending_data(&self) -> bool {
        !self.0.queue.is_empty()
    }
}

impl<T> futures::Stream for Receiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_wait(cx)
    }
}

#[cfg(debug_assertions)]
impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.0.has_receiver.set(false);
    }
}

// ------------------------------------------------------------------------------------------------

struct SemaphoreData {
    capacity: Cell<usize>,
    has_notifier: Cell<bool>,
    #[cfg(debug_assertions)]
    has_waiter: Cell<bool>,
}

impl Source for SemaphoreData {
    type Item = ();

    fn closed(&self) -> bool {
        !self.has_notifier.get()
    }

    fn extract_item(&self) -> Option<Self::Item> {
        let current_capacity = self.capacity.get();
        if !self.closed() && current_capacity > 0 {
            self.capacity.set(current_capacity - 1);
            Some(())
        } else {
            None
        }
    }
}

type SemaphoreStateRc = Rc<SharedState<SemaphoreData>>;

pub struct Notifier(SemaphoreStateRc);

pub struct Waiter(SemaphoreStateRc);

pub fn semaphore(initial_capacity: usize) -> (Notifier, Waiter) {
    let state = SharedState::new(SemaphoreData {
        capacity: Cell::new(initial_capacity),
        has_notifier: Cell::new(true),
        #[cfg(debug_assertions)]
        has_waiter: Cell::new(true),
    });
    (Notifier(state.clone()), Waiter(state))
}

impl Notifier {
    pub fn signal_one(&self) {
        #[cfg(debug_assertions)]
        debug_assert!(self.0.has_waiter.get());
        let current_capacity = self.0.capacity.get();
        self.0.capacity.set(current_capacity + 1);
        self.0.notify();
    }
}

impl Drop for Notifier {
    fn drop(&mut self) {
        self.0.has_notifier.set(false);
        self.0.notify();
    }
}

impl Waiter {
    pub fn acquire_one(&mut self) -> impl Future<Output = bool> + '_ {
        poll_fn(|cx| self.0.poll_wait(cx)).map(|v| v.is_some())
    }

    pub fn drain(&mut self) -> usize {
        self.0.capacity.replace(0)
    }
}

#[cfg(debug_assertions)]
impl Drop for Waiter {
    fn drop(&mut self) {
        self.0.has_waiter.set(false);
    }
}

// ------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use futures::prelude::*;
    use static_assertions::*;
    use std::sync::Arc;
    use tokio_test::task::spawn;
    use tokio_test::{assert_pending, assert_ready};

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

    #[test]
    fn test_sender_notifies_receiver() {
        let (sender, receiver) = channel::<i32>();

        let mut receiver = spawn(receiver);
        assert_pending!(receiver.poll_next());

        sender.send(42);
        assert!(receiver.is_woken());
        assert_eq!(Some(42), assert_ready!(receiver.poll_next()));
        assert_pending!(receiver.poll_next());

        drop(sender);
        assert!(receiver.is_woken());
        assert_eq!(None, assert_ready!(receiver.poll_next()));
    }

    #[test]
    fn test_receiver_drains_queue_after_sender_dies() {
        let (sender, receiver) = channel::<i32>();

        for i in 0..42 {
            sender.send(i);
        }
        drop(sender);

        let mut receiver = spawn(receiver);
        for i in 0..42 {
            let received = assert_ready!(receiver.poll_next());
            assert_eq!(Some(i), received);
        }
        assert_eq!(None, assert_ready!(receiver.poll_next()));
    }

    #[test]
    fn test_semaphore() {
        let (notifier, mut waiter) = semaphore(2);

        let ret = assert_ready!(spawn(waiter.acquire_one()).poll());
        assert!(ret);
        let ret = assert_ready!(spawn(waiter.acquire_one()).poll());
        assert!(ret);
        let mut wait_fut = spawn(waiter.acquire_one());
        assert_pending!(wait_fut.poll());

        notifier.signal_one();
        assert!(wait_fut.is_woken());

        let ret = assert_ready!(wait_fut.poll());
        assert!(ret);
        drop(wait_fut);
        let mut wait_fut = spawn(waiter.acquire_one());
        assert_pending!(wait_fut.poll());

        notifier.signal_one();
        notifier.signal_one();
        assert!(wait_fut.is_woken());

        let ret = assert_ready!(wait_fut.poll());
        assert!(ret);
        drop(wait_fut);
        let ret = assert_ready!(spawn(waiter.acquire_one()).poll());
        assert!(ret);

        let mut wait_fut = spawn(waiter.acquire_one());
        assert_pending!(wait_fut.poll());

        drop(notifier);
        assert!(wait_fut.is_woken());
        let ret = assert_ready!(wait_fut.poll());
        assert!(!ret);
    }

    #[test]
    fn test_semaphore_ignores_capacity_when_notifier_dies() {
        let (notifier, mut waiter) = semaphore(2);
        drop(notifier);

        let ret = assert_ready!(spawn(waiter.acquire_one()).poll());
        assert!(!ret);
    }

    #[test]
    fn test_drain_semaphore() {
        let (notifier, mut waiter) = semaphore(3);

        let ret = assert_ready!(spawn(waiter.acquire_one()).poll());
        assert!(ret);

        assert_eq!(2, waiter.drain());
        let mut wait_fut = spawn(waiter.acquire_one());
        assert_pending!(wait_fut.poll());

        notifier.signal_one();
        assert!(wait_fut.is_woken());
        let ret = assert_ready!(wait_fut.poll());
        assert!(ret);
    }
}
