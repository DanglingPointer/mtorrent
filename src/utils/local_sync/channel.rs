use super::shared_state::{SharedState, Source};
use crate::utils::sealed;
use std::cell::Cell;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

struct Data<T> {
    queue: sealed::Queue<T>,
    sender_count: Cell<usize>,
    #[cfg(debug_assertions)]
    has_receiver: Cell<bool>,
}

impl<T> Source for Data<T> {
    type Item = T;

    fn closed(&self) -> bool {
        self.sender_count.get() == 0
    }

    fn extract_item(&self) -> Option<Self::Item> {
        self.queue.pop()
    }
}

type StateRc<T> = Rc<SharedState<Data<T>>>;

pub struct Sender<T>(StateRc<T>);

pub struct Receiver<T>(StateRc<T>);

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let state = SharedState::new(Data {
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

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.0.receiver_dropped();
        #[cfg(debug_assertions)]
        self.0.has_receiver.set(false);
    }
}

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
}
