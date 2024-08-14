use super::shared_state::{SharedState, Source};
use futures::FutureExt;
use std::cell::Cell;
use std::future::{poll_fn, Future};
use std::rc::Rc;

struct Data {
    capacity: Cell<usize>,
    has_sender: Cell<bool>,
    #[cfg(debug_assertions)]
    has_receiver: Cell<bool>,
}

impl Source for Data {
    type Item = ();

    fn closed(&self) -> bool {
        !self.has_sender.get()
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

type StateRc = Rc<SharedState<Data>>;

pub struct Sender(StateRc);

pub struct Receiver(StateRc);

pub fn semaphore(initial_capacity: usize) -> (Sender, Receiver) {
    let state = SharedState::new(Data {
        capacity: Cell::new(initial_capacity),
        has_sender: Cell::new(true),
        #[cfg(debug_assertions)]
        has_receiver: Cell::new(true),
    });
    (Sender(state.clone()), Receiver(state))
}

impl Sender {
    pub fn signal_one(&self) {
        #[cfg(debug_assertions)]
        debug_assert!(self.0.has_receiver.get());
        let current_capacity = self.0.capacity.get();
        self.0.capacity.set(current_capacity + 1);
        self.0.notify();
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        self.0.has_sender.set(false);
        self.0.notify();
    }
}

impl Receiver {
    pub fn acquire_one(&mut self) -> impl Future<Output = bool> + '_ {
        poll_fn(|cx| self.0.poll_wait(cx)).map(|v| v.is_some())
    }

    pub fn drain(&mut self) -> usize {
        self.0.capacity.replace(0)
    }
}

#[cfg(debug_assertions)]
impl Drop for Receiver {
    fn drop(&mut self) {
        self.0.has_receiver.set(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test::task::spawn;
    use tokio_test::{assert_pending, assert_ready};

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
