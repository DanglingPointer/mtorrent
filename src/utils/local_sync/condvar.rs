use super::shared_state::{SharedState, Source};
use futures::FutureExt;
use std::cell::Cell;
use std::future::{poll_fn, Future};
use std::rc::Rc;

struct Data {
    notified: Cell<bool>,
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
        if !self.closed() && self.notified.get() {
            self.notified.set(false);
            Some(())
        } else {
            None
        }
    }
}

type StateRc = Rc<SharedState<Data>>;

pub struct Sender(StateRc);

pub struct Receiver(StateRc);

pub fn condvar() -> (Sender, Receiver) {
    let state = SharedState::new(Data {
        notified: Cell::new(false),
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
        self.0.notified.set(true);
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
    pub fn wait_for_one(&mut self) -> impl Future<Output = bool> + '_ {
        poll_fn(|cx| self.0.poll_wait(cx)).map(|v| v.is_some())
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.0.receiver_dropped();
        #[cfg(debug_assertions)]
        self.0.has_receiver.set(false);
    }
}
