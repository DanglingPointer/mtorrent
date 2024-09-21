use super::shared_state::{SharedState, Source};
use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

struct Data<T> {
    value: Cell<Option<T>>,
    has_sender: Cell<bool>,
    has_receiver: Cell<bool>,
}

impl<T> Source for Data<T> {
    type Item = T;

    fn closed(&self) -> bool {
        !self.has_sender.get()
    }

    fn extract_item(&self) -> Option<Self::Item> {
        self.value.replace(None)
    }
}

type StateRc<T> = Rc<SharedState<Data<T>>>;

pub struct Sender<T>(StateRc<T>);

pub struct Receiver<T>(StateRc<T>);

pub fn oneshot<T>() -> (Sender<T>, Receiver<T>) {
    let state = SharedState::new(Data {
        value: Cell::new(None),
        has_sender: Cell::new(true),
        has_receiver: Cell::new(true),
    });
    (Sender(state.clone()), Receiver(state))
}

impl<T> Sender<T> {
    pub fn send(self, value: T) -> bool {
        if self.0.has_receiver.get() {
            self.0.value.set(Some(value));
            self.0.notify();
            true
        } else {
            false
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.0.has_sender.set(false);
        self.0.notify();
    }
}

impl<T> Future for Receiver<T> {
    type Output = Option<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_wait(cx)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.0.receiver_dropped();
        self.0.has_receiver.set(false);
    }
}
