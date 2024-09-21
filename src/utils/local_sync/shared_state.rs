use std::cell::Cell;
use std::ops::Deref;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

pub(super) trait Source {
    type Item;
    fn closed(&self) -> bool;
    fn extract_item(&self) -> Option<Self::Item>;
}

pub(super) struct SharedState<T> {
    waker: Cell<Option<Waker>>,
    inner: T,
}

impl<T: Source> SharedState<T> {
    pub(super) fn new(inner: T) -> Rc<Self> {
        Rc::new(Self {
            waker: Cell::new(None),
            inner,
        })
    }

    pub(super) fn notify(&self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    pub(super) fn receiver_dropped(&self) {
        // remove waker so that we don't unnecessarily wake anyone when Sender is dropped
        self.waker.take();
    }

    // This should NEVER be called concurrently from different futures/tasks,
    // because we store only 1 waker
    pub(super) fn poll_wait(self: &mut Rc<Self>, cx: &mut Context<'_>) -> Poll<Option<T::Item>> {
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
