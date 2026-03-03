use local_async_utils::prelude::*;
use std::{cell::Cell, rc::Rc};

#[derive(Clone)]
pub struct Sender<T> {
    data: Rc<Cell<T>>,
    signal_sender: local_condvar::Sender,
}

impl<T> Sender<T> {
    /// Update the value and notify the receiver.
    pub fn set_and_notify(&self, val: T) {
        self.data.set(val);
        self.signal_sender.signal_one();
    }
}

pub struct Receiver<T> {
    data: Rc<Cell<T>>,
    signal_receiver: local_condvar::Receiver,
}

impl<T: Copy> Receiver<T> {
    /// Wait for a notification and get the latest value. Returns `None` if the sender was dropped.
    pub async fn wait_and_get(&mut self) -> Option<T> {
        let has_sender = self.signal_receiver.wait_for_one().await;
        if has_sender {
            Some(self.data.get())
        } else {
            None
        }
    }
}

/// Create a channel that can hold a single value of type `T`. The sender can update the value and
/// notify the receiver, which can wait for updates and retrieve the latest value.
///
/// This is a simplified version of `tokio::sync::watch` for single-threaded use.
pub fn channel<T: Copy>(value: T) -> (Sender<T>, Receiver<T>) {
    let data = Rc::new(Cell::new(value));
    let (sender, receiver) = local_condvar::condvar();
    (
        Sender {
            data: data.clone(),
            signal_sender: sender,
        },
        Receiver {
            data,
            signal_receiver: receiver,
        },
    )
}
