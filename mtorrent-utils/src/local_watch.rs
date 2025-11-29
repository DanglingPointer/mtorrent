use std::{cell::Cell, rc::Rc};

use local_async_utils::prelude::*;

#[derive(Clone)]
pub struct Sender<T> {
    data: Rc<Cell<T>>,
    signal_sender: local_condvar::Sender,
}

impl<T> Sender<T> {
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
    pub async fn wait_and_get(&mut self) -> Option<T> {
        let has_sender = self.signal_receiver.wait_for_one().await;
        if has_sender {
            Some(self.data.get())
        } else {
            None
        }
    }
}
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
