use std::cell::Cell;
use std::rc::{Rc, Weak};

struct SharedState {
    cancelled: Cell<bool>,
}

pub struct DetachingHandle {
    state: Weak<SharedState>,
}

impl DetachingHandle {
    pub fn is_active(&self) -> bool {
        self.state.upgrade().is_some()
    }

    pub fn cancel(self) {
        if let Some(state) = self.state.upgrade() {
            state.cancelled.set(true);
        }
    }
}

pub struct OwningHandle {
    state: Weak<SharedState>,
}

impl OwningHandle {
    pub fn is_active(&self) -> bool {
        self.state.upgrade().is_some()
    }
}

impl Drop for OwningHandle {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            state.cancelled.set(true);
        }
    }
}

pub struct Token {
    state: Rc<SharedState>,
}

impl Token {
    pub fn is_cancellation_requested(&self) -> bool {
        self.state.cancelled.get()
    }
}

pub fn new_detaching() -> (DetachingHandle, Token) {
    let state = Rc::new(SharedState {
        cancelled: Cell::new(false),
    });
    let handle = DetachingHandle {
        state: Rc::downgrade(&state),
    };
    let token = Token { state };
    (handle, token)
}

pub fn new_owning() -> (OwningHandle, Token) {
    let state = Rc::new(SharedState {
        cancelled: Cell::new(false),
    });
    let handle = OwningHandle {
        state: Rc::downgrade(&state),
    };
    let token = Token { state };
    (handle, token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detached_canceller_active_state() {
        let (handle, token) = new_detaching();
        assert!(handle.is_active());

        drop(token);
        assert!(!handle.is_active());
    }

    #[test]
    fn test_owning_canceller_active_state() {
        let (handle, token) = new_owning();
        assert!(handle.is_active());

        drop(token);
        assert!(!handle.is_active());
    }

    #[test]
    fn test_detached_canceller_cancels() {
        let (handle, token) = new_detaching();
        assert!(!token.is_cancellation_requested());

        handle.cancel();
        assert!(token.is_cancellation_requested());
    }

    #[test]
    fn test_owning_canceller_cancels() {
        let (handle, token) = new_owning();
        assert!(!token.is_cancellation_requested());

        drop(handle);
        assert!(token.is_cancellation_requested());
    }

    #[test]
    fn test_detached_handle_detaches() {
        let (handle, token) = new_detaching();

        drop(handle);
        assert!(!token.is_cancellation_requested());
    }
}
