use crate::utils::sealed::Set;
use std::cell::Cell;
use std::future::poll_fn;
use std::net::SocketAddr;
use std::ops::Deref;
use std::rc::Rc;
use std::task::{Poll, Waker};

pub fn connection_control<D>(
    connection_limit: usize,
    connection_data: D,
) -> (OutgoingConnectionControl<D>, IncomingConnectionControl<D>) {
    let state = Rc::new(State {
        budget: Cell::new(connection_limit),
        used_addrs: Default::default(),
        waker: Default::default(),
        data: connection_data,
    });
    (
        OutgoingConnectionControl {
            state: state.clone(),
        },
        IncomingConnectionControl { state },
    )
}

struct State<D> {
    budget: Cell<usize>,
    used_addrs: Set<SocketAddr>,
    waker: Cell<Option<Waker>>,
    data: D,
}

pub(super) struct ConnectionPermit<D> {
    state: Rc<State<D>>,
    addr: SocketAddr,
}

#[cfg(test)]
impl<D> std::fmt::Debug for ConnectionPermit<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionPermit").field("addr", &self.addr).finish()
    }
}

impl<D> Deref for ConnectionPermit<D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        &self.state.data
    }
}

impl<D> Drop for ConnectionPermit<D> {
    fn drop(&mut self) {
        self.state.budget.set(self.state.budget.get() + 1);
        self.state.used_addrs.remove(&self.addr);
        if let Some(waker) = self.state.waker.take() {
            waker.wake();
        }
    }
}

#[cfg_attr(test, derive(Debug))]
pub struct OutgoingConnectionPermit<D>(pub(super) ConnectionPermit<D>);
#[cfg_attr(test, derive(Debug))]
pub struct IncomingConnectionPermit<D>(pub(super) ConnectionPermit<D>);

pub struct OutgoingConnectionControl<D> {
    state: Rc<State<D>>,
}

impl<D> OutgoingConnectionControl<D> {
    /// Wait for available connection slot or return None if already connected to 'addr'
    pub async fn issue_permit(&mut self, addr: SocketAddr) -> Option<OutgoingConnectionPermit<D>> {
        // must use `&mut self` because we store only 1 waker
        poll_fn(move |cx| {
            if self.state.used_addrs.contains(&addr) {
                Poll::Ready(None)
            } else if self.state.budget.get() == 0 {
                let new_waker = match self.state.waker.replace(None) {
                    Some(waker) if waker.will_wake(cx.waker()) => waker,
                    _ => cx.waker().clone(),
                };
                self.state.waker.set(Some(new_waker));
                Poll::Pending
            } else {
                self.state.used_addrs.insert(addr);
                self.state.budget.set(self.state.budget.get() - 1);
                Poll::Ready(Some(OutgoingConnectionPermit(ConnectionPermit {
                    state: self.state.clone(),
                    addr,
                })))
            }
        })
        .await
    }
}

pub struct IncomingConnectionControl<D> {
    state: Rc<State<D>>,
}

impl<D> IncomingConnectionControl<D> {
    /// Issue immediate permit if a connection slot is available
    pub fn issue_permit(&mut self, addr: SocketAddr) -> Option<IncomingConnectionPermit<D>> {
        debug_assert!(!self.state.used_addrs.contains(&addr));
        if self.state.budget.get() == 0 {
            None
        } else {
            self.state.used_addrs.insert(addr);
            self.state.budget.set(self.state.budget.get() - 1);
            Some(IncomingConnectionPermit(ConnectionPermit {
                state: self.state.clone(),
                addr,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use tokio_test::task::spawn;
    use tokio_test::{assert_pending, assert_ready};
    macro_rules! addr {
        ($addr:literal) => {
            SocketAddr::from_str($addr).unwrap()
        };
    }

    #[test]
    fn test_incoming_control_respects_budget() {
        let (_out_ctrl, mut in_ctrl) = connection_control(2, ());

        // when
        let permit1 = in_ctrl.issue_permit(addr!("1.2.3.4:1111"));
        assert!(permit1.is_some());
        let permit2 = in_ctrl.issue_permit(addr!("1.2.3.4:1112"));
        assert!(permit2.is_some());

        // then
        let permit3 = in_ctrl.issue_permit(addr!("1.2.3.4:1113"));
        assert!(permit3.is_none());

        // when
        drop(permit2);

        // then
        let permit3 = in_ctrl.issue_permit(addr!("1.2.3.4:1113"));
        assert!(permit3.is_some());
    }

    #[test]
    fn test_notify_when_outgoing_connection_drops() {
        let (mut out_ctrl, _in_ctrl) = connection_control(1, ());

        let permit1 = {
            let mut fut1 = spawn(out_ctrl.issue_permit(addr!("1.2.3.4:1111")));
            assert_ready!(fut1.poll())
        };
        assert!(permit1.is_some());

        let mut fut2 = spawn(out_ctrl.issue_permit(addr!("1.2.3.4:2222")));
        assert_pending!(fut2.poll());

        drop(permit1);
        assert!(fut2.is_woken());
        let permit2 = assert_ready!(fut2.poll());
        assert!(permit2.is_some(), "permit denied");
    }

    #[test]
    fn test_notify_when_incoming_connection_drops() {
        let (mut out_ctrl, mut in_ctrl) = connection_control(1, ());
        let permit1 = in_ctrl.issue_permit(addr!("1.2.3.4:1112"));
        assert!(permit1.is_some());

        let mut fut2 = spawn(out_ctrl.issue_permit(addr!("1.2.3.4:1111")));
        assert_pending!(fut2.poll());

        drop(permit1);
        assert!(fut2.is_woken());
        let permit2 = assert_ready!(fut2.poll());
        assert!(permit2.is_some(), "permit denied");
    }

    #[test]
    fn test_outgoing_control_respects_uniqueness() {
        let (mut out_ctrl, mut in_ctrl) = connection_control(10, ());

        // when
        let permit1 = {
            let mut fut1 = spawn(out_ctrl.issue_permit(addr!("1.2.3.4:1111")));
            assert_ready!(fut1.poll())
        };
        assert!(permit1.is_some());

        // then
        let permit1_dup = {
            let mut fut1 = spawn(out_ctrl.issue_permit(addr!("1.2.3.4:1111")));
            assert_ready!(fut1.poll())
        };
        assert!(permit1_dup.is_none());

        // when
        let permit2 = {
            let mut fut2 = spawn(out_ctrl.issue_permit(addr!("1.2.3.4:2222")));
            assert_ready!(fut2.poll())
        };
        assert!(permit2.is_some());

        // then
        let permit2_dup = {
            let mut fut2_dup = spawn(out_ctrl.issue_permit(addr!("1.2.3.4:2222")));
            assert_ready!(fut2_dup.poll())
        };
        assert!(permit2_dup.is_none());

        // when
        let permit3 = in_ctrl.issue_permit(addr!("1.2.3.4:3333"));
        assert!(permit3.is_some());

        // then
        let permit3_dup = {
            let mut fut3_dup = spawn(out_ctrl.issue_permit(addr!("1.2.3.4:3333")));
            assert_ready!(fut3_dup.poll())
        };
        assert!(permit3_dup.is_none());
    }
}
