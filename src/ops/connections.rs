use crate::utils::set::Set;
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
    let state = SharedState::new(State {
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

type SharedState<D> = Rc<State<D>>;

pub(super) struct ConnectionPermit<D> {
    state: SharedState<D>,
    addr: Option<SocketAddr>,
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
        if let Some(addr) = self.addr {
            self.state.used_addrs.remove(&addr);
        }
        if let Some(waker) = self.state.waker.take() {
            waker.wake();
        }
    }
}

pub struct OutgoingConnectionPermit<D>(pub(super) ConnectionPermit<D>);
pub struct IncomingConnectionPermit<D>(pub(super) ConnectionPermit<D>);

pub struct OutgoingConnectionControl<D> {
    state: SharedState<D>,
}

impl<D> OutgoingConnectionControl<D> {
    /// Wait for available connection slot or returns None if already connected to 'addr'
    pub async fn issue_permit(&mut self, addr: SocketAddr) -> Option<OutgoingConnectionPermit<D>> {
        // must use `&mut self` because we store only 1 waker
        poll_fn(move |cx| {
            if self.state.used_addrs.contains(&addr) {
                Poll::Ready(None)
            } else if self.state.budget.get() == 0 {
                let _old_waker = self.state.waker.replace(Some(cx.waker().clone()));
                debug_assert!(_old_waker.map_or(true, |waker| waker.will_wake(cx.waker())));
                Poll::Pending
            } else {
                self.state.used_addrs.insert(addr);
                self.state.budget.set(self.state.budget.get() - 1);
                Poll::Ready(Some(OutgoingConnectionPermit(ConnectionPermit {
                    state: self.state.clone(),
                    addr: Some(addr),
                })))
            }
        })
        .await
    }
}

pub struct IncomingConnectionControl<D> {
    state: SharedState<D>,
}

impl<D> IncomingConnectionControl<D> {
    /// Issue immediate permit if a connection slot is available
    pub fn issue_permit(&mut self) -> Option<IncomingConnectionPermit<D>> {
        if self.state.budget.get() == 0 {
            None
        } else {
            self.state.budget.set(self.state.budget.get() - 1);
            Some(IncomingConnectionPermit(ConnectionPermit {
                state: self.state.clone(),
                addr: None,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sec;
    use futures::prelude::*;
    use std::str::FromStr;
    use tokio::{pin, select, task, time::sleep};

    macro_rules! with_timeout {
        ($fut:expr) => {
            tokio::time::timeout(sec!(60), $fut).await
        };
    }
    macro_rules! addr {
        ($addr:literal) => {
            SocketAddr::from_str($addr).unwrap()
        };
    }

    #[test]
    fn test_incoming_control_respects_budget() {
        let (_out_ctrl, mut in_ctrl) = connection_control(2, ());

        // when
        let permit1 = in_ctrl.issue_permit();
        assert!(permit1.is_some());
        let permit2 = in_ctrl.issue_permit();
        assert!(permit2.is_some());

        // then
        let permit3 = in_ctrl.issue_permit();
        assert!(permit3.is_none());

        // when
        drop(permit2);

        // then
        let permit3 = in_ctrl.issue_permit();
        assert!(permit3.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn test_outgoing_control_respects_budget() {
        let (mut out_ctrl, _in_ctrl) = connection_control(2, ());

        // when
        let permit1 =
            with_timeout!(out_ctrl.issue_permit(addr!("1.2.3.4:1111"))).expect("timed out");
        assert!(permit1.is_some());
        let permit2 =
            with_timeout!(out_ctrl.issue_permit(addr!("1.2.3.4:2222"))).expect("timed out");
        assert!(permit2.is_some());

        // then
        let permit3 = with_timeout!(out_ctrl.issue_permit(addr!("1.2.3.4:3333")));
        assert!(permit3.is_err());

        // when
        drop(permit2);

        // then
        let permit3 =
            with_timeout!(out_ctrl.issue_permit(addr!("1.2.3.4:3333"))).expect("timed out");
        assert!(permit3.is_some());
        let permit4 = with_timeout!(out_ctrl.issue_permit(addr!("1.2.3.4:4444")));
        assert!(permit4.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn test_outgoing_control_notifies_when_bugdet_becomes_available() {
        task::LocalSet::new()
            .run_until(async move {
                let (mut out_ctrl, _in_ctrl) = connection_control(1, ());
                let permit1 =
                    with_timeout!(out_ctrl.issue_permit(addr!("1.2.3.4:1111"))).expect("timed out");
                assert!(permit1.is_some());

                task::spawn_local(async move {
                    sleep(sec!(30)).await;
                    drop(permit1);
                });

                let permit_fut = out_ctrl.issue_permit(addr!("1.2.3.4:5555"));
                pin!(permit_fut);

                select! {
                    biased;
                    _ = &mut permit_fut => {
                        panic!("permit issued too early")
                    }
                    _ = sleep(sec!(30)) => {
                    }
                };
                task::yield_now().await;
                let permit2 = permit_fut.now_or_never().expect("permit not issued in time");
                assert!(permit2.is_some(), "permit denied");
            })
            .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_outgoing_control_respects_uniqueness() {
        let (mut out_ctrl, _in_ctrl) = connection_control(2, ());

        // when
        let _permit1 = with_timeout!(out_ctrl.issue_permit(addr!("1.2.3.4:1111")))
            .expect("timed out")
            .expect("permit denied");

        // then
        let permit1_dup =
            with_timeout!(out_ctrl.issue_permit(addr!("1.2.3.4:1111"))).expect("timed out");
        assert!(permit1_dup.is_none());

        // when
        let _permit2 = with_timeout!(out_ctrl.issue_permit(addr!("1.2.3.4:2222")))
            .expect("timed out")
            .expect("permit denied");
        let permit3 = with_timeout!(out_ctrl.issue_permit(addr!("1.2.3.4:3333")));
        assert!(permit3.is_err());

        // then
        let permit2_dup =
            with_timeout!(out_ctrl.issue_permit(addr!("1.2.3.4:2222"))).expect("timed out");
        assert!(permit2_dup.is_none());
    }
}
