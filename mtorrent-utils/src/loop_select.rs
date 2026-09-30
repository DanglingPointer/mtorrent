use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::task;

/// Type alias for the poll function signature used in [`loop_select`].
pub type LoopSelectPollFn<D, O> = fn(&mut D, &mut Context<'_>) -> Poll<ControlFlow<O>>;

/// Future returned by [`loop_select`].
pub struct LoopSelect<'d, D, O, const N: usize> {
    data: &'d mut D,
    poll_fns: [LoopSelectPollFn<D, O>; N],
}

impl<D, O, const N: usize> Future for LoopSelect<'_, D, O, N> {
    type Output = O;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        loop {
            let coop = ready!(task::coop::poll_proceed(cx));
            let mut made_progress = false;
            for poll_fn in &this.poll_fns {
                match poll_fn(this.data, cx) {
                    Poll::Ready(ControlFlow::Break(out)) => {
                        coop.made_progress();
                        return Poll::Ready(out);
                    }
                    Poll::Ready(ControlFlow::Continue(())) => {
                        made_progress = true;
                    }
                    Poll::Pending => {}
                }
            }
            if !made_progress {
                return Poll::Pending;
            }
            coop.made_progress();
        }
    }
}

/// Run multiple poll functions in a loop until one of them returns [`ControlFlow::Break`].
/// All functions have access to a mutable reference to the same data.
///
/// In each round, the functions are polled in the order of `poll_fns`. A poll function must:
/// - return `Poll::Ready(ControlFlow::Break(output))` to finish the loop with `output`; the
///   remaining functions are not polled in that round,
/// - return `Poll::Ready(ControlFlow::Continue(()))` only if it made progress,
/// - return `Poll::Pending` only after arranging for the waker in `cx` to be woken.
///
/// Rounds are repeated as long as at least one function made progress. The loop takes part in
/// Tokio's cooperative scheduling, so a function that always returns `Continue` doesn't block
/// the thread, but it keeps the task busy forever.
///
/// `poll_fns` must not be empty.
pub fn loop_select<D, O, const N: usize>(
    data: &mut D,
    poll_fns: [LoopSelectPollFn<D, O>; N],
) -> LoopSelect<'_, D, O, N> {
    const { assert!(N > 0, "loop_select requires at least one poll function") };
    LoopSelect { data, poll_fns }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio_test::task::spawn;
    use tokio_test::{assert_pending, assert_ready_eq};

    struct Counters {
        counter1: usize,
        counter2: usize,
        target1: usize,
        target2: usize,
    }

    impl Counters {
        fn new(target1: usize, target2: usize) -> Self {
            Self {
                counter1: 0,
                counter2: 0,
                target1,
                target2,
            }
        }
    }

    fn count1(ctx: &mut Counters, _cx: &mut Context<'_>) -> Poll<ControlFlow<usize>> {
        if ctx.counter1 < ctx.target1 {
            ctx.counter1 += 1;
            Poll::Ready(ControlFlow::Continue(()))
        } else {
            Poll::Ready(ControlFlow::Break(1))
        }
    }

    fn count2(ctx: &mut Counters, _cx: &mut Context<'_>) -> Poll<ControlFlow<usize>> {
        if ctx.counter2 < ctx.target2 {
            ctx.counter2 += 1;
            Poll::Ready(ControlFlow::Continue(()))
        } else {
            Poll::Ready(ControlFlow::Break(2))
        }
    }

    #[test]
    fn test_polls_in_order_and_stops_at_first_break() {
        let mut ctx = Counters::new(5, 3);
        assert_ready_eq!(spawn(loop_select(&mut ctx, [count1, count2])).poll(), 2);
        assert_eq!(ctx.counter1, 4);
        assert_eq!(ctx.counter2, 3);

        let mut ctx = Counters::new(5, 3);
        assert_ready_eq!(spawn(loop_select(&mut ctx, [count2, count1])).poll(), 2);
        assert_eq!(ctx.counter2, 3);
        assert_eq!(ctx.counter1, 3);
    }

    #[test]
    fn test_break_in_first_round() {
        let mut ctx = Counters::new(0, 3);
        assert_ready_eq!(spawn(loop_select(&mut ctx, [count1, count2])).poll(), 1);
        assert_eq!(ctx.counter1, 0);
        assert_eq!(ctx.counter2, 0);
    }

    struct Receivers {
        rx1: mpsc::UnboundedReceiver<u32>,
        rx2: mpsc::UnboundedReceiver<u32>,
        received: Vec<u32>,
    }

    fn recv_into(
        rx: &mut mpsc::UnboundedReceiver<u32>,
        received: &mut Vec<u32>,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Vec<u32>>> {
        match rx.poll_recv(cx) {
            Poll::Ready(Some(value)) => {
                received.push(value);
                Poll::Ready(ControlFlow::Continue(()))
            }
            Poll::Ready(None) => Poll::Ready(ControlFlow::Break(received.clone())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn recv1(ctx: &mut Receivers, cx: &mut Context<'_>) -> Poll<ControlFlow<Vec<u32>>> {
        recv_into(&mut ctx.rx1, &mut ctx.received, cx)
    }

    fn recv2(ctx: &mut Receivers, cx: &mut Context<'_>) -> Poll<ControlFlow<Vec<u32>>> {
        recv_into(&mut ctx.rx2, &mut ctx.received, cx)
    }

    #[test]
    fn test_pending_until_woken() {
        let (tx1, rx1) = mpsc::unbounded_channel();
        let (tx2, rx2) = mpsc::unbounded_channel();
        let mut ctx = Receivers {
            rx1,
            rx2,
            received: Vec::new(),
        };
        let mut fut = spawn(loop_select(&mut ctx, [recv1, recv2]));
        assert_pending!(fut.poll());

        tx2.send(1).unwrap();
        assert!(fut.is_woken());
        assert_pending!(fut.poll());

        tx1.send(2).unwrap();
        tx1.send(3).unwrap();
        assert!(fut.is_woken());
        assert_pending!(fut.poll());

        drop(tx2);
        assert!(fut.is_woken());
        assert_ready_eq!(fut.poll(), vec![1, 2, 3]);
    }

    #[test]
    fn test_continues_while_some_functions_pending() {
        let (tx1, rx1) = mpsc::unbounded_channel();
        let (_tx2, rx2) = mpsc::unbounded_channel();
        let mut ctx = Receivers {
            rx1,
            rx2,
            received: Vec::new(),
        };
        for value in 0..10 {
            tx1.send(value).unwrap();
        }
        drop(tx1);
        // rx2 stays pending, but rx1 keeps making progress until its channel is closed
        assert_ready_eq!(
            spawn(loop_select(&mut ctx, [recv2, recv1])).poll(),
            (0..10).collect::<Vec<_>>()
        );
    }

    fn always_continue(count: &mut usize, _cx: &mut Context<'_>) -> Poll<ControlFlow<()>> {
        *count += 1;
        if *count < 1_000_000 {
            Poll::Ready(ControlFlow::Continue(()))
        } else {
            // Fail instead of hanging if the coop budget is not enforced
            Poll::Ready(ControlFlow::Break(()))
        }
    }

    #[tokio::test]
    async fn test_yields_when_coop_budget_exhausted() {
        let mut count = 0;
        let mut fut = spawn(loop_select(&mut count, [always_continue]));
        assert_pending!(fut.poll());
        // Tokio defers the wakeup until the current task yields to the runtime
        tokio::task::yield_now().await;
        assert!(fut.is_woken());
        drop(fut);
        assert!(count > 0);
        assert!(count < 1_000_000);
    }
}
