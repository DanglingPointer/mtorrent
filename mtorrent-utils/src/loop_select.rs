use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::task;

pub type LoopSelectPollFn<Ctx, Out> = fn(&mut Ctx, cx: &mut Context<'_>) -> Poll<ControlFlow<Out>>;

struct LoopSelectUnpin<'c, Ctx: Unpin, Out, const I: usize> {
    ctx: &'c mut Ctx,
    poll_fns: [LoopSelectPollFn<Ctx, Out>; I],
}

impl<'c, Ctx: Unpin, Out, const I: usize> Future for LoopSelectUnpin<'c, Ctx, Out, I> {
    type Output = Out;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let coop = ready!(task::coop::poll_proceed(cx));
            let mut made_progress = false;
            for poll_fn in self.poll_fns {
                match poll_fn(self.ctx, cx) {
                    Poll::Ready(ControlFlow::Break(out)) => {
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

pub async fn loop_select<Ctx: Unpin, Out, const I: usize>(
    ctx: &mut Ctx,
    poll_fns: [LoopSelectPollFn<Ctx, Out>; I],
) -> Out {
    LoopSelectUnpin { ctx, poll_fns }.await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::ControlFlow;
    use std::task::{Context, Poll};

    struct TestCtx {
        counter1: usize,
        counter2: usize,
        target1: usize,
        target2: usize,
    }

    fn poll_fn1(ctx: &mut TestCtx, _cx: &mut Context<'_>) -> Poll<ControlFlow<()>> {
        if ctx.counter1 < ctx.target1 {
            ctx.counter1 += 1;
            Poll::Ready(ControlFlow::Continue(()))
        } else {
            Poll::Ready(ControlFlow::Break(()))
        }
    }
    fn poll_fn2(ctx: &mut TestCtx, _cx: &mut Context<'_>) -> Poll<ControlFlow<()>> {
        if ctx.counter2 < ctx.target2 {
            ctx.counter2 += 1;
            Poll::Ready(ControlFlow::Continue(()))
        } else {
            Poll::Ready(ControlFlow::Break(()))
        }
    }

    #[tokio::test]
    async fn test_loop_select() {
        let mut ctx = TestCtx {
            counter1: 0,
            counter2: 0,
            target1: 5,
            target2: 3,
        };
        loop_select(&mut ctx, [poll_fn1, poll_fn2]).await;
        assert_eq!(ctx.counter1, 4);
        assert_eq!(ctx.counter2, 3);

        let mut ctx = TestCtx {
            counter1: 0,
            counter2: 0,
            target1: 5,
            target2: 3,
        };
        loop_select(&mut ctx, [poll_fn2, poll_fn1]).await;
        assert_eq!(ctx.counter2, 3);
        assert_eq!(ctx.counter1, 3);
    }
}
