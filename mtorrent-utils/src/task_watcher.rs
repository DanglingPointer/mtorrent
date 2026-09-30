use futures_util::Stream;
use futures_util::stream::FusedStream;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::sync::mpsc;

/// Tracks a set of futures and reports when each of them exits, i.e. completes or is dropped.
///
/// Every future wrapped via [`watch`](Self::watch) or [`watch_tagged`](Self::watch_tagged)
/// reports its exit exactly once: either when it completes or when it is dropped before
/// completion, whichever happens first. Note that when a future is dropped before completion,
/// the exit is reported before the inner future itself is dropped.
///
/// Once all futures have been wrapped, call [`into_exits`](Self::into_exits) to get a stream
/// of [`Exit`]s. The stream ends after every watched future has exited.
#[derive(Debug)]
pub struct TaskWatcher<T> {
    tx: mpsc::UnboundedSender<Exit<T>>,
    rx: mpsc::UnboundedReceiver<Exit<T>>,
}

impl<T> TaskWatcher<T> {
    /// Create a watcher with no watched futures.
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self { tx, rx }
    }

    /// Wrap `future` so that its exit is reported with `tag`.
    pub fn watch_tagged<F: Future>(&self, tag: T, future: F) -> Watched<F, T> {
        Watched {
            future,
            notifier: Some(Notifier {
                tx: self.tx.clone(),
                tag,
            }),
        }
    }

    /// Same as [`watch_tagged`](Self::watch_tagged) using `T::default()` as the tag.
    pub fn watch<F: Future>(&self, future: F) -> Watched<F, T>
    where
        T: Default,
    {
        self.watch_tagged(T::default(), future)
    }

    /// Stop watching new futures and return a stream of exits of the watched futures.
    /// The stream ends once all of them have exited.
    pub fn into_exits(self) -> Exits<T> {
        Exits { rx: self.rx }
    }
}

impl<T> Default for TaskWatcher<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// How a watched future exited, together with its tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit<T> {
    /// The future ran to completion.
    Completed(T),
    /// The future was dropped before completion, e.g. because it was aborted or panicked.
    Dropped(T),
}

impl<T> Exit<T> {
    /// Tag of the exited future.
    pub fn tag(&self) -> &T {
        match self {
            Self::Completed(tag) | Self::Dropped(tag) => tag,
        }
    }

    /// Consume the exit and return the tag of the exited future.
    pub fn into_tag(self) -> T {
        match self {
            Self::Completed(tag) | Self::Dropped(tag) => tag,
        }
    }

    /// Whether the future ran to completion.
    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed(_))
    }
}

/// Stream of exits of watched futures, returned by [`TaskWatcher::into_exits`].
///
/// If a future completes, [`Exit::Completed`] is reported immediately, even if the [`Watched`]
/// future is dropped later. [`Exit::Dropped`] is only reported for futures dropped before
/// completion.
#[derive(Debug)]
pub struct Exits<T> {
    rx: mpsc::UnboundedReceiver<Exit<T>>,
}

impl<T> Stream for Exits<T> {
    type Item = Exit<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl<T> FusedStream for Exits<T> {
    fn is_terminated(&self) -> bool {
        self.rx.is_closed() && self.rx.is_empty()
    }
}

pin_project! {
    /// Future returned by [`TaskWatcher::watch`] and [`TaskWatcher::watch_tagged`].
    #[derive(Debug)]
    pub struct Watched<F, T> {
        #[pin]
        future: F,
        notifier: Option<Notifier<T>>,
    }

    impl<F, T> PinnedDrop for Watched<F, T> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if let Some(notifier) = this.notifier.take() {
                notifier.fire(Exit::Dropped);
            }
        }
    }
}

impl<F, T> Future for Watched<F, T>
where
    F: Future,
{
    type Output = <F as Future>::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let ret = ready!(this.future.poll(cx));
        if let Some(notifier) = this.notifier.take() {
            notifier.fire(Exit::Completed);
        }
        Poll::Ready(ret)
    }
}

#[derive(Debug)]
struct Notifier<T> {
    tx: mpsc::UnboundedSender<Exit<T>>,
    tag: T,
}

impl<T> Notifier<T> {
    fn fire(self, exit: fn(T) -> Exit<T>) {
        _ = self.tx.send(exit(self.tag));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::{pending, ready};
    use tokio::sync::oneshot;
    use tokio_test::{assert_pending, assert_ready, assert_ready_eq, task};

    #[test]
    fn test_no_watched_futures() {
        let watcher = TaskWatcher::<u32>::new();
        let mut exits = task::spawn(watcher.into_exits());
        assert_ready_eq!(exits.poll_next(), None);
    }

    #[test]
    fn test_pending_while_future_alive() {
        let watcher = TaskWatcher::new();
        let watched = watcher.watch_tagged(1, pending::<()>());
        let mut exits = task::spawn(watcher.into_exits());
        assert_pending!(exits.poll_next());

        drop(watched);
        assert!(exits.is_woken());
        assert_ready_eq!(exits.poll_next(), Some(Exit::Dropped(1)));
        assert_ready_eq!(exits.poll_next(), None);
    }

    #[test]
    fn test_reports_completion_before_drop() {
        let watcher = TaskWatcher::new();
        let mut watched = task::spawn(watcher.watch_tagged(1, ready(42)));
        let mut exits = task::spawn(watcher.into_exits());
        assert_pending!(exits.poll_next());

        assert_ready_eq!(watched.poll(), 42);
        assert!(exits.is_woken());
        assert_ready_eq!(exits.poll_next(), Some(Exit::Completed(1)));
        // The sender is released on completion, so the stream ends without dropping `watched`.
        assert_ready_eq!(exits.poll_next(), None);
        drop(watched);
    }

    #[test]
    fn test_reports_drop_without_completion() {
        let watcher = TaskWatcher::new();
        let watched = watcher.watch_tagged(7, pending::<()>());
        drop(watched);
        let mut exits = task::spawn(watcher.into_exits());
        assert_ready_eq!(exits.poll_next(), Some(Exit::Dropped(7)));
        assert_ready_eq!(exits.poll_next(), None);
    }

    #[test]
    fn test_drains_all_exits_in_order() {
        let watcher = TaskWatcher::new();
        let a = watcher.watch_tagged(1, pending::<()>());
        let b = watcher.watch_tagged(2, pending::<()>());
        let c = watcher.watch_tagged(3, pending::<()>());
        let mut exits = task::spawn(watcher.into_exits());
        drop(b);
        drop(c);
        drop(a);
        for expected in [2, 3, 1] {
            assert_ready_eq!(exits.poll_next(), Some(Exit::Dropped(expected)));
        }
        assert_ready_eq!(exits.poll_next(), None);
    }

    #[test]
    fn test_is_terminated() {
        let watcher = TaskWatcher::new();
        let a = watcher.watch_tagged(1, pending::<()>());
        let b = watcher.watch_tagged(2, pending::<()>());
        let mut exits = task::spawn(watcher.into_exits());
        assert!(!exits.is_terminated());

        drop(a);
        assert!(!exits.is_terminated());
        assert_ready_eq!(exits.poll_next(), Some(Exit::Dropped(1)));
        assert!(!exits.is_terminated());

        // All senders are gone, but an exit is still queued.
        drop(b);
        assert!(!exits.is_terminated());
        assert_ready_eq!(exits.poll_next(), Some(Exit::Dropped(2)));
        assert!(exits.is_terminated());
        assert_ready_eq!(exits.poll_next(), None);
        assert!(exits.is_terminated());
    }

    #[test]
    fn test_is_terminated_without_watched_futures() {
        let exits = TaskWatcher::<u32>::new().into_exits();
        assert!(exits.is_terminated());
    }

    #[test]
    fn test_default_tag() {
        let watcher = TaskWatcher::<u32>::default();
        drop(watcher.watch(pending::<()>()));
        let mut exits = task::spawn(watcher.into_exits());
        assert_ready_eq!(exits.poll_next(), Some(Exit::Dropped(0)));
        assert_ready_eq!(exits.poll_next(), None);
    }

    #[test]
    fn test_waiting_stream_is_woken_on_completion() {
        let watcher = TaskWatcher::new();
        let (tx, rx) = oneshot::channel::<()>();
        let mut watched = task::spawn(watcher.watch_tagged(5, rx));
        let mut exits = task::spawn(watcher.into_exits());

        assert_pending!(exits.poll_next());
        assert_pending!(watched.poll());

        tx.send(()).unwrap();
        assert!(watched.is_woken());
        assert_ready!(watched.poll()).unwrap();
        assert!(exits.is_woken());
        assert_ready_eq!(exits.poll_next(), Some(Exit::Completed(5)));
        assert_ready_eq!(exits.poll_next(), None);
        drop(watched);
    }

    #[test]
    fn test_reports_mixed_exits() {
        let watcher = TaskWatcher::new();
        let mut completing = task::spawn(watcher.watch_tagged("completing", ready(())));
        let dropped = watcher.watch_tagged("dropped", pending::<()>());
        let mut exits = task::spawn(watcher.into_exits());

        drop(dropped);
        assert_ready!(completing.poll());
        drop(completing);
        assert_ready_eq!(exits.poll_next(), Some(Exit::Dropped("dropped")));
        assert_ready_eq!(exits.poll_next(), Some(Exit::Completed("completing")));
        assert_ready_eq!(exits.poll_next(), None);
    }

    #[test]
    fn test_exit_accessors() {
        let completed = Exit::Completed(1);
        assert!(completed.is_completed());
        assert_eq!(completed.tag(), &1);
        assert_eq!(completed.into_tag(), 1);

        let dropped = Exit::Dropped(2);
        assert!(!dropped.is_completed());
        assert_eq!(dropped.tag(), &2);
        assert_eq!(dropped.into_tag(), 2);
    }

    #[test]
    fn test_polled_between_send_and_sender_drop() {
        // Tries to hit the window where a watched future dropped on another thread has already
        // sent its exit but not yet dropped its sender, while the stream yields the exit and is
        // immediately polled again. That poll must be woken once the sender is dropped, instead
        // of staying pending forever.
        const ITERATIONS: usize = 10_000;
        let mut window_hits = 0;
        for _ in 0..ITERATIONS {
            let watcher = TaskWatcher::new();
            let watched = watcher.watch_tagged(1, pending::<()>());
            let mut exits = task::spawn(watcher.into_exits());
            let dropper = std::thread::spawn(move || drop(watched));

            loop {
                match exits.poll_next() {
                    Poll::Ready(exit) => {
                        assert_eq!(exit, Some(Exit::Dropped(1)));
                        break;
                    }
                    Poll::Pending => std::hint::spin_loop(),
                }
            }

            let first_poll = exits.poll_next();
            dropper.join().unwrap();
            match first_poll {
                Poll::Ready(exit) => assert_eq!(exit, None),
                Poll::Pending => {
                    window_hits += 1;
                    assert!(exits.is_woken(), "stream not woken after the last sender dropped");
                    assert_ready_eq!(exits.poll_next(), None);
                }
            }
        }
        println!("race window hit {window_hits}/{ITERATIONS} times");
    }

    #[test]
    fn test_no_premature_end_across_threads() {
        const TASKS: usize = 16;
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();

        for _ in 0..200 {
            let watcher = TaskWatcher::new();
            let watched: Vec<_> =
                (0..TASKS).map(|i| watcher.watch_tagged(i, pending::<()>())).collect();
            let mut exits = watcher.into_exits();
            let threads: Vec<_> = watched
                .into_iter()
                .map(|watched| std::thread::spawn(move || drop(watched)))
                .collect();
            let received: Vec<_> = rt.block_on(async {
                use futures_util::StreamExt;
                let mut received = Vec::with_capacity(TASKS);
                while let Some(exit) = exits.next().await {
                    assert!(!exit.is_completed());
                    received.push(exit.into_tag());
                }
                received
            });
            threads.into_iter().for_each(|t| t.join().unwrap());
            let mut received = received;
            received.sort_unstable();
            assert_eq!(received, (0..TASKS).collect::<Vec<_>>());
        }
    }
}
