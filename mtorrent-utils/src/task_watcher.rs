use futures_util::Stream;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::sync::mpsc;

/// Tracks a set of futures and reports when each of them completes or is dropped.
///
/// Every future wrapped via [`watch`](Self::watch) or [`watch_tagged`](Self::watch_tagged)
/// sends its tag exactly once: either when it completes or when it is dropped, whichever
/// happens first. Note that when a future is dropped before completion, the tag is sent
/// before the inner future itself is dropped.
///
/// Once all futures have been wrapped, call [`into_finished`](Self::into_finished) to get a
/// stream of tags of the finished futures. The stream ends after every watched future has
/// finished.
#[derive(Debug)]
pub struct TaskWatcher<T> {
    tx: mpsc::UnboundedSender<T>,
    rx: mpsc::UnboundedReceiver<T>,
}

impl<T> TaskWatcher<T> {
    /// Create a watcher with no watched futures.
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self { tx, rx }
    }

    /// Wrap `future` so that `tag` is reported when it completes or is dropped.
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

    /// Stop watching new futures and return a stream of tags of the watched futures as they
    /// finish, i.e. complete or get dropped. The stream ends once all of them have finished.
    pub fn into_finished(self) -> Finished<T> {
        Finished { rx: self.rx }
    }
}

impl<T> Default for TaskWatcher<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Stream of tags of finished watched futures, returned by [`TaskWatcher::into_finished`].
///
/// A watched future counts as finished when it either completes or is dropped. If it
/// completes, its tag is reported immediately, even if the [`Watched`] future is dropped later.
#[derive(Debug)]
pub struct Finished<T> {
    rx: mpsc::UnboundedReceiver<T>,
}

impl<T> Stream for Finished<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
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
                notifier.fire();
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
            notifier.fire();
        }
        Poll::Ready(ret)
    }
}

#[derive(Debug)]
struct Notifier<T> {
    tx: mpsc::UnboundedSender<T>,
    tag: T,
}

impl<T> Notifier<T> {
    fn fire(self) {
        _ = self.tx.send(self.tag);
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
        let mut finished = task::spawn(watcher.into_finished());
        assert_ready_eq!(finished.poll_next(), None);
    }

    #[test]
    fn test_pending_while_future_alive() {
        let watcher = TaskWatcher::new();
        let watched = watcher.watch_tagged(1, pending::<()>());
        let mut finished = task::spawn(watcher.into_finished());
        assert_pending!(finished.poll_next());

        drop(watched);
        assert!(finished.is_woken());
        assert_ready_eq!(finished.poll_next(), Some(1));
        assert_ready_eq!(finished.poll_next(), None);
    }

    #[test]
    fn test_reports_completion_before_drop() {
        let watcher = TaskWatcher::new();
        let mut watched = task::spawn(watcher.watch_tagged(1, ready(42)));
        let mut finished = task::spawn(watcher.into_finished());
        assert_pending!(finished.poll_next());

        assert_ready_eq!(watched.poll(), 42);
        assert!(finished.is_woken());
        assert_ready_eq!(finished.poll_next(), Some(1));
        // The sender is released on completion, so the stream ends without dropping `watched`.
        assert_ready_eq!(finished.poll_next(), None);
        drop(watched);
    }

    #[test]
    fn test_reports_drop_without_completion() {
        let watcher = TaskWatcher::new();
        let watched = watcher.watch_tagged(7, pending::<()>());
        drop(watched);
        let mut finished = task::spawn(watcher.into_finished());
        assert_ready_eq!(finished.poll_next(), Some(7));
        assert_ready_eq!(finished.poll_next(), None);
    }

    #[test]
    fn test_drains_all_tags_in_order() {
        let watcher = TaskWatcher::new();
        let a = watcher.watch_tagged(1, pending::<()>());
        let b = watcher.watch_tagged(2, pending::<()>());
        let c = watcher.watch_tagged(3, pending::<()>());
        let mut finished = task::spawn(watcher.into_finished());
        drop(b);
        drop(c);
        drop(a);
        for expected in [2, 3, 1] {
            assert_ready_eq!(finished.poll_next(), Some(expected));
        }
        assert_ready_eq!(finished.poll_next(), None);
    }

    #[test]
    fn test_default_tag() {
        let watcher = TaskWatcher::<u32>::default();
        drop(watcher.watch(pending::<()>()));
        let mut finished = task::spawn(watcher.into_finished());
        assert_ready_eq!(finished.poll_next(), Some(0));
        assert_ready_eq!(finished.poll_next(), None);
    }

    #[test]
    fn test_waiting_stream_is_woken_on_completion() {
        let watcher = TaskWatcher::new();
        let (tx, rx) = oneshot::channel::<()>();
        let mut watched = task::spawn(watcher.watch_tagged(5, rx));
        let mut finished = task::spawn(watcher.into_finished());

        assert_pending!(finished.poll_next());
        assert_pending!(watched.poll());

        tx.send(()).unwrap();
        assert!(watched.is_woken());
        assert_ready!(watched.poll()).unwrap();
        assert!(finished.is_woken());
        assert_ready_eq!(finished.poll_next(), Some(5));
        assert_ready_eq!(finished.poll_next(), None);
        drop(watched);
    }

    #[test]
    fn test_polled_between_send_and_sender_drop() {
        // Tries to hit the window where a watched future dropped on another thread has already
        // sent its tag but not yet dropped its sender, while the stream yields the tag and is
        // immediately polled again. That poll must be woken once the sender is dropped, instead
        // of staying pending forever.
        const ITERATIONS: usize = 10_000;
        let mut window_hits = 0;
        for _ in 0..ITERATIONS {
            let watcher = TaskWatcher::new();
            let watched = watcher.watch_tagged(1, pending::<()>());
            let mut finished = task::spawn(watcher.into_finished());
            let dropper = std::thread::spawn(move || drop(watched));

            loop {
                match finished.poll_next() {
                    Poll::Ready(tag) => {
                        assert_eq!(tag, Some(1));
                        break;
                    }
                    Poll::Pending => std::hint::spin_loop(),
                }
            }

            let first_poll = finished.poll_next();
            dropper.join().unwrap();
            match first_poll {
                Poll::Ready(tag) => assert_eq!(tag, None),
                Poll::Pending => {
                    window_hits += 1;
                    assert!(finished.is_woken(), "stream not woken after the last sender dropped");
                    assert_ready_eq!(finished.poll_next(), None);
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
            let mut finished = watcher.into_finished();
            let threads: Vec<_> = watched
                .into_iter()
                .map(|watched| std::thread::spawn(move || drop(watched)))
                .collect();
            let received: Vec<_> = rt.block_on(async {
                use futures_util::StreamExt;
                let mut received = Vec::with_capacity(TASKS);
                while let Some(tag) = finished.next().await {
                    received.push(tag);
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
