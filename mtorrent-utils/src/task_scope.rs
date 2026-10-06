use std::mem;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::{runtime, task};

/// A collection of tokio task abort handles that aborts all still-running tasks on drop.
///
/// Finished handles are pruned each time a new task is spawned, so the scope does not
/// accumulate memory for completed tasks.
#[derive(Debug)]
pub struct TaskScope(Vec<task::AbortHandle>);

impl TaskScope {
    /// Create an empty scope with no tracked tasks.
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Spawn a `!Send` future on the current [`LocalRuntime`](tokio::runtime::LocalRuntime) and
    /// track its abort handle in this scope.
    pub fn spawn_local<F>(&mut self, future: F) -> task::JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        self.0.retain(|handle| !handle.is_finished());
        let join_handle = task::spawn_local(future);
        self.0.push(join_handle.abort_handle());
        join_handle
    }

    /// Spawn a future on the current tokio runtime and track its abort handle in this scope.
    pub fn spawn<F>(&mut self, future: F) -> task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.0.retain(|handle| !handle.is_finished());
        let join_handle = task::spawn(future);
        self.0.push(join_handle.abort_handle());
        join_handle
    }

    /// Spawn a future on the runtime referenced by `handle` and track its abort handle
    /// in this scope.
    pub fn spawn_on<F>(
        &mut self,
        future: F,
        handle: &runtime::Handle,
    ) -> task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.0.retain(|handle| !handle.is_finished());
        let join_handle = handle.spawn(future);
        self.0.push(join_handle.abort_handle());
        join_handle
    }

    /// Abort every tracked task and drop all handles emptying the scope.
    pub fn abort_all(&mut self) {
        for handle in mem::take(&mut self.0) {
            handle.abort();
        }
    }
}

impl Drop for TaskScope {
    fn drop(&mut self) {
        self.abort_all();
    }
}

impl Default for TaskScope {
    fn default() -> Self {
        Self::new()
    }
}

// ------------------------------------------------------------------------------------------------

/// Spawn a future on the current tokio runtime and return a handle that aborts the task on drop.
///
/// Unlike [`std::thread::scope`], the future must still be `'static`.
pub fn spawn_scoped<F>(future: F) -> ScopedTaskHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    ScopedTaskHandle(task::spawn(future))
}

/// Spawn a `!Send` future on the current [`LocalRuntime`](tokio::runtime::LocalRuntime) and
/// return a handle that aborts the task on drop.
pub fn spawn_scoped_local<F>(future: F) -> ScopedTaskHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    ScopedTaskHandle(task::spawn_local(future))
}

/// Spawn a future on the runtime referenced by `handle` and return a handle that aborts the task
/// on drop.
pub fn spawn_scoped_on<F>(future: F, handle: &runtime::Handle) -> ScopedTaskHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    ScopedTaskHandle(handle.spawn(future))
}

/// A [`JoinHandle`](task::JoinHandle) that aborts its task when dropped.
///
/// Awaiting it yields the task's output, just like awaiting a plain `JoinHandle`.
#[derive(Debug)]
pub struct ScopedTaskHandle<T>(task::JoinHandle<T>);

impl<T> Deref for ScopedTaskHandle<T> {
    type Target = task::JoinHandle<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for ScopedTaskHandle<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> Future for ScopedTaskHandle<T> {
    type Output = Result<T, task::JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

impl<T> Drop for ScopedTaskHandle<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::{Notify, mpsc};

    #[tokio::test]
    async fn test_dont_leak_finished_handles() {
        let mut scope = TaskScope::new();
        let start_signal = Arc::new(Notify::new());

        let (tx, mut rx) = mpsc::unbounded_channel();
        scope.spawn({
            let signal = start_signal.clone();
            async move {
                signal.notified().await;
                tx.send(42).unwrap();
            }
        });
        assert_eq!(scope.0.len(), 1);

        task::yield_now().await;
        let result = rx.try_recv();
        assert_eq!(result, Err(mpsc::error::TryRecvError::Empty));

        start_signal.notify_one();
        task::yield_now().await;
        let result = rx.try_recv();
        assert_eq!(result, Ok(42));

        let (tx, mut rx) = mpsc::unbounded_channel();
        scope.spawn({
            let signal = start_signal.clone();
            async move {
                signal.notified().await;
                tx.send(43).unwrap();
            }
        });
        assert_eq!(scope.0.len(), 1);

        task::yield_now().await;
        let result = rx.try_recv();
        assert_eq!(result, Err(mpsc::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn test_abort_all_on_drop() {
        let mut scope = TaskScope::new();
        let start_signal = Arc::new(Notify::new());
        let (tx, mut rx) = mpsc::unbounded_channel();

        let h1 = scope.spawn({
            let signal = start_signal.clone();
            let tx = tx.clone();
            async move {
                signal.notified().await;
                tx.send(42).unwrap();
            }
        });
        assert_eq!(scope.0.len(), 1);

        let h2 = scope.spawn({
            let signal = start_signal.clone();
            let tx = tx.clone();
            async move {
                signal.notified().await;
                tx.send(43).unwrap();
            }
        });
        assert_eq!(scope.0.len(), 2);

        task::yield_now().await;
        let result = rx.try_recv();
        assert_eq!(result, Err(mpsc::error::TryRecvError::Empty));

        drop(tx);
        drop(scope);
        start_signal.notify_one();
        task::yield_now().await;
        let result = rx.try_recv();
        assert_eq!(result, Err(mpsc::error::TryRecvError::Disconnected));
        assert!(h1.is_finished());
        assert!(h2.is_finished());
    }

    #[tokio::test]
    async fn test_scoped_handle_returns_output() {
        let mut handle = tokio_test::task::spawn(spawn_scoped(async { 42 }));
        task::yield_now().await;
        let result = tokio_test::assert_ready!(handle.poll());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_scoped_handle_aborts_on_drop() {
        let start_signal = Arc::new(Notify::new());
        let (tx, mut rx) = mpsc::unbounded_channel();

        let handle = spawn_scoped({
            let signal = start_signal.clone();
            async move {
                signal.notified().await;
                tx.send(42).unwrap();
            }
        });
        let abort_handle = handle.abort_handle();

        task::yield_now().await;
        let result = rx.try_recv();
        assert_eq!(result, Err(mpsc::error::TryRecvError::Empty));

        drop(handle);
        start_signal.notify_one();
        task::yield_now().await;
        let result = rx.try_recv();
        assert_eq!(result, Err(mpsc::error::TryRecvError::Disconnected));
        assert!(abort_handle.is_finished());
    }
}
