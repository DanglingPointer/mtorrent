use std::mem;
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
}
