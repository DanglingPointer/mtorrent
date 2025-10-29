use std::sync::Arc;
use std::time::Duration;
use std::{io, thread};
use tokio::runtime;
use tokio::sync::Notify;
#[cfg(tokio_unstable)]
use tokio::sync::oneshot;

pub mod simple {
    use super::*;

    pub struct Config {
        /// Name of the worker thread.
        pub name: String,
        /// Stack size of the worker thread.
        pub stack_size: usize,
    }

    impl Default for Config {
        fn default() -> Self {
            Self {
                name: "mtorrent-worker".to_owned(),
                stack_size: 128 * 1024,
            }
        }
    }

    impl From<rt::Config> for Config {
        fn from(rt_config: rt::Config) -> Self {
            Config {
                name: rt_config.name,
                stack_size: rt_config.stack_size,
            }
        }
    }

    /// Handle to a worker thread.
    /// Dropping the handle will block until the thread exits.
    pub struct Handle {
        pub(super) th_handle: Option<thread::JoinHandle<()>>,
    }

    impl Drop for Handle {
        /// Blocks until the worker thread exits.
        fn drop(&mut self) {
            if let Some(th_handle) = self.th_handle.take() {
                let _ = th_handle.join();
            }
        }
    }
}

pub mod rt {
    use super::*;

    pub struct Config {
        /// Name of the worker thread.
        pub name: String,
        /// Stack size of the worker thread.
        pub stack_size: usize,
        /// See [`tokio::runtime::Builder::enable_io`].
        pub io_enabled: bool,
        /// See [`tokio::runtime::Builder::enable_time`].
        pub time_enabled: bool,
        /// See [`tokio::runtime::Builder::max_blocking_threads`].
        pub max_blocking_threads: usize,
        /// See [`tokio::runtime::Runtime::shutdown_timeout`].
        pub shutdown_timeout: Duration,
    }

    impl Default for Config {
        fn default() -> Self {
            Self {
                name: "mtorrent-worker".to_owned(),
                io_enabled: false,
                time_enabled: false,
                stack_size: 128 * 1024,
                max_blocking_threads: 1,
                shutdown_timeout: Duration::ZERO,
            }
        }
    }

    /// Handle to a worker thread running a Tokio runtime.
    /// Dropping the handle will signal the runtime to shut down, then block until the thread exits.
    pub struct Handle {
        pub(super) stop_signal: Arc<Notify>,
        pub(super) rt_handle: runtime::Handle,
        pub(super) _simple_handle: simple::Handle,
    }

    impl Handle {
        /// Get a reference to the Tokio runtime handle.
        pub fn runtime_handle(&self) -> &runtime::Handle {
            &self.rt_handle
        }
    }

    impl Drop for Handle {
        /// Sends shutdown signal to the Tokio runtime, then blocks until the worker thread exits.
        fn drop(&mut self) {
            self.stop_signal.notify_one();
        }
    }
}

/// Create a worker thread running the provided closure.
pub fn without_runtime<F: FnOnce() + Send + 'static>(
    config: simple::Config,
    f: F,
) -> io::Result<simple::Handle> {
    let name = config.name;
    let th_handle = thread::Builder::new().stack_size(config.stack_size).name(name.clone()).spawn(
        move || {
            log::debug!("Worker '{name}' started");
            f();
            log::debug!("Worker '{name}' stopped");
        },
    )?;

    Ok(simple::Handle {
        th_handle: Some(th_handle),
    })
}

/// Create a worker thread running a [`tokio::runtime::Runtime`](https://docs.rs/tokio/latest/tokio/runtime/struct.Runtime.html) with the specified configuration.
pub fn with_runtime(config: rt::Config) -> io::Result<rt::Handle> {
    let mut builder = runtime::Builder::new_current_thread();
    if config.io_enabled {
        builder.enable_io();
    }
    if config.time_enabled {
        builder.enable_time();
    }
    let rt = builder.max_blocking_threads(config.max_blocking_threads).build()?;
    let rt_handle = rt.handle().clone();

    let shutdown_timeout = config.shutdown_timeout;
    let stop_signal = Arc::new(Notify::const_new());
    let stop_notified = stop_signal.clone().notified_owned();

    let simple_handle = without_runtime(From::from(config), move || {
        rt.block_on(stop_notified);
        rt.shutdown_timeout(shutdown_timeout);
    })?;

    Ok(rt::Handle {
        stop_signal,
        rt_handle,
        _simple_handle: simple_handle,
    })
}

/// Create a worker thread running a [`tokio::runtime::LocalRuntime`](https://docs.rs/tokio/latest/tokio/runtime/struct.LocalRuntime.html) with the specified configuration.
#[cfg(tokio_unstable)]
#[cfg_attr(docsrs, doc(cfg(tokio_unstable)))]
pub fn with_local_runtime(config: rt::Config) -> io::Result<rt::Handle> {
    let mut builder = runtime::Builder::new_current_thread();
    builder.max_blocking_threads(config.max_blocking_threads);
    if config.io_enabled {
        builder.enable_io();
    }
    if config.time_enabled {
        builder.enable_time();
    }

    let name = config.name.clone();
    let shutdown_timeout = config.shutdown_timeout;

    let (handle_sender, handle_receiver) = oneshot::channel::<runtime::Handle>();
    let stop_signal = Arc::new(Notify::const_new());
    let stop_notified = stop_signal.clone().notified_owned();

    let simple_handle = without_runtime(From::from(config), move || {
        let rt = builder
            .build_local(Default::default())
            .unwrap_or_else(|_| panic!("Failed to build runtime '{name}'"));

        rt.block_on(async move {
            let handle = runtime::Handle::current();
            handle_sender.send(handle).unwrap_or_else(|_| unreachable!());
            stop_notified.await;
        });
        rt.shutdown_timeout(shutdown_timeout);
    })?;

    let rt_handle = handle_receiver
        .blocking_recv()
        .map_err(|_| io::Error::other("failed to build runtime"))?;

    Ok(rt::Handle {
        stop_signal,
        rt_handle,
        _simple_handle: simple_handle,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use local_async_utils::prelude::*;
    use std::time::Instant;

    #[test]
    fn test_rt_worker_handle_doesnt_block_forever() {
        let max_end_time = Instant::now() + sec!(5);
        let handle = thread::spawn(move || {
            let _worker_hnd = with_runtime(Default::default()).expect("build worker");
        });
        while Instant::now() < max_end_time {
            if handle.is_finished() {
                handle.join().expect("DON'T PANIC");
                return;
            }
        }
        panic!("Worker failed to exit within 10 sec");
    }

    #[test]
    fn test_rt_worker_doesnt_wait_for_blocking_tasks() {
        let max_end_time = Instant::now() + sec!(5);
        let handle = thread::spawn(move || {
            let worker = with_runtime(Default::default()).expect("build worker");
            worker.runtime_handle().spawn_blocking(|| std::thread::sleep(min!(1)));
        });

        while Instant::now() < max_end_time {
            if handle.is_finished() {
                handle.join().expect("DON'T PANIC");
                return;
            }
        }
        panic!("Worker failed to exit within 10 sec");
    }

    #[test]
    fn test_local_rt_worker_handle_doesnt_block_forever() {
        let max_end_time = Instant::now() + sec!(5);
        let handle = thread::spawn(move || {
            let _worker_hnd = with_local_runtime(Default::default()).expect("build worker");
        });
        while Instant::now() < max_end_time {
            if handle.is_finished() {
                handle.join().expect("DON'T PANIC");
                return;
            }
        }
        panic!("Worker failed to exit within 10 sec");
    }

    #[test]
    fn test_local_rt_worker_doesnt_wait_for_blocking_tasks() {
        let max_end_time = Instant::now() + sec!(5);
        let handle = thread::spawn(move || {
            let worker = with_local_runtime(Default::default()).expect("build worker");
            worker.runtime_handle().spawn_blocking(|| std::thread::sleep(min!(1)));
        });

        while Instant::now() < max_end_time {
            if handle.is_finished() {
                handle.join().expect("DON'T PANIC");
                return;
            }
        }
        panic!("Worker failed to exit within 10 sec");
    }
}
