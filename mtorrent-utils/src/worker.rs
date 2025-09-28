use std::sync::Arc;
use std::{io, thread};
use tokio::runtime;
use tokio::sync::{Notify, oneshot};

pub mod simple {
    use super::*;

    pub struct Config {
        pub name: String,
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

    pub struct Handle {
        pub(super) th_handle: Option<thread::JoinHandle<()>>,
    }

    impl Drop for Handle {
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
        pub name: String,
        pub io_enabled: bool,
        pub time_enabled: bool,
        pub stack_size: usize,
        pub max_blocking_threads: usize,
    }

    impl Default for Config {
        fn default() -> Self {
            Self {
                name: "mtorrent-worker".to_owned(),
                io_enabled: false,
                time_enabled: false,
                stack_size: 128 * 1024,
                max_blocking_threads: 1,
            }
        }
    }

    pub struct Handle {
        pub(super) stop_signal: Arc<Notify>,
        pub(super) rt_handle: runtime::Handle,
        pub(super) _simple_handle: simple::Handle,
    }

    impl Handle {
        pub fn runtime_handle(&self) -> &runtime::Handle {
            &self.rt_handle
        }
    }

    impl Drop for Handle {
        fn drop(&mut self) {
            self.stop_signal.notify_one();
        }
    }
}

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

    let stop_signal = Arc::new(Notify::const_new());
    let stop_notified = stop_signal.clone().notified_owned();

    let simple_handle = without_runtime(From::from(config), move || {
        rt.block_on(stop_notified);
    })?;

    Ok(rt::Handle {
        stop_signal,
        rt_handle,
        _simple_handle: simple_handle,
    })
}

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
    let (handle_sender, handle_receiver) = oneshot::channel::<runtime::Handle>();
    let stop_signal = Arc::new(Notify::const_new());
    let stop_notified = stop_signal.clone().notified_owned();

    let simple_handle = without_runtime(From::from(config), move || {
        builder
            .build_local(Default::default())
            .unwrap_or_else(|_| panic!("Failed to build runtime '{name}'"))
            .block_on(async move {
                let handle = runtime::Handle::current();
                handle_sender.send(handle).unwrap_or_else(|_| unreachable!());
                stop_notified.await;
            });
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
        let max_end_time = Instant::now() + sec!(10);
        let handle = thread::spawn(move || {
            let _worker_hnd = with_runtime(Default::default()).expect("build worker");
        });
        while Instant::now() < max_end_time {
            if handle.is_finished() {
                handle.join().expect("thread panicked");
                return;
            }
        }
        panic!("Worker failed to exit within 10 sec");
    }

    #[test]
    fn test_local_rt_worker_handle_doesnt_block_forever() {
        let max_end_time = Instant::now() + sec!(10);
        let handle = thread::spawn(move || {
            let _worker_hnd = with_local_runtime(Default::default()).expect("build worker");
        });
        while Instant::now() < max_end_time {
            if handle.is_finished() {
                handle.join().expect("thread panicked");
                return;
            }
        }
        panic!("Worker failed to exit within 10 sec");
    }
}
