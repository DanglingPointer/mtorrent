use std::thread;
use tokio::{runtime, sync::oneshot};

pub mod simple {
    use super::*;

    pub struct Config {
        pub name: String,
        pub stack_size: usize,
    }

    impl Default for Config {
        fn default() -> Self {
            Self {
                name: "Unnamed".to_string(),
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
    }

    impl Default for Config {
        fn default() -> Self {
            Self {
                name: "Unnamed".to_string(),
                io_enabled: false,
                time_enabled: false,
                stack_size: 128 * 1024,
            }
        }
    }

    pub struct Handle {
        pub(super) stop_signal: Option<oneshot::Sender<()>>,
        pub(super) rt_handle: runtime::Handle,
        pub(super) _simple_handle: simple::Handle,
    }

    impl Handle {
        pub fn runtime_handle(&self) -> runtime::Handle {
            self.rt_handle.clone()
        }
    }

    impl Drop for Handle {
        fn drop(&mut self) {
            if let Some(stop_signal) = self.stop_signal.take() {
                let _ = stop_signal.send(());
            }
        }
    }
}

pub fn without_runtime<F: FnOnce() + Send + 'static>(
    config: simple::Config,
    f: F,
) -> simple::Handle {
    let name = config.name.clone();
    let th_handle = thread::Builder::new()
        .stack_size(config.stack_size)
        .name(name.clone())
        .spawn(move || {
            log::debug!("Worker '{name}' started");
            f();
            log::debug!("Worker '{name}' stopped");
        })
        .unwrap_or_else(|_| panic!("Failed to spawn thread for '{}'", config.name));

    simple::Handle {
        th_handle: Some(th_handle),
    }
}

pub fn with_runtime(config: rt::Config) -> rt::Handle {
    let mut builder = runtime::Builder::new_current_thread();
    if config.io_enabled {
        builder.enable_io();
    }
    if config.time_enabled {
        builder.enable_time();
    }
    let rt = builder
        .build()
        .unwrap_or_else(|_| panic!("Failed to build runtime '{}'", config.name));
    let rt_handle = rt.handle().clone();

    let (stop_sender, stop_receiver) = oneshot::channel::<()>();
    let simple_handle = without_runtime(From::from(config), move || {
        rt.block_on(async move {
            stop_receiver.await.unwrap();
        });
    });

    rt::Handle {
        stop_signal: Some(stop_sender),
        rt_handle,
        _simple_handle: simple_handle,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn test_rt_worker_handle_doesnt_block_forever() {
        let max_end_time = Instant::now() + Duration::from_secs(10);
        let handle = thread::spawn(move || {
            let _worker_hnd = with_runtime(Default::default());
        });
        while Instant::now() < max_end_time {
            if handle.is_finished() {
                return;
            }
        }
        panic!("Worker failed to exit within 10 sec");
    }
}
