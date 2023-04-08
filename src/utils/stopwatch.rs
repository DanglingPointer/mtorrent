use std::fmt;
use tokio::time;

pub struct Stopwatch {
    lvl: log::Level,
    starttime: time::Instant,
    location: &'static str,
    what: String,
}

impl Stopwatch {
    pub fn new(lvl: log::Level, location: &'static str, args: fmt::Arguments) -> Self {
        Self {
            lvl,
            starttime: time::Instant::now(),
            location,
            what: fmt::format(args),
        }
    }
}

impl Drop for Stopwatch {
    fn drop(&mut self) {
        let duration = time::Instant::now() - self.starttime;
        if duration.as_micros() >= 1000 {
            log::log!(self.lvl, "{}: {} finished in {:?}", self.location, self.what, duration);
        }
    }
}

#[macro_export]
macro_rules! debug_stopwatch {
    ($($arg:tt)+) => {
        $crate::utils::stopwatch::Stopwatch::new(log::Level::Debug, module_path!(), format_args!($($arg)+))
    };
}

#[macro_export]
macro_rules! info_stopwatch {
    ($($arg:tt)+) => {
        $crate::utils::stopwatch::Stopwatch::new(log::Level::Info, module_path!(), format_args!($($arg)+))
    };
}

#[macro_export]
macro_rules! warn_stopwatch {
    ($($arg:tt)+) => {
        $crate::utils::stopwatch::Stopwatch::new(log::Level::Warn, module_path!(), format_args!($($arg)+))
    };
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    struct FakeLogger {
        entries: Arc<Mutex<Vec<(log::Level, String)>>>,
    }

    impl log::Log for FakeLogger {
        fn enabled(&self, _: &log::Metadata) -> bool {
            true
        }

        fn log(&self, record: &log::Record) {
            self.entries
                .lock()
                .unwrap()
                .push((record.metadata().level(), fmt::format(*record.args())));
        }

        fn flush(&self) {}
    }

    #[tokio::test(start_paused = true)]
    async fn test_stopwatch() {
        let entries = Arc::new(Mutex::new(Vec::new()));
        let logger = FakeLogger {
            entries: entries.clone(),
        };
        log::set_boxed_logger(Box::new(logger))
            .map(|()| log::set_max_level(log::LevelFilter::Trace))
            .unwrap();

        {
            let sw = debug_stopwatch!("Task with nr {}", 42);
            time::sleep(Duration::from_micros(1000)).await;
            drop(sw);

            let logged_data = entries.lock().unwrap();
            assert_eq!(1, logged_data.len());
            assert_eq!(
                (
                    log::Level::Debug,
                    "mtorrent::utils::stopwatch::test: Task with nr 42 finished in 1ms".to_string()
                ),
                logged_data[0]
            );
        }

        {
            entries.lock().unwrap().clear();
            let sw = info_stopwatch!("Info task nr {}", 42);
            time::sleep(Duration::from_secs(42)).await;
            drop(sw);

            let logged_data = entries.lock().unwrap();
            assert_eq!(1, logged_data.len());
            assert_eq!(
                (
                    log::Level::Info,
                    "mtorrent::utils::stopwatch::test: Info task nr 42 finished in 42s".to_string()
                ),
                logged_data[0]
            );
        }

        {
            entries.lock().unwrap().clear();
            let sw = warn_stopwatch!("Warn task nr {}", 42);
            time::sleep(Duration::from_secs(300)).await;
            drop(sw);

            let logged_data = entries.lock().unwrap();
            assert_eq!(1, logged_data.len());
            assert_eq!(
                (
                    log::Level::Warn,
                    "mtorrent::utils::stopwatch::test: Warn task nr 42 finished in 300s"
                        .to_string()
                ),
                logged_data[0]
            );
        }
    }
}
