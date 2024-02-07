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
        let duration = self.starttime.elapsed();
        if duration.as_micros() >= 1000 {
            log::log!(target: self.location, self.lvl, "{} finished in {:?}", self.what, duration);
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
