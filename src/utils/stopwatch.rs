use crate::{millisec, sec};
use std::{fmt, time::Duration};
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
        let threshold = match self.lvl {
            log::Level::Error => sec!(10),
            log::Level::Warn => sec!(1),
            log::Level::Info | log::Level::Debug => millisec!(1),
            log::Level::Trace => Duration::ZERO,
        };
        let duration = self.starttime.elapsed();
        if duration > threshold {
            log::log!(target: self.location, self.lvl, "{} finished in {:?}", self.what, duration);
        }
    }
}

#[macro_export]
macro_rules! trace_stopwatch {
    ($($arg:tt)+) => {
        $crate::utils::stopwatch::Stopwatch::new(log::Level::Trace, module_path!(), format_args!($($arg)+))
    };
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
