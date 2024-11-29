#[macro_export]
macro_rules! sec {
    ($arg:expr) => {{
        std::time::Duration::from_secs($arg)
    }};
}

#[macro_export]
macro_rules! millisec {
    ($arg:expr) => {{
        std::time::Duration::from_millis($arg)
    }};
}

#[macro_export]
macro_rules! min {
    ($arg:expr) => {{
        std::time::Duration::from_secs($arg * 60)
    }};
}
