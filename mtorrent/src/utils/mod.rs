pub(crate) mod config;

/// Interface for monitoring the state of the download.
pub mod listener;

/// Startup related utility functions.
pub mod startup;

pub mod re_exports;

/// Macro for joining all tasks in a `JoinSet` with a specified timeout.
macro_rules! join_all_with_timeout {
    ($join_set:expr, $timeout:expr) => {{
        let mut join_set: tokio::task::JoinSet<_> = $join_set;
        if tokio::time::timeout($timeout, async { while join_set.join_next().await.is_some() {} })
            .await
            .is_err()
        {
            log::warn!("Failed to join all tasks in {:?}", $timeout);
            debug_assert!(false, "Failed to join all tasks in {:?}", $timeout);
        }
    }};
}
pub(crate) use join_all_with_timeout;
