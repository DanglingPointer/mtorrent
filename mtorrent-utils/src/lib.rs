#![cfg_attr(docsrs, feature(doc_cfg))]
//! Collection of miscellaneous utilities used by the [`mtorrent`](https://crates.io/crates/mtorrent) crate and its components. Some of the utilities are specific to the BitTorrent protocol, while others are generic and can be used in any Tokio-based application.

/// Bitrate measurement utilities.
pub mod bandwidth;

/// Bencoding parser and serializer.
pub mod benc;

/// Rate-limited connection permits for peer addresses.
pub mod connect_throttle;

/// FIFO set with optional bounded capacity and deduplication.
pub mod fifo_set;

/// Local IP address discovery and socket helpers.
pub mod ip;

/// Single-value watch channel for `!Send` types.
pub mod local_watch;

/// Cooperative poll-loop multiplexing multiple futures.
pub mod loop_select;

/// BitTorrent peer ID generation and wrapper.
pub mod peer_id;

/// Trait for splitting a bidirectional stream into read/write halves.
pub mod split_stream;

/// Logging stopwatch macros at various log levels.
pub mod stopwatch;

/// UPnP/IGD port forwarding helpers.
pub mod upnp;

/// Dedicated single-threaded Tokio worker thread.
pub mod worker;
