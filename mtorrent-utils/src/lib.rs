#![cfg_attr(docsrs, feature(doc_cfg))]
//! Collection of miscellaneous utilities used by the [`mtorrent`](https://crates.io/crates/mtorrent)
//! crate and its components. Some utilities are specific to the BitTorrent protocol, while others
//! are generic and can be used in any Tokio-based application.
//!
//! # Utilities
//!
//! ## BitTorrent and networking
//!
//! - [`benc`] parses and serializes bencoded data.
//! - [`peer_id`] generates and represents BitTorrent peer IDs.
//! - [`net`] provides local-address discovery, socket configuration, deterministic dynamic ports,
//!   and compact peer-address decoding.
//! - [`split_stream`] abstracts splitting bidirectional streams into read and write halves.
//! - [`upnp`] maintains UPnP/IGD port mappings in a background task.
//!
//! ## Async tasks and runtimes
//!
//! - [`loop_select`] cooperatively polls several operations sharing mutable state.
//! - [`select_next`] waits for the next item from any stream in a keyed collection.
//! - [`task_scope`] aborts spawned Tokio tasks when their scope or handle is dropped.
//! - [`task_watcher`] reports whether watched futures completed or were dropped.
//! - [`worker`] runs closures or single-threaded Tokio runtimes on dedicated threads.
//!
//! ## General utilities
//!
//! - [`bandwidth`] measures average bitrate.
//! - [`fifo_set`] provides insertion-ordered sets with optional bounded capacity.
//! - [`connect_recorder`] tracks connected and recently seen peers.

/// Bitrate measurement utilities.
pub mod bandwidth;

/// Bencoding parser and serializer.
pub mod benc;

/// FIFO set with optional bounded capacity and deduplication.
pub mod fifo_set;

/// Local IP address discovery and socket helpers.
pub mod net;

/// Single-value watch channel for `!Send` types.
#[doc(hidden)]
pub mod local_watch;

/// Cooperative poll-loop multiplexing multiple futures.
pub mod loop_select;

/// BitTorrent peer ID generation and wrapper.
pub mod peer_id;

/// Trait for splitting a bidirectional stream into read/write halves.
pub mod split_stream;

mod stopwatch;

/// UPnP/IGD port forwarding helpers.
pub mod upnp;

/// Dedicated single-threaded Tokio worker thread.
pub mod worker;

/// Connect recorder for tracking connected and known peers.
pub mod connect_recorder;

/// Scoped task spawning that aborts outstanding tasks when the scope is dropped.
pub mod task_scope;

/// Wait for the next item from any stream in a keyed collection of streams.
pub mod select_next;

/// Get notified when futures complete or are dropped.
pub mod task_watcher;
