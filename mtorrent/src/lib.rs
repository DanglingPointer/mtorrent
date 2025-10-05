//! Complete implementation of BitTorrent client functionality including
//! torrent file handling, peer management, piece selection, data transfer,
//! and integration with a Kademlia-based Distributed Hash Table (DHT).
//!
//! Example usage:
//! - CLI application: [`mtorrent-cli`](https://github.com/DanglingPointer/mtorrent/blob/master/mtorrent-cli/)
//! - GUI application: [`mtorrent-gui`](https://github.com/DanglingPointer/mtorrent-gui/)

/// Application modules.
/// These modules contain entry points and high-level logic for the BitTorrent client.
pub mod app;

/// Utility functions and types.
pub mod utils;

pub(crate) mod ops;
