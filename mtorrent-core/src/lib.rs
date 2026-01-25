//! Basic types for building asynchronous Tokio-based BitTorrent clients.

/// Utilities for managing data.
pub mod data;

/// Processing of the initial input.
pub mod input;

/// Fundamentals for the peer wire protocol. Example usage can be found [here](https://github.com/DanglingPointer/mtorrent/tree/7aeacb6b70e19a36ef4c1db868f3e54a0755e4a0/mtorrent/src/ops/peer).
pub mod pwp;

/// Implementation of the tracker protocol. Example usage can be found [here](https://github.com/DanglingPointer/mtorrent/blob/7aeacb6b70e19a36ef4c1db868f3e54a0755e4a0/mtorrent/src/ops/announces.rs).
pub mod trackers;

/// Micro Transport Protocol (uTP) implementation.
pub mod utp;
