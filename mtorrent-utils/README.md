[![CI](https://github.com/DanglingPointer/mtorrent/actions/workflows/ci.yml/badge.svg)](https://github.com/DanglingPointer/mtorrent/actions/workflows/ci.yml)
[![Crates.io Version](https://img.shields.io/crates/v/mtorrent-utils)](https://crates.io/crates/mtorrent-utils)
[![docs.rs](https://img.shields.io/docsrs/mtorrent-utils)](https://docs.rs/mtorrent-utils/latest)
[![codecov](https://codecov.io/github/DanglingPointer/mtorrent/graph/badge.svg?token=UA46BNVZ4T)](https://codecov.io/github/DanglingPointer/mtorrent)

# mtorrent-utils

Collection of miscellaneous utilities used by the [`mtorrent`](https://crates.io/crates/mtorrent) crate and its components. Some of the utilities are specific to the BitTorrent protocol, while others are generic and can be used in any Tokio-based application.

## Utilities

### BitTorrent and networking

- [`benc`](https://docs.rs/mtorrent-utils/latest/mtorrent_utils/benc/) parses and serializes bencoded data.
- [`peer_id`](https://docs.rs/mtorrent-utils/latest/mtorrent_utils/peer_id/) generates and represents BitTorrent peer IDs.
- [`net`](https://docs.rs/mtorrent-utils/latest/mtorrent_utils/net/) provides local-address discovery, socket configuration, deterministic dynamic ports, and compact peer-address decoding.
- [`split_stream`](https://docs.rs/mtorrent-utils/latest/mtorrent_utils/split_stream/) abstracts splitting bidirectional streams into read and write halves.
- [`upnp`](https://docs.rs/mtorrent-utils/latest/mtorrent_utils/upnp/) maintains UPnP/IGD port mappings in a background task.

### Async tasks and runtimes

- [`loop_select`](https://docs.rs/mtorrent-utils/latest/mtorrent_utils/loop_select/) cooperatively polls several operations sharing mutable state.
- [`select_next`](https://docs.rs/mtorrent-utils/latest/mtorrent_utils/select_next/) waits for the next item from any stream in a keyed collection.
- [`task_scope`](https://docs.rs/mtorrent-utils/latest/mtorrent_utils/task_scope/) aborts spawned Tokio tasks when their scope or handle is dropped.
- [`task_watcher`](https://docs.rs/mtorrent-utils/latest/mtorrent_utils/task_watcher/) reports whether watched futures completed or were dropped.
- [`worker`](https://docs.rs/mtorrent-utils/latest/mtorrent_utils/worker/) runs closures or single-threaded Tokio runtimes on dedicated threads.

### General utilities

- [`bandwidth`](https://docs.rs/mtorrent-utils/latest/mtorrent_utils/bandwidth/) measures average bitrate.
- [`fifo_set`](https://docs.rs/mtorrent-utils/latest/mtorrent_utils/fifo_set/) provides insertion-ordered sets with optional bounded capacity.
- [`connect_recorder`](https://docs.rs/mtorrent-utils/latest/mtorrent_utils/connect_recorder/) tracks connected and recently seen peers.
