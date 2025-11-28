[![CI](https://github.com/DanglingPointer/mtorrent/actions/workflows/ci.yml/badge.svg)](https://github.com/DanglingPointer/mtorrent/actions/workflows/ci.yml)
[![Crates.io Version](https://img.shields.io/crates/v/mtorrent-core)](https://crates.io/crates/mtorrent-core)
[![docs.rs](https://img.shields.io/docsrs/mtorrent-core)](https://docs.rs/mtorrent-core/latest)
[![codecov](https://codecov.io/github/DanglingPointer/mtorrent/graph/badge.svg?token=UA46BNVZ4T)](https://codecov.io/github/DanglingPointer/mtorrent)

# mtorrent-core

Basic types for building asynchronous Tokio-based BitTorrent clients. Contains following:
- full implementation of the peer wire protocol
- HTTP and UDP tracker protocols
- parsing of metainfo files and magnet links
- managing download and upload of data
- filesystem operations for downloaded data
- keeping track of connected peers and their states

Used as part of the [`mtorrent`](https://crates.io/crates/mtorrent) client. Example usage can be found [here](https://github.com/DanglingPointer/mtorrent/tree/7aeacb6b70e19a36ef4c1db868f3e54a0755e4a0/mtorrent/src/ops).
