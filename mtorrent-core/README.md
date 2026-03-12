[![CI](https://github.com/DanglingPointer/mtorrent/actions/workflows/ci.yml/badge.svg)](https://github.com/DanglingPointer/mtorrent/actions/workflows/ci.yml)
[![Crates.io Version](https://img.shields.io/crates/v/mtorrent-core)](https://crates.io/crates/mtorrent-core)
[![docs.rs](https://img.shields.io/docsrs/mtorrent-core)](https://docs.rs/mtorrent-core/latest)
[![codecov](https://codecov.io/github/DanglingPointer/mtorrent/graph/badge.svg?token=UA46BNVZ4T)](https://codecov.io/github/DanglingPointer/mtorrent)

# mtorrent-core

Basic types for building asynchronous Tokio-based BitTorrent clients. Some of the features:
- peer wire protocol over TCP and uTP, including extended messages
- opt-in protocol encryption (aka message stream encryption)
- tracker protocol over HTTP and UDP
- parsing of metainfo files and magnet links
- strategies for downloading and seeding pieces
- storage and filesystem operations for torrents
- management of the states of connected peers

Used as part of the [`mtorrent`](https://crates.io/crates/mtorrent) client. Example usage can be found [here](https://github.com/DanglingPointer/mtorrent/tree/7aeacb6b70e19a36ef4c1db868f3e54a0755e4a0/mtorrent/src/ops).
