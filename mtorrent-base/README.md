[![CI](https://github.com/DanglingPointer/mtorrent/actions/workflows/ci.yml/badge.svg)](https://github.com/DanglingPointer/mtorrent/actions/workflows/ci.yml)
[![Crates.io Version](https://img.shields.io/crates/v/mtorrent-base)](https://crates.io/crates/mtorrent-base)
[![docs.rs](https://img.shields.io/docsrs/mtorrent-base)](https://docs.rs/mtorrent-base/latest)
[![codecov](https://codecov.io/github/DanglingPointer/mtorrent/graph/badge.svg?token=UA46BNVZ4T)](https://codecov.io/github/DanglingPointer/mtorrent)

# mtorrent-base

A collection of basic types for building asynchronous Tokio-based BitTorrent clients. Some of the features:
- peer wire protocol over TCP and uTP, using IPv4 or IPv6
- extended message protocol and Peer Exchange
- opt-in protocol encryption (aka message stream encryption)
- tracker protocol over HTTP and UDP, using IPv4 or IPv6
- parsing of metainfo files and magnet links
- rarest-first algorithm for downloading and seeding pieces
- storage and filesystem operations for reading and writing torrent data
- management of the states of connected peers

This crate is a part of the [`mtorrent`](https://crates.io/crates/mtorrent) client and examples of its usage can be found [here](https://github.com/DanglingPointer/mtorrent/tree/main/mtorrent/src/ops). Note that most of the types must be used inside a [`tokio::LocalRuntime`](https://docs.rs/tokio/latest/tokio/runtime/struct.LocalRuntime.html).

This crate was previously published as [`mtorrent-core`](https://crates.io/crates/mtorrent-core), which is now deprecated.
