[![CI](https://github.com/DanglingPointer/mtorrent/actions/workflows/ci.yml/badge.svg)](https://github.com/DanglingPointer/mtorrent/actions/workflows/ci.yml)
[![Crates.io Version](https://img.shields.io/crates/v/mtorrent)](https://crates.io/crates/mtorrent)
[![docs.rs](https://img.shields.io/docsrs/mtorrent)](https://docs.rs/mtorrent/latest)
[![codecov](https://codecov.io/github/DanglingPointer/mtorrent/graph/badge.svg?token=UA46BNVZ4T)](https://codecov.io/github/DanglingPointer/mtorrent)

# mtorrent
Lightweight Bittorrent client engine in Rust. Blazingly fast, incredibly robust and very impressive in general. This is a high-level library crate, for executables see below:
- CLI version: [`mtorrent-cli`](https://crates.io/crates/mtorrent-cli).
- GUI version: [`mtorrent-gui`](https://github.com/DanglingPointer/mtorrent-gui) or [`mtorrent-egui`](https://github.com/DanglingPointer/mtorrent-egui).

For low-level library components, see [`mtorrent-core`](https://crates.io/crates/mtorrent-core), [`mtorrent-dht`](https://crates.io/crates/mtorrent-dht) and [`mtorrent-utils`](https://crates.io/crates/mtorrent-utils).

## Installation
Download the latest pre-compiled executable for Linux or Windows here:
- CLI version: https://github.com/DanglingPointer/mtorrent/releases/latest
- GUI version: https://github.com/DanglingPointer/mtorrent-gui/releases/latest

Alternatively, compile [`mtorrent-egui`](https://github.com/DanglingPointer/mtorrent-egui) locally from source.

## Features
- Peer Wire Protocol over IPv4 and IPv6
- HTTP and UDP trackers over IPv4 and IPv6
- Peer Exchange extension
- Magnet links and metadata exchange
- DHT
