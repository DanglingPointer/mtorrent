[![CI](https://github.com/DanglingPointer/mtorrent/actions/workflows/ci.yml/badge.svg)](https://github.com/DanglingPointer/mtorrent/actions/workflows/ci.yml)
[![Crates.io Version](https://img.shields.io/crates/v/mtorrent-cli)](https://crates.io/crates/mtorrent-cli)

# mtorrent-cli
Lightweight CLI Bittorrent client in Rust. Blazingly fast, incredibly robust and very impressive in general. For GUI version see [`mtorrent-gui`](https://github.com/DanglingPointer/mtorrent-gui).

# Installation
Download the latest pre-compiled binary for Linux or Windows here: https://github.com/DanglingPointer/mtorrent/releases/latest

Alternatively, compile locally using the following commands:
- Linux: `RUSTFLAGS="--cfg=tokio_unstable" cargo install mtorrent-cli`
- Windows: `$env:RUSTFLAGS="--cfg=tokio_unstable"; cargo install mtorrent-cli`

# Features
- Peer Wire Protocol over IPv4 and IPv6
- HTTP and UDP trackers over IPv4 and IPv6
- Peer Exchange extension
- Magnet links and metadata exchange
- DHT

# Usage
```
$ mtorrent-cli --help
Fast and lightweight CLI BitTorrent client in Rust

Usage: mtorrent-cli [OPTIONS] <METAINFO_URI>

Arguments:
  <METAINFO_URI>  Magnet link or path to a .torrent file

Options:
  -o, --output <PATH>  Output folder
      --no-upnp        Disable UPnP
      --no-dht         Disable DHT
  -h, --help           Print help
  -V, --version        Print version
```