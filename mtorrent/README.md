# mtorrent
Lightweight CLI Bittorrent client in Rust. Blazingly fast, incredibly robust and very impressive in general.

# Installation
Download the latest pre-compiled binary for Linux or Windows here: https://github.com/DanglingPointer/mtorrent/releases/latest

Alternatively, run `cargo install mtorrent` to compile and install locally.

# Features
- Peer Wire Protocol over IPv4 and IPv6
- HTTP and UDP trackers over IPv4 and IPv6
- Peer Exchange extension
- Magnet links and metadata exchange
- DHT

# Usage
```
$ mtorrent --help
Fast and lightweight CLI BitTorrent client in Rust

Usage: mtorrent [OPTIONS] <METAINFO_URI>

Arguments:
  <METAINFO_URI>  Magnet link or path to a .torrent file

Options:
  -o, --output <PATH>  Output folder
      --no-upnp        Disable UPnP
      --no-dht         Disable DHT
  -h, --help           Print help
  -V, --version        Print version
```