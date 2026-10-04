[![CI](https://github.com/DanglingPointer/mtorrent/actions/workflows/ci.yml/badge.svg)](https://github.com/DanglingPointer/mtorrent/actions/workflows/ci.yml)
[![Crates.io Version](https://img.shields.io/crates/v/mtorrent-cli)](https://crates.io/crates/mtorrent-cli)

# mtorrent-cli
Lightweight CLI Bittorrent client in Rust. Blazingly fast, incredibly robust and very impressive in general. In addition to this CLI executable, the following GUI versions are available:
  - [`mtorrent-gui`](https://github.com/DanglingPointer/mtorrent-gui)
  - [`rill`](https://github.com/sachesi/rill) by [@sachesi](https://github.com/sachesi)
  - [`mtorrent-egui`](https://github.com/DanglingPointer/mtorrent-egui) by [@michael-eddy](https://github.com/michael-eddy)

# Installation
Download the latest pre-compiled binary for Linux or Windows here: https://github.com/DanglingPointer/mtorrent/releases/latest

Alternatively, compile locally and install using `cargo install mtorrent-cli`.

# Features
- Peer Wire Protocol over IPv4 and IPv6
- HTTP and UDP trackers over IPv4 and IPv6
- Peer Exchange extension
- Magnet links and metadata exchange
- DHT
- Selective download (downloading only some files of a multi-file torrent)

# Usage
```
$ mtorrent-cli --help
Fast and lightweight CLI BitTorrent client in Rust

Usage: mtorrent-cli [OPTIONS] <METAINFO_URI>

Arguments:
  <METAINFO_URI>  Magnet link or path to a .torrent file

Options:
  -o, --output <PATH>          Output folder
      --config-dir <PATH>      Folder to write config files to
  -p, --port <PORT>            Port for peer connections (both TCP and uTP)
  -i, --interface <INTERFACE>  Name of network interface to bind all sockets to (e.g. "eth0" or "lo")
      --no-upnp                Disable UPnP
      --no-dht                 Disable DHT
      --seed                   Keep seeding after the download is complete (until interrupted)
  -f, --files <INDICES>        Download only the files with these comma-separated 0-based indices (see --list-files). Other files created by mtorrent are deleted when the download stops
      --list-files             Print the files of a .torrent file with their indices and exit
  -h, --help                   Print help
  -V, --version                Print version
```

To download only some of the files, first list the files with their indices, then select the ones to download:
```
$ mtorrent-cli --list-files screenshots.torrent
0  412007  Screenshot from 2024-01-21 15-38-37.png
1  346505  Screenshot from 2024-02-06 16-46-22.png
2  306096  Screenshot from 2024-02-10 00-16-37.png
$ mtorrent-cli screenshots.torrent --files 1
```
Selecting files is not supported together with `--seed`. If any of the indices is invalid, mtorrent-cli exits with an error.
