[![Tests](https://github.com/DanglingPointer/mtorrent/actions/workflows/rust.yml/badge.svg)](https://github.com/DanglingPointer/mtorrent/actions/workflows/rust.yml)

# mtorrent
Simple lightweight Bittorrent client in Rust. Uses Tokio, no threadpool, currently uses 3 threads in total. WIP.

## Useful links
- https://wiki.theory.org/BitTorrentSpecification
- https://www.bittorrent.org/beps/bep_0000.html

## Progress
#### Required:
- [x] [Basic protocol](https://www.bittorrent.org/beps/bep_0003.html)
- [ ] [Business logic](https://wiki.theory.org/BitTorrentSpecification#Algorithms)
- [x] HTTP tracker client
#### Optional:
- [x] [Multitracker Metadata Extension](https://www.bittorrent.org/beps/bep_0012.html)
- [x] [UDP Tracker Protocol](https://www.bittorrent.org/beps/bep_0015.html)
- [ ] [DHT](https://www.bittorrent.org/beps/bep_0005.html)
- [ ] [Fast Extension](https://www.bittorrent.org/beps/bep_0006.html)
- [ ] [Extension Protocol](https://www.bittorrent.org/beps/bep_0010.html)
- [ ] [PEX](https://www.bittorrent.org/beps/bep_0011.html)

## Building

Requires Docker. To build run `./builder.sh [args]` where `args` is the arguments to `cargo`. When using VS Code, open the root folder, then, when prompted, build and open the dev container (requires Dev Containers extension).
