[![Tests](https://github.com/DanglingPointer/mtorrent/actions/workflows/rust.yml/badge.svg)](https://github.com/DanglingPointer/mtorrent/actions/workflows/rust.yml)

# mtorrent
Simple lightweight Bittorrent client in Rust. No Tokio, no threadpool, currently uses 2 threads in total. WIP.

## Useful links
- https://wiki.theory.org/BitTorrentSpecification
- https://www.bittorrent.org/beps/bep_0000.html

## Progress
#### Required:
- [x] [Basic protocol](https://www.bittorrent.org/beps/bep_0003.html)
- [ ] [Business logic](https://wiki.theory.org/BitTorrentSpecification#Algorithms)
- [ ] HTTP engine (without thread pool / tokio)
#### Optional:
- [x] [Multitracker Metadata Extension](https://www.bittorrent.org/beps/bep_0012.html)
- [x] [UDP Tracker Protocol](https://www.bittorrent.org/beps/bep_0015.html)
- [ ] [DHT](https://www.bittorrent.org/beps/bep_0005.html)
- [ ] [Fast Extension](https://www.bittorrent.org/beps/bep_0006.html)
- [ ] [Extension Protocol](https://www.bittorrent.org/beps/bep_0010.html)
- [ ] [PEX](https://www.bittorrent.org/beps/bep_0011.html)
