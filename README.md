[![Build and test](https://github.com/DanglingPointer/mtorrent/actions/workflows/ci.yml/badge.svg)](https://github.com/DanglingPointer/mtorrent/actions/workflows/ci.yml)

# mtorrent
CLI Bittorrent client in Rust. Based on Tokio (without a threadpool, currently runs 3 threads in total). Blazingly fast, incredibly robust and very impressive in general.

## Useful links
- https://wiki.theory.org/BitTorrentSpecification
- https://www.bittorrent.org/beps/bep_0000.html
- https://codethechange.stanford.edu/guides/guide_kademlia.html (Kademlia)

## Progress
#### Required:
- [x] [Basic protocol](https://www.bittorrent.org/beps/bep_0003.html)
- [x] [Business logic](https://wiki.theory.org/BitTorrentSpecification#Algorithms)
- [x] HTTP tracker client
#### Optional:
- [x] [Multitracker Metadata Extension](https://www.bittorrent.org/beps/bep_0012.html)
- [x] [UDP Tracker Protocol](https://www.bittorrent.org/beps/bep_0015.html)
- [x] [Extension Protocol](https://www.bittorrent.org/beps/bep_0010.html)
- [x] [PEX](https://www.bittorrent.org/beps/bep_0011.html)
- [x] [Magnet URIs](http://www.bittorrent.org/beps/bep_0009.html)
- [x] [IPv6 Tracker Extension](https://www.bittorrent.org/beps/bep_0007.html)
- [ ] [DHT](https://www.bittorrent.org/beps/bep_0005.html) ([Kademlia](https://www.scs.stanford.edu/~dm/home/papers/kpos.pdf))
- [ ] [IPv6 DHT](https://www.bittorrent.org/beps/bep_0032.html)
- [ ] [STUN (for DHT)?](https://datatracker.ietf.org/doc/html/rfc8489)
- [ ] [Fast Extension](https://www.bittorrent.org/beps/bep_0006.html)
- [ ] [BitTorrent v2](http://bittorrent.org/beps/bep_0052.html)

## Building

Build on Linux using the provided dev container. Install Docker, then run `./cargo.sh [args]` where `args` is the arguments to `cargo` (e.g. `./cargo.sh build --release`).

Alternatively, use `./devshell.sh` to execute arbitrary commands inside the dev container.

When using VS Code, open the root folder, then, when prompted, build and open the dev container (requires Dev Containers extension).
