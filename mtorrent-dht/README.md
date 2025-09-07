[![CI](https://github.com/DanglingPointer/mtorrent/actions/workflows/ci.yml/badge.svg)](https://github.com/DanglingPointer/mtorrent/actions/workflows/ci.yml)
[![Crates.io Version](https://img.shields.io/crates/v/mtorrent-dht)](https://crates.io/crates/mtorrent-dht)
[![docs.rs](https://img.shields.io/docsrs/mtorrent-dht)](https://docs.rs/mtorrent-dht/latest/mtorrent_dht/)

# mtorrent-dht

Asynchronous implementation of Kademlia-based Distributed Hash Table (DHT) for the BitTorrent protocol. Based on Tokio and used as part of the [`mtorrent`](https://crates.io/crates/mtorrent) client.

Example usage:
```rust
use mtorrent_dht as dht;

// Create the UDP socket for DHT:
let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, local_port)).await?;

// Set up the DHT stack:
let (outgoing_msgs_sink, incoming_msgs_source, io_driver) = dht::setup_udp(socket);
let (client, server, router) =
    dht::setup_queries(outgoing_msgs_sink, incoming_msgs_source, max_concurrent_queries);
let processor = dht::Processor::new(config_dir, client);

// Run the DHT system:
tokio::join!(io_driver.run(), router.run(), processor.run(server, cmd_receiver));
```