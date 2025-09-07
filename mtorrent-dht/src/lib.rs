//! Complete implementation of Kademlia-based Distributed Hash Table (DHT) used in the BitTorrent protocol.
//!
//! Example usage:
//! ```no_run
//! use mtorrent_dht as dht;
//! # use tokio::net::UdpSocket;
//! # use std::net::*;
//!
//! # let local_port = 6881;
//! # let max_concurrent_queries = Some(100);
//! # let config_dir = std::env::current_dir().unwrap();
//! let (cmd_sender, cmd_receiver) = dht::setup_commands();
//!
//! # tokio::task::spawn_local(async move {
//! // Create the UDP socket for DHT:
//! let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, local_port)).await.unwrap();
//!
//! // Set up the DHT stack that consists of 3 layers:
//! let (outgoing_msgs_sink, incoming_msgs_source, io_driver) = dht::setup_udp(socket);
//! let (client, server, router) =
//!     dht::setup_queries(outgoing_msgs_sink, incoming_msgs_source, max_concurrent_queries);
//! let processor = dht::Processor::new(config_dir, client);
//!
//! // Run the DHT system:
//! tokio::join!(io_driver.run(), router.run(), processor.run(server, cmd_receiver));
//! # });
//!
//! // now send commands to the `cmd_sender` from a different task or runtime
//!
//! ```

mod cmds;
mod config;
mod error;
mod kademlia;
mod msgs;
mod peers;
mod processor;
mod queries;
mod tasks;
mod u160;
mod udp;

pub use cmds::*;
pub use processor::Processor;
pub use queries::*;
pub use udp::*;
