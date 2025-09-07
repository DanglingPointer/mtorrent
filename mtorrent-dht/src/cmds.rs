use derive_more::Debug;
use futures_util::Stream;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

/// Command from the user to the DHT system.
#[derive(Debug)]
pub enum Command {
    /// Attempt to connect to a new node and insert it into the routing table.
    AddNode {
        addr: SocketAddr,
    },
    /// Commence search for peers for the given torrent.
    FindPeers {
        /// Info hash of the torrent.
        info_hash: [u8; 20],
        /// Channel for sending back addresses of the discovered peers.
        #[debug(skip)]
        callback: mpsc::Sender<SocketAddr>,
        /// Peer wire protocol port of the local peer.
        local_peer_port: u16,
    },
}

/// Command receiver used internally by the DHT code.
pub struct CommandSource(mpsc::Receiver<Command>);

impl Stream for CommandSource {
    type Item = Command;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

/// Sender of commands to the DHT system.
pub type CommandSink = mpsc::Sender<Command>;

/// Set up a channel for the user commands to the DHT system.
pub fn setup_commands() -> (CommandSink, CommandSource) {
    let (sender, receiver) = mpsc::channel(1);
    (sender, CommandSource(receiver))
}
