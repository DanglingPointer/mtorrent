use super::u160::U160;
use futures::Stream;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

pub enum Command {
    AddNode {
        addr: SocketAddr,
    },
    FindPeers {
        info_hash: U160,
        callback: mpsc::Sender<SocketAddr>,
    },
}

pub struct Server(mpsc::Receiver<Command>);

impl Stream for Server {
    type Item = Command;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

pub fn setup_cmds() -> (mpsc::Sender<Command>, Server) {
    let (sender, receiver) = mpsc::channel(512);
    (sender, Server(receiver))
}