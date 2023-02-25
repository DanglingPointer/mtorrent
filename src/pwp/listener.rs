use futures::channel::mpsc;
use futures::prelude::*;
use std::io;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};

pub struct ListenerRunner {
    channel: mpsc::Sender<TcpStream>,
    listener_addr: SocketAddr,
}

impl ListenerRunner {
    pub async fn run(mut self) -> io::Result<()> {
        let listener = TcpListener::bind(self.listener_addr).await?;
        loop {
            let (stream, _addr) = listener.accept().await?;
            self.channel
                .send(stream)
                .await
                .map_err(|_| io::Error::from(io::ErrorKind::Other))?;
        }
    }
}

pub struct ListenMonitor {
    channel: mpsc::Receiver<TcpStream>,
    pending_stream: Option<TcpStream>,
}

impl ListenMonitor {
    pub fn take_pending_stream(&mut self) -> Option<TcpStream> {
        self.pending_stream.take()
    }
    pub async fn handle_incoming(&mut self) {
        assert!(self.pending_stream.is_none());
        self.pending_stream = self.channel.next().await;
    }
}

pub fn listener_on_addr<A: Into<SocketAddr>>(
    addr: A,
) -> io::Result<(ListenMonitor, ListenerRunner)> {
    let (sender, receiver) = mpsc::channel::<TcpStream>(0);
    Ok((
        ListenMonitor {
            channel: receiver,
            pending_stream: None,
        },
        ListenerRunner {
            channel: sender,
            listener_addr: addr.into(),
        },
    ))
}
