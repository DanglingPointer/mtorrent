use async_io::Async;
use futures::channel::mpsc;
use futures::pin_mut;
use futures::prelude::*;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};

pub struct ListenerRunner {
    channel: mpsc::Sender<Async<TcpStream>>,
    listener: Async<TcpListener>,
}

impl ListenerRunner {
    pub async fn run(mut self) -> io::Result<()> {
        let incoming = self.listener.incoming();
        pin_mut!(incoming);

        while let Some(stream) = incoming.next().await {
            let stream = stream?;
            self.channel
                .send(stream)
                .await
                .map_err(|_| io::Error::from(io::ErrorKind::Other))?;
        }
        unreachable!()
    }
}

pub struct ListenMonitor {
    channel: mpsc::Receiver<Async<TcpStream>>,
    pending_stream: Option<Async<TcpStream>>,
}

impl ListenMonitor {
    pub fn take_pending_stream(&mut self) -> Option<Async<TcpStream>> {
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
    let listener = Async::<TcpListener>::bind(addr)?;
    let (sender, receiver) = mpsc::channel::<Async<TcpStream>>(0);
    Ok((
        ListenMonitor {
            channel: receiver,
            pending_stream: None,
        },
        ListenerRunner {
            channel: sender,
            listener,
        },
    ))
}
