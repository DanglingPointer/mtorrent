use crate::peers::handshake::{do_handshake_incoming, do_handshake_outgoing, Handshake};
use crate::peers::peer_message::PeerMessage;
use async_channel;
use async_io::Async;
use futures::executor::LocalSpawner;
use futures::io::BufWriter;
use futures::prelude::*;
use futures::select;
use futures::task::LocalSpawnExt;
use log::{error, info};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::{Ipv4Addr, Shutdown, SocketAddr, SocketAddrV4, TcpListener, TcpStream};

mod handshake;
pub mod peer_message;

pub enum EventToPeers {
    CreateOutboundConnection {
        remote_addr: SocketAddr,
        remote_peer_id: Option<[u8; 20]>,
        info_hash: [u8; 20],
    },
    SendMessage {
        remote_peer_id: [u8; 20],
        message: PeerMessage,
    },
    CloseConnection {
        remote_peer_id: [u8; 20],
    },
    Stop,
}

pub enum EventFromPeers {
    NewConnection {
        remote_handshake: Handshake,
    },
    MessageReceived {
        remote_peer_id: [u8; 20],
        message: PeerMessage,
    },
    ConnectionClosed {
        remote_peer_id: [u8; 20],
        error: Option<io::Error>,
    },
}

pub struct ConnectionManager {
    local_peer_id: [u8; 20],
    listener: Async<TcpListener>,
    executor: LocalSpawner,
    event_sender: async_channel::Sender<EventFromPeers>,
    event_receiver: async_channel::Receiver<EventToPeers>,
    writers: RefCell<HashMap<[u8; 20], async_channel::Sender<EventToPeers>>>,
}

impl ConnectionManager {
    pub fn new(
        local_peer_id: [u8; 20],
        listener_port: u16,
        executor: LocalSpawner,
        event_sender: async_channel::Sender<EventFromPeers>,
        event_receiver: async_channel::Receiver<EventToPeers>,
    ) -> Self {
        let server_addr = SocketAddr::from(SocketAddrV4::new(Ipv4Addr::LOCALHOST, listener_port));
        let listener = Async::<TcpListener>::bind(server_addr).unwrap();

        ConnectionManager {
            local_peer_id,
            listener,
            executor,
            event_sender,
            event_receiver,
            writers: RefCell::new(HashMap::new()),
        }
    }

    pub async fn run(self) {
        info!("ConnectionManager starting");
        select! {
            _ = self.accept_connections().fuse() => info!("accept_connections() stopped"),
            _ = self.serve_connections().fuse() => info!("serve_connections() stopped"),
        };
        info!("ConnectionManager exiting");
    }

    async fn accept_connections(&self) {
        loop {
            match self.listener.accept().await {
                Err(error) => {
                    error!("accept_connections exiting - listener error: {}", error);
                    return;
                }
                Ok((socket, _remote_addr)) => match self.create_inbound_connection(socket).await {
                    Err(error) => error!("Error creating inbound connection: {}", error),
                    Ok(event) => {
                        if let Err(e) = self.event_sender.try_send(event) {
                            info!("accept_connections exiting - no event channel: {}", e);
                            return;
                        }
                    }
                },
            }
        }
    }

    async fn create_inbound_connection(
        &self,
        socket: Async<TcpStream>,
    ) -> io::Result<EventFromPeers> {
        let (socket, remote_info) =
            do_handshake_incoming(socket, &self.local_peer_id[..], None).await?;
        add_new_connection(
            socket,
            remote_info.peer_id,
            self.event_sender.clone(),
            &self.executor,
            &mut self.writers.borrow_mut(),
        )?;
        Ok(EventFromPeers::NewConnection {
            remote_handshake: remote_info,
        })
    }

    async fn serve_connections(&self) {
        loop {
            match self.event_receiver.recv().await {
                Err(error) => {
                    info!("peers_main stopping: {}", error);
                    return;
                }
                Ok(EventToPeers::SendMessage {
                    remote_peer_id,
                    message,
                }) => {
                    if let Some(event_sender) = self.writers.borrow_mut().get_mut(&remote_peer_id) {
                        event_sender
                            .try_send(EventToPeers::SendMessage {
                                remote_peer_id,
                                message,
                            })
                            .unwrap();
                    } else {
                        error!(
                            "InboundEvent::SendMessage: connection {:?} not found",
                            &remote_peer_id
                        );
                    }
                }
                Ok(EventToPeers::CloseConnection { remote_peer_id }) => {
                    info!("closing connection to {:?}", &remote_peer_id);
                    if let Some(event_sender) = self.writers.borrow_mut().get_mut(&remote_peer_id) {
                        event_sender
                            .try_send(EventToPeers::CloseConnection { remote_peer_id })
                            .unwrap();
                        self.writers.borrow_mut().remove(&remote_peer_id);
                    } else {
                        error!(
                            "InboundEvent::CloseConnection: connection {:?} not found",
                            &remote_peer_id
                        );
                    }
                }
                Ok(EventToPeers::CreateOutboundConnection {
                    remote_addr,
                    remote_peer_id,
                    info_hash,
                }) => {
                    let remote_peer_id = remote_peer_id.as_ref().map(|id| &id[..]);
                    let local_info = Handshake {
                        peer_id: self.local_peer_id,
                        info_hash,
                    };
                    match create_outbound_connection(remote_addr, local_info, remote_peer_id).await
                    {
                        Err(error) => error!(
                            "failed to create outbound connection to {}: {}",
                            remote_addr, error
                        ),
                        Ok((socket, remote_info)) => {
                            let remote_peer_id = remote_info.peer_id;
                            if let Err(error) = add_new_connection(
                                socket,
                                remote_peer_id,
                                self.event_sender.clone(),
                                &self.executor,
                                &mut self.writers.borrow_mut(),
                            ) {
                                error!("failed to add outbound connection: {}", error);
                            } else {
                                self.event_sender
                                    .try_send(EventFromPeers::NewConnection {
                                        remote_handshake: remote_info,
                                    })
                                    .unwrap();
                            }
                        }
                    }
                }
                Ok(EventToPeers::Stop) => {
                    for (_, event_sender) in self.writers.borrow_mut().iter_mut() {
                        let _ = event_sender
                            .send(EventToPeers::CloseConnection {
                                remote_peer_id: Default::default(),
                            })
                            .await;
                    }
                    return;
                }
            }
        }
    }
}

struct Receiver {
    source: Async<TcpStream>,
    remote_peer_id: [u8; 20],
    event_sender: async_channel::Sender<EventFromPeers>,
}

impl Receiver {
    async fn run(mut self) {
        loop {
            match PeerMessage::read_from(&mut self.source).await {
                Ok(message) => {
                    self.event_sender
                        .try_send(EventFromPeers::MessageReceived {
                            remote_peer_id: self.remote_peer_id,
                            message,
                        })
                        .unwrap();
                }
                Err(error) => {
                    self.event_sender
                        .try_send(EventFromPeers::ConnectionClosed {
                            remote_peer_id: self.remote_peer_id,
                            error: Some(error),
                        })
                        .unwrap();
                }
            }
        }
    }
}

struct Sender {
    sink: BufWriter<Async<TcpStream>>,
    event_receiver: async_channel::Receiver<EventToPeers>,
}

impl Sender {
    async fn run(mut self) {
        loop {
            match self.event_receiver.recv().await {
                Ok(EventToPeers::SendMessage { message, .. }) => {
                    if let Err(error) = message.write_to(&mut self.sink).await {
                        error!("write failed: {}", error);
                    }
                }
                Ok(EventToPeers::CloseConnection { .. }) => {
                    let _ = self.sink.get_mut().close().await;
                    let _ = self.sink.get_mut().get_mut().shutdown(Shutdown::Both);
                    return;
                }
                Ok(_) => {
                    error!("Unexpected event received");
                    return;
                }
                Err(e) => {
                    error!("Error receiving event: {}", e);
                    return;
                }
            }
        }
    }
}

fn add_new_connection(
    socket: Async<TcpStream>,
    remote_peer_id: [u8; 20],
    event_sender: async_channel::Sender<EventFromPeers>,
    executor: &LocalSpawner,
    writers: &mut HashMap<[u8; 20], async_channel::Sender<EventToPeers>>,
) -> io::Result<()> {
    let socket_copy = Async::<TcpStream>::try_from(socket.get_ref().try_clone()?)?;

    let (local_event_sender, local_event_receiver) = async_channel::unbounded::<EventToPeers>();

    let writer = Sender {
        sink: BufWriter::new(socket_copy),
        event_receiver: local_event_receiver,
    };
    let reader = Receiver {
        source: socket,
        remote_peer_id,
        event_sender,
    };
    match writers.insert(remote_peer_id, local_event_sender) {
        None => {
            executor
                .spawn_local(reader.run())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}", e)))?;
            executor
                .spawn_local(writer.run())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}", e)))?;
            Ok(())
        }
        Some(_) => Err(io::Error::new(
            io::ErrorKind::Other,
            format!("connection to {:?} already exists", remote_peer_id),
        )),
    }
}

async fn create_outbound_connection(
    server_addr: SocketAddr,
    local_info: Handshake,
    server_peer_id: Option<&[u8]>,
) -> io::Result<(Async<TcpStream>, Handshake)> {
    let socket = Async::<TcpStream>::connect(server_addr).await?;
    do_handshake_outgoing(socket, local_info.clone(), server_peer_id).await
}
