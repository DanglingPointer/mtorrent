use crate::peers::handshake::{do_handshake_incoming, do_handshake_outgoing, Handshake};
use crate::peers::peer_message::PeerMessage;
use async_channel;
use async_io::Async;
use futures::executor::LocalSpawner;
use futures::io::BufWriter;
use futures::prelude::*;
use futures::task::LocalSpawnExt;
use log::{error, info};
use std::collections::HashMap;
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::{fmt, io};

mod handshake;
pub mod peer_message;

pub enum EventToPeers {
    CreateOutboundConnection {
        remote_addr: SocketAddr,
        remote_peer_id: Option<[u8; 20]>,
        info_hash: [u8; 20],
    },
    CreateInboundConnection {
        socket: Async<TcpStream>,
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
    executor: LocalSpawner,
    event_sender: async_channel::Sender<EventFromPeers>,
    event_receiver: async_channel::Receiver<EventToPeers>,
    writers: HashMap<[u8; 20], async_channel::Sender<EventToPeers>>,
}

impl ConnectionManager {
    pub fn new(
        local_peer_id: [u8; 20],
        executor: LocalSpawner,
        event_sender: async_channel::Sender<EventFromPeers>,
        event_receiver: async_channel::Receiver<EventToPeers>,
    ) -> Self {
        ConnectionManager {
            local_peer_id,
            executor,
            event_sender,
            event_receiver,
            writers: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        info!("ConnectionManager starting");
        let mut should_stop = false;
        while !should_stop {
            match self.event_receiver.recv().await {
                Err(error) => {
                    info!("peers_main stopping: {}", error);
                    should_stop = true;
                }
                Ok(event) => {
                    should_stop = matches!(&event, EventToPeers::Stop);
                    match self.handle_event(event).await {
                        Err(e) => error!("{}", e),
                        _ => (),
                    }
                }
            }
        }
        info!("ConnectionManager exiting");
    }

    async fn handle_event(&mut self, event: EventToPeers) -> io::Result<()> {
        match event {
            EventToPeers::SendMessage {
                remote_peer_id,
                message,
            } => {
                let event_sender =
                    self.writers
                        .get_mut(&remote_peer_id)
                        .ok_or(custom_error(format!(
                            "InboundEvent::SendMessage: connection {:?} not found",
                            &remote_peer_id
                        )))?;

                event_sender
                    .try_send(EventToPeers::SendMessage {
                        remote_peer_id,
                        message,
                    })
                    .map_err(convert_error)?;
            }
            EventToPeers::CloseConnection { remote_peer_id } => {
                info!("closing connection to {:?}", &remote_peer_id);

                let event_sender =
                    self.writers
                        .get_mut(&remote_peer_id)
                        .ok_or(custom_error(format!(
                            "InboundEvent::CloseConnection: connection {:?} not found",
                            &remote_peer_id
                        )))?;

                event_sender
                    .try_send(EventToPeers::CloseConnection { remote_peer_id })
                    .map_err(convert_error)?;

                self.writers.remove(&remote_peer_id);
            }
            EventToPeers::CreateInboundConnection { socket } => {
                let remote_info = self.create_inbound_connection(socket).await.map_err(|e| {
                    custom_error(format!("Error creating inbound connection: {}", e))
                })?;
                self.event_sender
                    .try_send(EventFromPeers::NewConnection {
                        remote_handshake: remote_info,
                    })
                    .map_err(convert_error)?;
            }
            EventToPeers::CreateOutboundConnection {
                remote_addr,
                remote_peer_id,
                info_hash,
            } => {
                let remote_peer_id = remote_peer_id.as_ref().map(|id| &id[..]);
                let local_info = Handshake {
                    peer_id: self.local_peer_id,
                    info_hash,
                };
                let remote_info = self
                    .create_outbound_connection(remote_addr, local_info, remote_peer_id)
                    .await
                    .map_err(|e| {
                        custom_error(format!(
                            "failed to create outbound connection to {}: {}",
                            remote_addr, e
                        ))
                    })?;
                self.event_sender
                    .try_send(EventFromPeers::NewConnection {
                        remote_handshake: remote_info,
                    })
                    .map_err(convert_error)?;
            }
            EventToPeers::Stop => {
                for (_, event_sender) in self.writers.iter_mut() {
                    let _ = event_sender
                        .send(EventToPeers::CloseConnection {
                            remote_peer_id: Default::default(),
                        })
                        .await;
                }
            }
        }
        Ok(())
    }

    async fn create_inbound_connection(
        &mut self,
        socket: Async<TcpStream>,
    ) -> io::Result<Handshake> {
        let (socket, remote_info) =
            do_handshake_incoming(socket, &self.local_peer_id[..], None).await?;
        add_new_connection(
            socket,
            remote_info.peer_id,
            self.event_sender.clone(),
            &self.executor,
            &mut self.writers,
        )?;
        Ok(remote_info)
    }

    async fn create_outbound_connection(
        &mut self,
        server_addr: SocketAddr,
        local_info: Handshake,
        server_peer_id: Option<&[u8]>,
    ) -> io::Result<Handshake> {
        let socket = Async::<TcpStream>::connect(server_addr).await?;
        let (socket, remote_info) =
            do_handshake_outgoing(socket, local_info.clone(), server_peer_id).await?;
        let remote_peer_id = remote_info.peer_id;
        add_new_connection(
            socket,
            remote_peer_id,
            self.event_sender.clone(),
            &self.executor,
            &mut self.writers,
        )?;
        Ok(remote_info)
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
            executor.spawn_local(reader.run()).map_err(convert_error)?;
            executor.spawn_local(writer.run()).map_err(convert_error)?;
            Ok(())
        }
        Some(_) => Err(custom_error(format!(
            "connection to {:?} already exists",
            remote_peer_id
        ))),
    }
}

fn convert_error<E: fmt::Display>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, format!("{}", e))
}

fn custom_error(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::Other, msg)
}
