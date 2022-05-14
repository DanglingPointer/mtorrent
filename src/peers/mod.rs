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

#[derive(Debug)]
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
                    let result = self.event_sender.try_send(EventFromPeers::MessageReceived {
                        remote_peer_id: self.remote_peer_id,
                        message,
                    });
                    if result.is_err() {
                        return;
                    }
                }
                Err(error) => {
                    error!("read failed: {}", error);
                    let _ = self
                        .event_sender
                        .try_send(EventFromPeers::ConnectionClosed {
                            remote_peer_id: self.remote_peer_id,
                            error: Some(error),
                        });
                    return;
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_channel::{Receiver, Sender};
    use futures::executor::LocalPool;
    use futures::join;
    use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
    use std::thread::JoinHandle;

    async fn accept_tcp_connection(listener_port: u16) -> Async<TcpStream> {
        let server_addr = SocketAddr::from(SocketAddrV4::new(Ipv4Addr::LOCALHOST, listener_port));
        let listener = Async::<TcpListener>::bind(server_addr).unwrap();

        let (socket, _remote_addr) = listener.accept().await.unwrap();
        socket
    }

    fn spawn_peers(
        count: usize,
    ) -> (
        Vec<Sender<EventToPeers>>,
        Vec<Receiver<EventFromPeers>>,
        JoinHandle<()>,
    ) {
        let mut senders_to_peers = Vec::with_capacity(count);
        let mut receivers_from_peers = Vec::with_capacity(count);

        let mut peer_senders = Vec::with_capacity(count);
        let mut peer_receivers = Vec::with_capacity(count);

        for _ in 0..count {
            let (sender_to_peer, peer_receiver) = async_channel::unbounded::<EventToPeers>();
            senders_to_peers.push(sender_to_peer);
            peer_receivers.push(peer_receiver);

            let (peer_sender, receiver_from_peer) = async_channel::unbounded::<EventFromPeers>();
            peer_senders.push(peer_sender);
            receivers_from_peers.push(receiver_from_peer);
        }

        let handle = std::thread::spawn(move || {
            let mut pool = LocalPool::new();

            for i in 0..count {
                let peer = ConnectionManager::new(
                    [i as u8; 20],
                    pool.spawner(),
                    peer_senders[i].clone(),
                    peer_receivers[i].clone(),
                );
                pool.spawner()
                    .spawn_local(async move {
                        peer.run().await;
                    })
                    .unwrap();
            }
            drop(peer_senders);
            drop(peer_receivers);

            pool.run();
        });

        (senders_to_peers, receivers_from_peers, handle)
    }

    #[test]
    fn test_connect_two_peers() {
        let listener_port = 6880u16;
        let server_addr = SocketAddr::from(SocketAddrV4::new(Ipv4Addr::LOCALHOST, listener_port));

        let (senders_to_peers, receivers_from_peers, handle) = spawn_peers(2);

        let peer_0_create_inbound_connection = async {
            let accepted_socket = accept_tcp_connection(listener_port).await;
            senders_to_peers[0]
                .send(EventToPeers::CreateInboundConnection {
                    socket: accepted_socket,
                })
                .await
                .unwrap();
        };
        let peer_1_create_outbound_connection = async {
            senders_to_peers[1]
                .send(EventToPeers::CreateOutboundConnection {
                    remote_addr: server_addr,
                    remote_peer_id: None,
                    info_hash: [42u8; 20],
                })
                .await
                .unwrap();
        };
        let peer_0_verify_new_connection = async {
            match receivers_from_peers[0].recv().await.unwrap() {
                EventFromPeers::NewConnection { remote_handshake } => {
                    assert_eq!([42u8; 20], remote_handshake.info_hash);
                    assert_eq!([1u8; 20], remote_handshake.peer_id);
                }
                event => {
                    panic!("Unexpected event from peer 0: {:?}", event);
                }
            }
        };
        let peer_1_verify_new_connection = async {
            match receivers_from_peers[1].recv().await.unwrap() {
                EventFromPeers::NewConnection { remote_handshake } => {
                    assert_eq!([42u8; 20], remote_handshake.info_hash);
                    assert_eq!([0u8; 20], remote_handshake.peer_id);
                }
                event => {
                    panic!("Unexpected event from peer 1: {:?}", event);
                }
            }
        };
        async_io::block_on(async {
            join!(
                peer_0_create_inbound_connection,
                peer_1_create_outbound_connection,
                peer_0_verify_new_connection,
                peer_1_verify_new_connection
            )
        });

        let peer_0_send_msg = async {
            senders_to_peers[0]
                .send(EventToPeers::SendMessage {
                    remote_peer_id: [1u8; 20],
                    message: PeerMessage::Have { piece_index: 42 },
                })
                .await
                .unwrap();
        };
        let peer_1_verify_msg = async {
            match receivers_from_peers[1].recv().await.unwrap() {
                EventFromPeers::MessageReceived {
                    remote_peer_id,
                    message: PeerMessage::Have { piece_index: 42 },
                } => {
                    assert_eq!([0u8; 20], remote_peer_id);
                }
                event => {
                    panic!("Unexpected event from peer 1: {:?}", event);
                }
            }
        };
        async_io::block_on(async { join!(peer_0_send_msg, peer_1_verify_msg) });

        let peer_0_stop = async {
            senders_to_peers[0].send(EventToPeers::Stop).await.unwrap();
        };
        let peer_1_stop = async {
            senders_to_peers[1].send(EventToPeers::Stop).await.unwrap();
        };
        async_io::block_on(async { join!(peer_0_stop, peer_1_stop) });
        drop(senders_to_peers);
        drop(receivers_from_peers);

        handle.join().unwrap();
    }
}
