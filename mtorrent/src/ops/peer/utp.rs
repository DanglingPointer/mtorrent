use super::super::PeerReporter;
use bytes::BytesMut;
use futures_util::StreamExt;
use mtorrent_core::{pe, pwp, utp};
use mtorrent_utils::peer_id::PeerId;
use std::io;
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio::{join, runtime, select, task, time};

pub fn launch_utp(pwp_runtime: &runtime::Handle, local_addr: SocketAddr) -> UtpHandle {
    let (cmd_sender, cmd_receiver) = mpsc::channel(1);
    pwp_runtime.spawn(async move {
        match UdpSocket::bind(local_addr).await {
            Ok(socket) => {
                task::spawn_local(async move {
                    let (endpoint, connect_reporter, udp_demux) = utp::new_endpoint(socket);
                    join!(udp_demux.run(), bridge_task(endpoint, cmd_receiver, connect_reporter));
                });
            }
            Err(e) => {
                log::error!("Failed to create uTP socket: {e}");
            }
        }
    });
    UtpHandle(cmd_sender)
}

pub(super) struct OutboundConnectArgs {
    pub(super) local_peer_id: PeerId,
    pub(super) info_hash: [u8; 20],
    pub(super) extension_protocol_enabled: bool,
    pub(super) protocol_encryption_enabled: bool,
    pub(super) peer_addr: SocketAddr,
    pub(super) deadline: Instant,
}

pub(super) struct InboundConnectArgs {
    pub(super) local_peer_id: PeerId,
    pub(super) info_hash: [u8; 20],
    pub(super) extension_protocol_enabled: bool,
    pub(super) peer_addr: SocketAddr,
    pub(super) data: utp::InboundConnectData,
    pub(super) deadline: Instant,
}

pub(super) type ConnectResult =
    io::Result<(pwp::DownloadChannels, pwp::UploadChannels, Option<pwp::ExtendedChannels>)>;

enum Command {
    Restart {
        reporter: PeerReporter,
    },
    OutboundConnect {
        args: OutboundConnectArgs,
        resp: oneshot::Sender<ConnectResult>,
    },
    InboundConnect {
        args: InboundConnectArgs,
        resp: oneshot::Sender<ConnectResult>,
    },
}

#[derive(Clone)]
pub struct UtpHandle(mpsc::Sender<Command>);

impl UtpHandle {
    #[cfg(test)]
    pub(super) fn new_mock() -> Self {
        let (cmd_sender, _cmd_receiver) = mpsc::channel(1);
        UtpHandle(cmd_sender)
    }

    pub(super) async fn outbound_connect(&self, args: OutboundConnectArgs) -> ConnectResult {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::OutboundConnect {
            args,
            resp: resp_tx,
        };
        self.0.send(cmd).await.map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        resp_rx.await.map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?
    }

    pub(super) async fn inbound_connect(&self, args: InboundConnectArgs) -> ConnectResult {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::InboundConnect {
            args,
            resp: resp_tx,
        };
        self.0.send(cmd).await.map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        resp_rx.await.map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?
    }

    pub(crate) async fn restart(&self, peer_reporter: PeerReporter) -> io::Result<()> {
        self.0
            .send(Command::Restart {
                reporter: peer_reporter,
            })
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))
    }
}

async fn bridge_task(
    endpoint: utp::EndpointHandle,
    mut cmd_receiver: mpsc::Receiver<Command>,
    mut listener: utp::InboundListener,
) {
    struct Bridge {
        reporter: Option<PeerReporter>,
        endpoint: utp::EndpointHandle,
    }

    impl Bridge {
        async fn report_inbound(&mut self, addr: SocketAddr, data: utp::InboundConnectData) {
            match self.reporter.as_ref() {
                Some(reporter) => {
                    if !reporter.report_accepted_utp(addr, data).await {
                        self.reporter.take();
                    }
                }
                None => {
                    log::warn!("Ignored inbound uTP connect: no reporter");
                }
            }
        }
        async fn process_command(&mut self, cmd: Command) {
            match cmd {
                Command::Restart { reporter } => {
                    self.reporter = Some(reporter);
                    self.endpoint.reset_connections().await;
                }
                Command::OutboundConnect { args, resp } => {
                    let endpoint = self.endpoint.clone();
                    task::spawn_local(async move {
                        let ret = time::timeout_at(args.deadline, async {
                            let mut stream =
                                endpoint.add_outbound_connection(args.peer_addr).await?;
                            let crypto = if args.protocol_encryption_enabled {
                                pe::outbound_handshake(&mut stream, &args.info_hash, &[0u8; 0][..])
                                    .await?
                            } else {
                                None
                            };
                            pwp::channels_for_outbound_connection(
                                &args.local_peer_id,
                                &args.info_hash,
                                args.extension_protocol_enabled,
                                args.peer_addr,
                                stream,
                                None,
                                crypto,
                            )
                            .await
                        })
                        .await;
                        _ = resp.send(ret.unwrap_or_else(|e| Err(e.into())));
                    });
                }
                Command::InboundConnect { args, resp } => {
                    let endpoint = self.endpoint.clone();
                    task::spawn_local(async move {
                        let ret = time::timeout_at(args.deadline, async {
                            let stream =
                                endpoint.add_inbound_connection(args.peer_addr, args.data).await?;
                            match pe::detect_encryption(stream).await? {
                                pe::MaybeEncrypted::Plain(stream) => {
                                    pwp::channels_for_inbound_connection(
                                        &args.local_peer_id,
                                        &args.info_hash,
                                        args.extension_protocol_enabled,
                                        args.peer_addr,
                                        stream,
                                        None,
                                    )
                                    .await
                                }
                                pe::MaybeEncrypted::Encrypted(mut stream) => {
                                    let mut ia_buffer = BytesMut::new();
                                    let crypto = pe::inbound_handshake(
                                        &mut stream,
                                        &args.info_hash,
                                        &mut ia_buffer,
                                    )
                                    .await?;
                                    let (_, stream) = stream.into_parts();
                                    let stream = pe::PrefixedStream::new(ia_buffer, stream);
                                    pwp::channels_for_inbound_connection(
                                        &args.local_peer_id,
                                        &args.info_hash,
                                        args.extension_protocol_enabled,
                                        args.peer_addr,
                                        stream,
                                        crypto,
                                    )
                                    .await
                                }
                            }
                        })
                        .await;
                        _ = resp.send(ret.unwrap_or_else(|e| Err(e.into())));
                    });
                }
            }
        }
    }

    let mut bridge = Bridge {
        reporter: None,
        endpoint,
    };

    loop {
        select! {
            biased;
            cmd = cmd_receiver.recv() => {
                let Some(cmd) = cmd else {
                    break;
                };
                bridge.process_command(cmd).await;
            }
            inbound = listener.next() => {
                let Some((addr, data)) = inbound else {
                    break;
                };
                bridge.report_inbound(addr, data).await;
            }
        }
    }
}
