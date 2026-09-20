use super::super::PeerReporter;
use crate::utils::join_all_with_timeout;
use bytes::BytesMut;
use futures_util::{Stream, StreamExt, stream};
use local_async_utils::prelude::*;
use mtorrent_base::{pe, pwp, utp};
use mtorrent_utils::net;
use mtorrent_utils::peer_id::PeerId;
use mtorrent_utils::task_scope::TaskScope;
use std::io;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio::{join, select, task, time};

pub fn init_utp(
    local_addr_v4: SocketAddrV4,
    local_addr_v6: SocketAddrV6,
    interface: Option<String>,
) -> (UtpHandle, UtpActor) {
    let (cmd_sender, cmd_receiver) = mpsc::channel(1);
    (
        UtpHandle(cmd_sender),
        UtpActor {
            local_addr_v4,
            local_addr_v6,
            interface,
            cmd_receiver,
        },
    )
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

pub struct UtpActor {
    local_addr_v4: SocketAddrV4,
    local_addr_v6: SocketAddrV6,
    interface: Option<String>,
    cmd_receiver: mpsc::Receiver<Command>,
}

impl UtpActor {
    pub async fn run(self) {
        let Self {
            local_addr_v4,
            local_addr_v6,
            interface,
            cmd_receiver,
        } = self;

        let mut scope = TaskScope::new();
        _ = scope
            .spawn_local(async move {
                let _sw = info_stopwatch!(sec!(0), "UtpActor");

                let local_addr_v4 = SocketAddr::V4(local_addr_v4);
                let local_addr_v6 = SocketAddr::V6(local_addr_v6);

                let mut tasks = task::JoinSet::new();

                let v6_result = create_endpoint(local_addr_v6, interface.as_deref(), &mut tasks);
                let v4_result = create_endpoint(local_addr_v4, interface.as_deref(), &mut tasks);

                for (result, local_addr) in
                    [(&v4_result, local_addr_v4), (&v6_result, local_addr_v6)]
                {
                    match result {
                        Ok((_, _)) => log::info!("Created uTP endpoint on {local_addr}"),
                        Err(e) => log::error!("Failed to create uTP endpoint on {local_addr}: {e}"),
                    }
                }

                match (v4_result, v6_result) {
                    (Ok(v4), Ok(v6)) => {
                        run_bridge(v4, v6, cmd_receiver).await;
                    }
                    (Ok(v4), Err(_)) => {
                        let ep = v4.0.clone();
                        run_bridge(v4, (ep, stream::pending()), cmd_receiver).await;
                    }
                    (Err(_), Ok(v6)) => {
                        let ep = v6.0.clone();
                        run_bridge((ep, stream::pending()), v6, cmd_receiver).await;
                    }
                    (Err(_), Err(_)) => {}
                }

                join_all_with_timeout!(tasks, sec!(3));
            })
            .await;
    }
}

fn create_endpoint(
    local_addr: SocketAddr,
    interface: Option<&str>,
    join_set: &mut task::JoinSet<()>,
) -> io::Result<(utp::EndpointHandle, utp::InboundListener)> {
    let socket = net::bound_udp_socket(local_addr, interface)?;
    let (endpoint, listener, driver) = utp::new_endpoint(socket);
    join_set.spawn_local(driver.run());
    Ok((endpoint, listener))
}

async fn run_bridge<
    L1: Stream<Item = (SocketAddr, utp::InboundConnectData)> + Unpin,
    L2: Stream<Item = (SocketAddr, utp::InboundConnectData)> + Unpin,
>(
    (endpoint_v4, listener_v4): (utp::EndpointHandle, L1),
    (endpoint_v6, listener_v6): (utp::EndpointHandle, L2),
    cmd_receiver: mpsc::Receiver<Command>,
) {
    let (reporter_sender, reporter_receiver) = local_unbounded::channel();

    select! {
        biased;
        _ = handle_commands(cmd_receiver, endpoint_v4, endpoint_v6, reporter_sender) => (),
        _ = handle_inbound(listener_v4, listener_v6, reporter_receiver) => (),
    }
}

async fn handle_inbound(
    mut listener_v4: impl Stream<Item = (SocketAddr, utp::InboundConnectData)> + Unpin,
    mut listener_v6: impl Stream<Item = (SocketAddr, utp::InboundConnectData)> + Unpin,
    mut reporter_receiver: local_unbounded::Receiver<PeerReporter>,
) {
    let Some(mut reporter) = reporter_receiver.next().await else {
        return;
    };

    loop {
        let next_inbound = async {
            select! {
                inbound = listener_v4.next() => inbound,
                inbound = listener_v6.next() => inbound,
            }
        };

        select! {
            biased;
            received = reporter_receiver.next() => match received {
                Some(new_reporter) => {
                    reporter = new_reporter;
                }
                None => break,
            },
            received = next_inbound => match received {
                Some((addr, data)) => {
                    _ = reporter.report_accepted_utp(addr, data).await;
                }
                None => break,
            },
        }
    }
}

async fn handle_commands(
    mut cmd_receiver: mpsc::Receiver<Command>,
    endpoint_v4: utp::EndpointHandle,
    endpoint_v6: utp::EndpointHandle,
    reporter_sender: local_unbounded::Sender<PeerReporter>,
) {
    while let Some(cmd) = cmd_receiver.recv().await {
        match cmd {
            Command::Restart { reporter } => {
                _ = reporter_sender.send(reporter);
                join!(endpoint_v4.reset_connections(), endpoint_v6.reset_connections(),);
            }
            Command::OutboundConnect { args, resp } => {
                let endpoint = match args.peer_addr {
                    SocketAddr::V4(_) => endpoint_v4.clone(),
                    SocketAddr::V6(_) => endpoint_v6.clone(),
                };
                task::spawn_local(async move {
                    let ret = time::timeout_at(args.deadline, async {
                        let mut stream = endpoint.add_outbound_connection(args.peer_addr).await?;
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
                let endpoint = match args.peer_addr {
                    SocketAddr::V4(_) => endpoint_v4.clone(),
                    SocketAddr::V6(_) => endpoint_v6.clone(),
                };
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};
    use tokio::net::UdpSocket;

    #[tokio::test(flavor = "local")]
    async fn test_stopping_utp_releases_port() {
        _ = simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Debug).init();

        // find available port
        let port = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap()
            .local_addr()
            .unwrap()
            .port();

        let local_ipv4_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
        let local_ipv6_addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, port, 0, 0);

        // start uTP
        let (handle, actor) = init_utp(local_ipv4_addr, local_ipv6_addr, None);
        let task_handle = task::spawn_local(actor.run());
        task::yield_now().await;

        // verify that uTP is running and the port is in use
        handle.restart(PeerReporter::new_mock()).await.unwrap();
        task::yield_now().await;
        assert!(UdpSocket::bind((Ipv4Addr::LOCALHOST, port)).await.is_err());

        // stop uTP
        drop(handle);
        task_handle.await.unwrap();

        // verify that the port is released
        assert!(UdpSocket::bind((Ipv4Addr::LOCALHOST, port)).await.is_ok());
    }

    #[tokio::test(flavor = "local")]
    async fn test_aborting_utp_releases_port() {
        _ = simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Debug).init();

        // find available port
        let port = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap()
            .local_addr()
            .unwrap()
            .port();

        let local_ipv4_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
        let local_ipv6_addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, port, 0, 0);

        // start uTP
        let (handle, actor) = init_utp(local_ipv4_addr, local_ipv6_addr, None);
        let task_handle = task::spawn_local(actor.run());
        task::yield_now().await;

        // verify that uTP is running and the port is in use
        handle.restart(PeerReporter::new_mock()).await.unwrap();
        task::yield_now().await;
        assert!(UdpSocket::bind((Ipv4Addr::LOCALHOST, port)).await.is_err());

        // abort the uTP task
        task_handle.abort();
        let _ = task_handle.await;

        // verify that the port is released
        assert!(UdpSocket::bind((Ipv4Addr::LOCALHOST, port)).await.is_ok());
    }
}
