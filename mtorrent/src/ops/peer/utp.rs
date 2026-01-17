use super::super::PeerReporter;
use futures_util::StreamExt;
use local_async_utils::prelude::*;
use mtorrent_core::pwp;
use mtorrent_core::utp;
use mtorrent_utils::peer_id::PeerId;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::join;
use tokio::net::UdpSocket;
use tokio::runtime;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task;
use tokio::time;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub fn launch_utp(
    pwp_runtime: &runtime::Handle,
    local_addr: SocketAddr,
    peer_reporter: PeerReporter,
    canceller: CancellationToken,
) -> UtpHandle {
    let (cmd_sender, cmd_receiver) = mpsc::channel(1);
    pwp_runtime.spawn(async move {
        // the canceller below is mostly needed when binding UDP socket
        task::spawn_local(canceller.run_until_cancelled_owned(async move {
            match UdpSocket::bind(local_addr).await {
                Ok(socket) => {
                    let (connection_spawner, connect_reporter, io_driver) = utp::init(socket);
                    join!(
                        io_driver.run(),
                        handle_commands(cmd_receiver, connection_spawner),
                        handle_accepted(peer_reporter, connect_reporter)
                    );
                }
                Err(e) => {
                    log::error!("Failed to create uTP socket: {e}");
                }
            }
        }));
    });
    UtpHandle(cmd_sender)
}

pub(super) struct OutboundConnectArgs {
    pub(super) local_peer_id: PeerId,
    pub(super) info_hash: [u8; 20],
    pub(super) extension_protocol_enabled: bool,
    pub(super) peer_addr: SocketAddr,
    pub(super) timeout: Duration,
}

pub(super) struct InboundConnectArgs {
    pub(super) local_peer_id: PeerId,
    pub(super) info_hash: [u8; 20],
    pub(super) extension_protocol_enabled: bool,
    pub(super) peer_addr: SocketAddr,
    pub(super) data: utp::InboundConnectData,
    pub(super) timeout: Duration,
}

pub(super) type ConnectResult =
    io::Result<(pwp::DownloadChannels, pwp::UploadChannels, Option<pwp::ExtendedChannels>)>;

enum Command {
    OutboundConnect {
        args: OutboundConnectArgs,
        resp: oneshot::Sender<ConnectResult>,
    },
    InboundConnect {
        args: InboundConnectArgs,
        resp: oneshot::Sender<ConnectResult>,
    },
}

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
}

async fn handle_commands(
    mut receiver: mpsc::Receiver<Command>,
    mut connection_spawner: utp::ConnectionSpawner,
) {
    let _sw = info_stopwatch!(Duration::ZERO, "handle_commands() task");
    while let Some(cmd) = receiver.recv().await {
        match cmd {
            Command::OutboundConnect { args, resp } => {
                let deadline = Instant::now() + args.timeout;
                let Some(stream) =
                    connection_spawner.spawn_outbound(args.peer_addr, args.timeout).await
                else {
                    // uTP shutdown
                    _ = resp.send(Err(io::ErrorKind::BrokenPipe.into()));
                    return;
                };
                task::spawn_local(async move {
                    let ret = time::timeout_at(
                        deadline,
                        pwp::channels_for_outbound_connection(
                            &args.local_peer_id,
                            &args.info_hash,
                            args.extension_protocol_enabled,
                            args.peer_addr,
                            stream,
                            None,
                        ),
                    )
                    .await;
                    _ = resp.send(ret.unwrap_or_else(|e| Err(e.into())));
                });
            }
            Command::InboundConnect { args, resp } => {
                let deadline = Instant::now() + args.timeout;
                let Some(stream) =
                    connection_spawner.spawn_inbound(args.peer_addr, args.data, args.timeout).await
                else {
                    // uTP shutdown
                    _ = resp.send(Err(io::ErrorKind::BrokenPipe.into()));
                    return;
                };
                task::spawn_local(async move {
                    let ret = time::timeout_at(
                        deadline,
                        pwp::channels_for_inbound_connection(
                            &args.local_peer_id,
                            Some(&args.info_hash),
                            args.extension_protocol_enabled,
                            args.peer_addr,
                            stream,
                        ),
                    )
                    .await;
                    _ = resp.send(ret.unwrap_or_else(|e| Err(e.into())));
                });
            }
        }
    }
}

async fn handle_accepted(peer_reporter: PeerReporter, mut connect_reporter: utp::ConnectReporter) {
    let _sw = info_stopwatch!(Duration::ZERO, "handle_accepted() task");
    loop {
        let Some((addr, data)) = connect_reporter.next().await else {
            break;
        };
        if !peer_reporter.report_accepted_utp(addr, data).await {
            break;
        }
    }
}
