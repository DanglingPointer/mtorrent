use super::super::PeerReporter;
use futures_util::StreamExt;
use mtorrent_core::pwp;
use mtorrent_core::utp;
use mtorrent_utils::peer_id::PeerId;
use std::io;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::runtime;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task;
use tokio::time;
use tokio::time::Instant;
use tokio::{join, select};

pub fn launch_utp(pwp_runtime: &runtime::Handle, local_addr: SocketAddr) -> UtpHandle {
    let (cmd_sender, cmd_receiver) = mpsc::channel(1);
    pwp_runtime.spawn(async move {
        match UdpSocket::bind(local_addr).await {
            Ok(socket) => {
                task::spawn_local(async move {
                    let (connection_spawner, connect_reporter, io_driver) = utp::init(socket);
                    join!(
                        io_driver.run(),
                        bridge_task(connection_spawner, cmd_receiver, connect_reporter)
                    );
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
    mut connection_spawner: utp::ConnectionSpawner,
    mut cmd_receiver: mpsc::Receiver<Command>,
    mut connect_reporter: utp::ConnectReporter,
) {
    async fn handle_command(
        cmd: Option<Command>,
        connection_spawner: &mut utp::ConnectionSpawner,
        reporter_option: &mut Option<PeerReporter>,
    ) -> ControlFlow<()> {
        let Some(cmd) = cmd else {
            return ControlFlow::Break(());
        };
        match cmd {
            Command::Restart { reporter } => {
                *reporter_option = Some(reporter);
                connection_spawner.reset_all();
            }
            Command::OutboundConnect { args, resp } => {
                let deadline = Instant::now() + args.timeout;
                let Some(stream) =
                    connection_spawner.spawn_outbound(args.peer_addr, args.timeout).await
                else {
                    // uTP shutdown
                    _ = resp.send(Err(io::ErrorKind::BrokenPipe.into()));
                    return ControlFlow::Break(());
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
                    return ControlFlow::Break(());
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
        ControlFlow::Continue(())
    }

    async fn handle_connect(
        connect: Option<(SocketAddr, utp::InboundConnectData)>,
        reporter_option: &mut Option<PeerReporter>,
    ) -> ControlFlow<()> {
        let Some((addr, data)) = connect else {
            return ControlFlow::Break(());
        };
        match reporter_option.as_ref() {
            Some(reporter) => {
                if !reporter.report_accepted_utp(addr, data).await {
                    reporter_option.take();
                }
            }
            None => {
                log::warn!("Ignored inbound utp connect: no reporter");
            }
        }
        ControlFlow::Continue(())
    }

    let mut reporter = None;

    loop {
        select! {
            biased;
            cmd = cmd_receiver.recv() => {
                if handle_command(cmd, &mut connection_spawner, &mut reporter).await.is_break() {
                    break;
                }
            }
            connect = connect_reporter.next() => {
                if handle_connect(connect, &mut reporter).await.is_break() {
                    break;
                }
            }
        }
    }
}
