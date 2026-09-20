use derive_more::Debug;
use local_async_utils::prelude::*;
use mtorrent_base::pwp::{PeerOrigin, TransportProto};
use mtorrent_base::utp;
use mtorrent_utils::connect_recorder::{ConnectRecord, ConnectRecorder};
use mtorrent_utils::task_scope::TaskScope;
use rand::RngExt;
use std::cell::Cell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use std::{cmp, io};
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};

#[derive(Debug)]
struct OutboundConnect {
    addr: SocketAddr,
    origin: PeerOrigin,
    attempt: usize,
}

#[derive(Debug)]
struct InboundConnect {
    addr: SocketAddr,
    data: InboundData,
}

#[derive(Debug)]
enum InboundData {
    Tcp(TcpStream),
    Utp(utp::InboundConnectData),
}

#[derive(Clone)]
pub struct PeerReporter {
    discovered_reporter: mpsc::Sender<OutboundConnect>,
    accepted_reporter: mpsc::Sender<InboundConnect>,
}

impl PeerReporter {
    pub async fn report_discovered(&self, addr: SocketAddr, origin: PeerOrigin) -> bool {
        if addr.ip().is_unspecified() || matches!(addr.port(), 0..1024) {
            // invalid address, ignore it
            !self.discovered_reporter.is_closed()
        } else {
            self.discovered_reporter
                .send(OutboundConnect {
                    addr,
                    origin,
                    attempt: 0,
                })
                .await
                .is_ok()
        }
    }

    pub async fn report_accepted_tcp(&self, addr: SocketAddr, stream: TcpStream) -> bool {
        self.accepted_reporter
            .send(InboundConnect {
                addr,
                data: InboundData::Tcp(stream),
            })
            .await
            .is_ok()
    }

    pub async fn report_accepted_utp(
        &self,
        addr: SocketAddr,
        data: utp::InboundConnectData,
    ) -> bool {
        self.accepted_reporter
            .send(InboundConnect {
                addr,
                data: InboundData::Utp(data),
            })
            .await
            .is_ok()
    }
}

#[cfg_attr(test, mockall::automock(type PeerConnection = i32;))]
pub trait PeerConnector {
    type PeerConnection;

    fn max_connections(&self) -> usize;
    fn connect_retry_interval(&self) -> Duration;
    fn max_connect_retries(&self) -> usize;

    fn outbound_connect_and_handshake(
        &self,
        peer_addr: SocketAddr,
        use_pe: bool,
        deadline: Instant,
    ) -> impl Future<Output = io::Result<Self::PeerConnection>>;

    fn outbound_utp_connect_and_handshake(
        &self,
        peer_addr: SocketAddr,
        use_pe: bool,
        deadline: Instant,
    ) -> impl Future<Output = io::Result<Self::PeerConnection>>;

    fn inbound_connect_and_handshake(
        &self,
        peer_addr: SocketAddr,
        deadline: Instant,
        stream: TcpStream,
    ) -> impl Future<Output = io::Result<Self::PeerConnection>>;

    fn inbound_utp_connect_and_handshake(
        &self,
        peer_addr: SocketAddr,
        deadline: Instant,
        data: utp::InboundConnectData,
    ) -> impl Future<Output = io::Result<Self::PeerConnection>>;

    fn run_connection(
        &self,
        origin: PeerOrigin,
        transport: TransportProto,
        connection: Self::PeerConnection,
    ) -> impl Future<Output = io::Result<()>>;
}

pub fn connect_control<C: PeerConnector + 'static>(
    connector_factory: impl FnOnce(&PeerReporter) -> C,
) -> (PeerReporter, ConnectControl<C>) {
    let (discovered_tx, discovered_rx) = mpsc::channel(1);
    let (accepted_tx, accepted_rx) = mpsc::channel(1);

    let reporter = PeerReporter {
        discovered_reporter: discovered_tx.clone(),
        accepted_reporter: accepted_tx,
    };
    let connector = connector_factory(&reporter);
    (
        reporter,
        ConnectControl {
            reconnect_reporter: discovered_tx,
            discovered_peers_receiver: discovered_rx,
            accepted_peers_receiver: accepted_rx,
            recorder: ConnectRecorder::new(512),
            capacity: local_semaphore::Semaphore::new(connector.max_connections()),
            connector: Rc::new(connector),
            child_tasks: TaskScope::new(),
        },
    )
}

pub struct ConnectControl<C: PeerConnector> {
    reconnect_reporter: mpsc::Sender<OutboundConnect>,
    discovered_peers_receiver: mpsc::Receiver<OutboundConnect>,
    accepted_peers_receiver: mpsc::Receiver<InboundConnect>,
    connector: Rc<C>,
    recorder: ConnectRecorder,
    capacity: local_semaphore::Semaphore,
    child_tasks: TaskScope,
}

macro_rules! log {
    ($e:expr, $($arg:tt)+) => {{
        let lvl = if is_fatal_error(&$e) {
            log::Level::Warn
        } else {
            log::Level::Debug
        };
        log::log!(lvl, $($arg)+);
    }}
}

impl<C: PeerConnector + 'static> ConnectControl<C> {
    pub async fn run(mut self) {
        loop {
            let slot = self.capacity.acquire_permit().await;

            select! {
                biased;
                accepted = self.accepted_peers_receiver.recv() => {
                    match accepted {
                        Some(peer) => self.handle_accepted(peer, slot),
                        None => return,
                    }
                }
                discovered = self.discovered_peers_receiver.recv() => {
                    match discovered {
                        Some(peer) => self.handle_discovered(peer, slot),
                        None => return,
                    }
                }
            }
        }
    }

    fn handle_discovered(&mut self, outbound: OutboundConnect, slot: local_semaphore::Permit) {
        if let Some(record) = self.recorder.create_record(outbound.addr, outbound.attempt > 0) {
            let peer_addr = outbound.addr;
            let connector = self.connector.clone();
            let reconnect_reporter = self.reconnect_reporter.clone();
            self.child_tasks.spawn_local(async move {
                if let Err(e) =
                    outgoing_pwp_connection(outbound, &*connector, slot, record, reconnect_reporter)
                        .await
                {
                    log!(e, "Outgoing peer connection to {peer_addr} failed: {e}");
                }
            });
        }
    }

    fn handle_accepted(&mut self, inbound: InboundConnect, slot: local_semaphore::Permit) {
        if let Some(record) = self.recorder.create_record(inbound.addr, true) {
            let peer_addr = inbound.addr;
            let connector = self.connector.clone();
            let reconnect_reporter = self.reconnect_reporter.clone();
            self.child_tasks.spawn_local(async move {
                if let Err(e) =
                    incoming_pwp_connection(inbound, &*connector, slot, record, reconnect_reporter)
                        .await
                {
                    log!(e, "Incoming peer connection from {peer_addr} failed: {e}");
                }
            });
        } else {
            log::error!(
                "Incoming {} peer connection from {} rejected: already connected",
                match inbound.data {
                    InboundData::Tcp(_) => "TCP",
                    InboundData::Utp(_) => "uTP",
                },
                inbound.addr
            );
        }
    }
}

// ------------------------------------------------------------------------------------------------

fn is_fatal_error(e: &io::Error) -> bool {
    match e.kind() {
        io::ErrorKind::ConnectionRefused
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::UnexpectedEof
        | io::ErrorKind::TimedOut
        | io::ErrorKind::Interrupted
        // the 2 below happen when the preliminary connection hasn't exited yet:
        | io::ErrorKind::AddrInUse
        | io::ErrorKind::AddrNotAvailable => false,
        _ => true,
    }
}

fn with_jitter(duration: Duration) -> Duration {
    if cfg!(test) {
        duration
    } else {
        let max_jitter = cmp::min(duration, sec!(1));
        let min = duration - max_jitter;
        let max = duration + max_jitter;
        rand::rng().random_range(min..=max)
    }
}

async fn outgoing_pwp_connection<C: PeerConnector>(
    connect: OutboundConnect,
    connector: &C,
    slot: local_semaphore::Permit,
    record: ConnectRecord,
    reconnect_reporter: mpsc::Sender<OutboundConnect>,
) -> io::Result<()> {
    log::debug!("{connect:?} initiated");
    let connect_deadline = Instant::now() + with_jitter(connector.connect_retry_interval());
    let use_pe = connect.attempt == 0;

    let fatal_error = Cell::new(None);
    let non_fatal_error = Cell::new(None);

    let tcp_connect = async {
        match connector
            .outbound_connect_and_handshake(connect.addr, use_pe, connect_deadline)
            .await
        {
            Ok(connection) => Some(connection),
            Err(e) => {
                if is_fatal_error(&e) {
                    fatal_error.set(Some(e));
                } else {
                    non_fatal_error.set(Some(e));
                }
                None
            }
        }
    };

    let utp_connect = async {
        match connector
            .outbound_utp_connect_and_handshake(connect.addr, use_pe, connect_deadline)
            .await
        {
            Ok(connection) => Some(connection),
            Err(e) => {
                if is_fatal_error(&e) {
                    fatal_error.set(Some(e));
                } else {
                    non_fatal_error.set(Some(e));
                }
                None
            }
        }
    };

    // Try both TCP and uTP connections, prioritizing TCP. If both fail, return the non-fatal error
    // if any, otherwise the fatal error.
    let connect_result = select! {
        biased;
        Some(tcp_connection) = tcp_connect => {
            Ok((tcp_connection, TransportProto::Tcp))
        }
        Some(utp_connection) = utp_connect => {
            Ok((utp_connection, TransportProto::Utp))
        }
        else => {
            if let Some(e) = non_fatal_error.into_inner() {
                Err(e)
            } else if let Some(e) = fatal_error.into_inner() {
                Err(e)
            } else {
                unreachable!("both connection attempts failed without error");
            }
        }
    };

    let (connection, transport) = match connect_result {
        Ok(connection) => connection,
        Err(e) => {
            if connect.attempt < connector.max_connect_retries() && !is_fatal_error(&e) {
                drop(slot);
                time::sleep_until(connect_deadline).await;
                _ = reconnect_reporter
                    .send(OutboundConnect {
                        attempt: connect.attempt + 1,
                        ..connect
                    })
                    .await;
            }
            return Err(e);
        }
    };

    log::debug!("{connect:?} succeeded");
    let connected_time = Instant::now();

    let run_result = connector.run_connection(connect.origin, transport, connection).await;

    // Fatal error means we disconnected the peer intentionally, and
    // <5s since connect means peer probably didn't like our handshake.
    // If none of the above is the case, we'll try to reconnect
    if let Err(e) = &run_result
        && !is_fatal_error(e)
        && connected_time.elapsed() > sec!(5)
    {
        log::warn!("Peer {} disconnected: {e}. Reconnecting in 1s...", connect.addr);
        drop(slot);
        // wait 1 sec for the pwp/utp actor to stop and the remote to receive our RST
        time::sleep(sec!(1)).await;

        drop(record);
        _ = reconnect_reporter
            .send(OutboundConnect {
                attempt: 1,
                ..connect
            })
            .await;
    }
    run_result
}

async fn incoming_pwp_connection<C: PeerConnector>(
    connect: InboundConnect,
    connector: &C,
    slot: local_semaphore::Permit,
    record: ConnectRecord,
    reconnect_reporter: mpsc::Sender<OutboundConnect>,
) -> io::Result<()> {
    log::debug!("{connect:?} accepted");
    let connect_deadline = Instant::now() + with_jitter(connector.connect_retry_interval());

    let (connection, transport) = match connect.data {
        InboundData::Tcp(stream) => (
            connector
                .inbound_connect_and_handshake(connect.addr, connect_deadline, stream)
                .await?,
            TransportProto::Tcp,
        ),
        InboundData::Utp(data) => (
            connector
                .inbound_utp_connect_and_handshake(connect.addr, connect_deadline, data)
                .await?,
            TransportProto::Utp,
        ),
    };

    log::debug!("Inbound connection from {} succeeded", connect.addr);

    let run_result = connector.run_connection(PeerOrigin::Listener, transport, connection).await;

    // Fatal error means we disconnected the peer intentionally
    if let Err(e) = &run_result
        && !is_fatal_error(e)
    {
        log::warn!("Peer {} disconnected: {e}. Reconnecting in 1s...", connect.addr);
        drop(slot);
        // wait 1 sec for the pwp/utp actor to stop and the remote to receive our RST
        time::sleep(sec!(1)).await;

        drop(record);
        _ = reconnect_reporter
            .send(OutboundConnect {
                addr: connect.addr,
                origin: PeerOrigin::Listener,
                attempt: 1,
            })
            .await;
    }
    run_result
}

// ------------------------------------------------------------------------------------------------

#[cfg(test)]
impl PeerReporter {
    pub fn new_mock() -> Self {
        let (discovered_tx, _discovered_rx) = mpsc::channel(1);
        let (accepted_tx, _accepted_rx) = mpsc::channel(1);
        Self {
            discovered_reporter: discovered_tx,
            accepted_reporter: accepted_tx,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::peer::testutils::setup;
    use futures_util::FutureExt;
    use mockall::predicate::eq;
    use rstest::rstest;
    use rstest_reuse::{self, *};
    use std::future::pending;
    use std::net::Ipv4Addr;
    use std::sync::{Arc, Mutex};
    use tokio::sync::oneshot;
    use tokio::task;
    use tokio::time::{sleep, sleep_until};

    fn addr(i: u16) -> SocketAddr {
        (Ipv4Addr::LOCALHOST, 1024 + i).into()
    }

    #[template]
    #[rstest]
    fn for_retriable_errors(
        #[values(
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::UnexpectedEof,
            io::ErrorKind::TimedOut,
            io::ErrorKind::AddrInUse,
            io::ErrorKind::AddrNotAvailable,
            io::ErrorKind::Interrupted
        )]
        error_kind: io::ErrorKind,
    ) {
    }

    #[template]
    #[rstest]
    fn for_fatal_errors(
        #[values(
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::AlreadyExists,
            io::ErrorKind::InvalidInput,
            io::ErrorKind::InvalidData,
            io::ErrorKind::Other
        )]
        error_kind: io::ErrorKind,
    ) {
    }

    #[apply(for_retriable_errors)]
    #[tokio::test(flavor = "local", start_paused = true)]
    async fn test_outbound_connect_retry_on_error(error_kind: io::ErrorKind) {
        setup(false);

        let start_time = Instant::now();
        let peer_addr = addr(1);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |&addr, &use_pe, _deadline| {
                addr == peer_addr && Instant::now() == start_time && use_pe
            })
            .returning(move |_, _, _deadline| {
                async move { Err(io::Error::from(error_kind)) }.boxed()
            });
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |&addr, &use_pe, _deadline| {
                addr == peer_addr && Instant::now() == start_time + sec!(10) && !use_pe
            })
            .returning(move |_, _, _deadline| {
                async move { Err(io::Error::from(error_kind)) }.boxed()
            });
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |&addr, &use_pe, _deadline| {
                addr == peer_addr && Instant::now() == start_time + sec!(20) && !use_pe
            })
            .returning(move |_, _, _deadline| {
                async move { Err(io::Error::from(error_kind)) }.boxed()
            });

        let (reporter, ctrl) = connect_control(move |_| connector);
        task::spawn_local(ctrl.run());

        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
        sleep(sec!(1)).await;
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Dht).await);
        sleep(sec!(1)).await;
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Pex).await);

        sleep(Duration::MAX).await;
    }

    #[tokio::test(flavor = "local", start_paused = true)]
    async fn test_outbound_connect_retry_on_timeout() {
        setup(false);

        let start_time = Instant::now();
        let peer_addr = addr(1);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |&addr, &use_pe, _deadline| {
                addr == peer_addr && Instant::now() == start_time && use_pe
            })
            .returning(move |_, _, deadline| {
                async move {
                    sleep_until(deadline).await;
                    Err(io::Error::from(io::ErrorKind::TimedOut))
                }
                .boxed()
            });
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |&addr, &use_pe, _deadline| {
                addr == peer_addr && Instant::now() == start_time + sec!(10) && !use_pe
            })
            .returning(move |_, _, deadline| {
                async move {
                    sleep_until(deadline).await;
                    Err(io::Error::from(io::ErrorKind::TimedOut))
                }
                .boxed()
            });
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |&addr, &use_pe, _deadline| {
                addr == peer_addr && Instant::now() == start_time + sec!(20) && !use_pe
            })
            .returning(move |_, _, deadline| {
                async move {
                    sleep_until(deadline).await;
                    Err(io::Error::from(io::ErrorKind::TimedOut))
                }
                .boxed()
            });

        let (reporter, ctrl) = connect_control(move |_| connector);
        task::spawn_local(ctrl.run());

        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
        sleep(sec!(1)).await;
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Dht).await);
        sleep(sec!(1)).await;
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Pex).await);

        sleep(Duration::MAX).await;
    }

    #[tokio::test(flavor = "local", start_paused = true)]
    async fn test_successful_connect_after_retry() {
        setup(false);

        let start_time = Instant::now();
        let peer_addr = addr(1);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |&addr, &use_pe, _deadline| {
                addr == peer_addr && Instant::now() == start_time && use_pe
            })
            .returning(move |_, _, deadline| {
                async move {
                    sleep_until(deadline).await;
                    Err(io::Error::from(io::ErrorKind::TimedOut))
                }
                .boxed()
            });
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |&addr, &use_pe, _deadline| {
                addr == peer_addr && Instant::now() == start_time + sec!(10) && !use_pe
            })
            .returning(move |_, _, deadline| {
                async move {
                    sleep_until(deadline).await;
                    Ok(43)
                }
                .boxed()
            });
        connector
            .expect_run_connection()
            .once()
            .withf(move |origin, transport, c| {
                *origin == PeerOrigin::Tracker
                    && *transport == TransportProto::Tcp
                    && *c == 43
                    && Instant::now() == start_time + sec!(20)
            })
            .returning(|_, _, _| pending::<io::Result<()>>().boxed());

        let (reporter, ctrl) = connect_control(move |_| connector);
        task::spawn_local(ctrl.run());

        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
        sleep(sec!(1)).await;
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Dht).await);
        sleep(sec!(1)).await;
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Pex).await);

        sleep(Duration::MAX).await;
    }

    #[apply(for_fatal_errors)]
    #[tokio::test(flavor = "local", start_paused = true)]
    async fn test_outbound_connect_no_retry(error_kind: io::ErrorKind) {
        setup(false);

        let start_time = Instant::now();
        let peer_addr = addr(1);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);

        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |&addr, &use_pe, _deadline| {
                addr == peer_addr && Instant::now() == start_time && use_pe
            })
            .returning(move |_, _, _deadline| {
                async move { Err(io::Error::from(error_kind)) }.boxed()
            });

        connector
            .expect_outbound_utp_connect_and_handshake()
            .once()
            .withf(move |&addr, &use_pe, _deadline| {
                addr == peer_addr && Instant::now() == start_time && use_pe
            })
            .returning(move |_, _, _deadline| {
                async move { Err(io::Error::from(error_kind)) }.boxed()
            });

        let (reporter, ctrl) = connect_control(move |_| connector);
        task::spawn_local(ctrl.run());

        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
        sleep(sec!(1)).await;
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Dht).await);
        sleep(sec!(1)).await;
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Pex).await);
    }

    #[apply(for_retriable_errors)]
    #[tokio::test(flavor = "local", start_paused = true)]
    async fn test_run_connection_reconnect(error_kind: io::ErrorKind) {
        setup(false);

        let peer_addr = addr(1);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        connector
            .expect_outbound_connect_and_handshake()
            .times(9)
            .returning(|_, _, _| async move { Ok(42) }.boxed());
        connector
            .expect_run_connection()
            .times(9)
            .with(eq(PeerOrigin::Tracker), eq(TransportProto::Tcp), eq(42))
            .returning(move |_, _, _| {
                async move {
                    sleep(sec!(6)).await;
                    Err(io::Error::from(error_kind))
                }
                .boxed()
            });

        let (reporter, ctrl) = connect_control(move |_| connector);
        task::spawn_local(ctrl.run());

        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Dht).await);
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Pex).await);
        sleep(sec!(60)).await;
        task::yield_now().await;
        drop(reporter);
    }

    #[apply(for_fatal_errors)]
    #[tokio::test(flavor = "local", start_paused = true)]
    async fn test_run_connection_no_reconnect_on_fatal_error(error_kind: io::ErrorKind) {
        setup(false);

        let peer_addr = addr(1);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .returning(|_, _, _| async move { Ok(42) }.boxed());
        connector
            .expect_run_connection()
            .once()
            .with(eq(PeerOrigin::Tracker), eq(TransportProto::Tcp), eq(42))
            .returning(move |_, _, _| {
                async move {
                    sleep(sec!(20)).await;
                    Err(io::Error::from(error_kind))
                }
                .boxed()
            });

        let (reporter, ctrl) = connect_control(move |_| connector);
        task::spawn_local(ctrl.run());

        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
        sleep(sec!(1)).await;
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Dht).await);
        sleep(sec!(1)).await;
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Pex).await);
    }

    #[tokio::test(flavor = "local", start_paused = true)]
    async fn test_run_connection_no_reconnect_if_short_lived() {
        setup(false);

        let peer_addr = addr(1);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .returning(|_, _, _| async move { Ok(42) }.boxed());
        connector
            .expect_run_connection()
            .once()
            .with(eq(PeerOrigin::Tracker), eq(TransportProto::Tcp), eq(42))
            .returning(move |_, _, _| {
                async move {
                    sleep(sec!(5)).await;
                    Err(io::Error::from(io::ErrorKind::UnexpectedEof))
                }
                .boxed()
            });

        let (reporter, ctrl) = connect_control(move |_| connector);
        task::spawn_local(ctrl.run());

        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
        sleep(sec!(1)).await;
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Dht).await);
        sleep(sec!(1)).await;
        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Pex).await);
    }

    #[tokio::test(flavor = "local", start_paused = true)]
    async fn test_run_concurrent_connections_up_to_capacity() {
        setup(false);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        for port in 1..=100 {
            let addr = addr(port);

            connector
                .expect_outbound_connect_and_handshake()
                .once()
                .with(eq(addr), eq(true), mockall::predicate::always())
                .returning(move |_, _, _deadline| async move { Ok(port as i32) }.boxed());
            connector
                .expect_run_connection()
                .once()
                .with(eq(PeerOrigin::Dht), eq(TransportProto::Tcp), eq(port as i32))
                .returning(|_, _, _| pending::<io::Result<()>>().boxed());
        }

        let (reporter, ctrl) = connect_control(move |_| connector);
        task::spawn_local(ctrl.run());

        for port in 1..=100 {
            println!("reporting discovered peer {}", port);
            let peer_addr = addr(port);
            task::yield_now().await;
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Dht).now_or_never().unwrap());
            task::yield_now().await;
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Pex).now_or_never().unwrap());
        }
        task::yield_now().await;
        assert!(
            reporter
                .report_discovered(addr(12345), PeerOrigin::Pex)
                .now_or_never()
                .is_none()
        );
    }

    #[tokio::test(flavor = "local", start_paused = true)]
    async fn test_release_capacity_when_retrying() {
        setup(false);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(1usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        for port in 1..=100 {
            let addr = addr(port);
            connector
                .expect_outbound_connect_and_handshake()
                .once()
                .with(eq(addr), eq(true), mockall::predicate::always())
                .returning(move |_, _, _deadline| {
                    async move { Err(io::ErrorKind::AddrNotAvailable.into()) }.boxed()
                });
        }

        let (reporter, ctrl) = connect_control(move |_| connector);
        task::spawn_local(ctrl.run());

        for port in 1..=100 {
            let peer_addr = addr(port);
            task::yield_now().await;
            assert!(
                reporter
                    .report_discovered(peer_addr, PeerOrigin::Tracker)
                    .now_or_never()
                    .unwrap()
            );
            task::yield_now().await;
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Dht).now_or_never().unwrap());
            task::yield_now().await;
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Pex).now_or_never().unwrap());
        }
        drop(reporter);
    }

    #[tokio::test(flavor = "local", start_paused = true)]
    async fn test_outbound_connect_explicit_cancellation() {
        setup(false);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        let token = Arc::new(());
        let peer_addr = addr(42);
        let token_clone = token.clone();
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |addr, &use_pe, _deadline| addr == &peer_addr && use_pe)
            .return_once(move |_, _, _deadline| {
                async move {
                    let _token = token_clone;
                    pending::<io::Result<i32>>().await
                }
                .boxed()
            });

        let mut canceller = task::JoinSet::new();
        let (reporter, ctrl) = connect_control(move |_| connector);
        canceller.spawn_local(ctrl.run());

        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
        task::yield_now().await;
        assert_eq!(Arc::strong_count(&token), 2);

        canceller.abort_all();
        task::yield_now().await;
        assert_eq!(Arc::strong_count(&token), 1);
    }

    #[tokio::test(flavor = "local", start_paused = true)]
    async fn test_outbound_connect_implicit_cancellation() {
        setup(false);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        let token = Arc::new(());
        let peer_addr = addr(42);
        let token_clone = token.clone();
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |addr, &use_pe, _deadline| addr == &peer_addr && use_pe)
            .return_once(move |_, _, _deadline| {
                async move {
                    let _token = token_clone;
                    pending::<io::Result<i32>>().await
                }
                .boxed()
            });

        let (reporter, ctrl) = connect_control(move |_| connector);
        let run_task = task::spawn_local(ctrl.run());

        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
        task::yield_now().await;
        assert_eq!(Arc::strong_count(&token), 2);

        drop(reporter);
        task::yield_now().await;
        assert!(run_task.is_finished());
        assert_eq!(Arc::strong_count(&token), 1);
    }

    #[tokio::test(flavor = "local", start_paused = true)]
    async fn test_run_connection_explicit_cancellation() {
        setup(false);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        let peer_addr = addr(123);
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |addr, &use_pe, _deadline| addr == &peer_addr && use_pe)
            .returning(move |_, _, _| async move { Ok(42) }.boxed());

        let token = Arc::new(());
        let token_clone = token.clone();
        connector
            .expect_run_connection()
            .once()
            .with(eq(PeerOrigin::Tracker), eq(TransportProto::Tcp), eq(42))
            .return_once(move |_, _, _| {
                async move {
                    let _token = token_clone;
                    pending::<io::Result<()>>().await
                }
                .boxed()
            });

        let mut canceller = task::JoinSet::new();
        let (reporter, ctrl) = connect_control(move |_| connector);
        canceller.spawn_local(ctrl.run());

        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
        task::yield_now().await;
        assert_eq!(Arc::strong_count(&token), 2);

        canceller.abort_all();
        task::yield_now().await;
        assert_eq!(Arc::strong_count(&token), 1);
    }

    #[tokio::test(flavor = "local", start_paused = true)]
    async fn test_run_connection_implicit_cancellation() {
        setup(false);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        let peer_addr = addr(123);
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |addr, &use_pe, _deadline| addr == &peer_addr && use_pe)
            .returning(move |_, _, _| async move { Ok(42) }.boxed());

        let token = Arc::new(());
        let token_clone = token.clone();
        connector
            .expect_run_connection()
            .once()
            .with(eq(PeerOrigin::Tracker), eq(TransportProto::Tcp), eq(42))
            .return_once(move |_, _, _| {
                async move {
                    let _token = token_clone;
                    pending::<io::Result<()>>().await
                }
                .boxed()
            });

        let (reporter, ctrl) = connect_control(move |_| connector);
        let run_task = task::spawn_local(ctrl.run());

        assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
        task::yield_now().await;
        assert_eq!(Arc::strong_count(&token), 2);

        drop(reporter);
        task::yield_now().await;
        assert!(run_task.is_finished());
        assert_eq!(Arc::strong_count(&token), 1);
    }

    #[tokio::test(flavor = "local")]
    async fn test_peer_reporter_filters_out_invalid_peer_addrs() {
        setup(false);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().once().return_const(100usize);

        let (reporter, ctrl) = connect_control(move |_| connector);
        task::spawn_local(ctrl.run());

        let invalid_ip_addrs = [SocketAddr::from(([0, 0, 0, 0], 6881))];
        let invalid_port_addrs = (0..1024).map(|port| SocketAddr::from(([1, 2, 3, 4], port)));

        for peer_addr in invalid_ip_addrs {
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
            task::yield_now().await;
        }

        for peer_addr in invalid_port_addrs {
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
            task::yield_now().await;
        }
    }

    #[tokio::test(flavor = "local")]
    async fn test_prioritize_inbound_connections_over_outbound() {
        setup(false);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().once().return_const(1usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);

        let initial_peer_addr = addr(5000);
        let discovered_addr = addr(5001);
        let accepted_addr = addr(5002);

        let (initial_peer_exit, initial_exit_receiver) = oneshot::channel::<()>();
        let initial_exit_receiver = Arc::new(Mutex::new(Some(initial_exit_receiver)));

        let accepted_data = utp::InboundConnectData::new_mock();

        // initial peer expectations
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .with(eq(initial_peer_addr), eq(true), mockall::predicate::always())
            .returning(move |_, _, _deadline| async move { Ok(42) }.boxed());
        connector
            .expect_run_connection()
            .once()
            .with(eq(PeerOrigin::Tracker), eq(TransportProto::Tcp), eq(42))
            .returning(move |_, _, _| {
                let receiver = initial_exit_receiver.clone();
                async move {
                    let receiver = receiver.lock().unwrap().take().unwrap();
                    _ = receiver.await;
                    Ok(())
                }
                .boxed()
            });

        // accepted peer expectations
        connector
            .expect_inbound_utp_connect_and_handshake()
            .once()
            .with(eq(accepted_addr), mockall::predicate::always(), eq(accepted_data.clone()))
            .returning(move |_, _deadline, _stream| async move { Ok(43) }.boxed());
        connector
            .expect_run_connection()
            .once()
            .with(eq(PeerOrigin::Listener), eq(TransportProto::Utp), eq(43))
            .returning(|_, _, _| pending::<io::Result<()>>().boxed());

        let (reporter, ctrl) = connect_control(move |_| connector);
        task::spawn_local(ctrl.run());

        // initial peer
        assert!(reporter.report_discovered(initial_peer_addr, PeerOrigin::Tracker).await);
        task::yield_now().await;

        // new discovered peer (should be ignored)
        assert!(reporter.report_discovered(discovered_addr, PeerOrigin::Dht).await);
        task::yield_now().await;

        // accepted peer (should be prioritized)
        assert!(reporter.report_accepted_utp(accepted_addr, accepted_data).await);
        task::yield_now().await;

        // signal initial peer to exit
        initial_peer_exit.send(()).unwrap();
        task::yield_now().await;
    }
}
