use derive_more::Debug;
use local_async_utils::prelude::*;
use mtorrent_core::pwp::PeerOrigin;
use mtorrent_core::utp;
use mtorrent_utils::connect_throttle::{ConnectPermit, ConnectThrottle};
use rand::RngExt;
use std::cell::Cell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use std::{cmp, io};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};
use tokio::{select, task};
use tokio_util::sync::CancellationToken;

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
        self.discovered_reporter
            .send(OutboundConnect {
                addr,
                origin,
                attempt: 0,
            })
            .await
            .is_ok()
    }

    pub async fn report_accepted(&self, addr: SocketAddr, stream: TcpStream) -> bool {
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
        deadline: Instant,
    ) -> impl Future<Output = io::Result<Self::PeerConnection>>;

    fn outbound_utp_connect_and_handshake(
        &self,
        peer_addr: SocketAddr,
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
            throttle: ConnectThrottle::new(connector.max_connections(), 512),
            connector: Rc::new(connector),
            canceller: CancellationToken::new(),
        },
    )
}

pub struct ConnectControl<C: PeerConnector> {
    reconnect_reporter: mpsc::Sender<OutboundConnect>,
    discovered_peers_receiver: mpsc::Receiver<OutboundConnect>,
    accepted_peers_receiver: mpsc::Receiver<InboundConnect>,
    connector: Rc<C>,
    throttle: ConnectThrottle,
    canceller: CancellationToken,
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
        let _auto_cancel = self.canceller.clone().drop_guard();
        loop {
            select! {
                biased;
                accepted = self.accepted_peers_receiver.recv() => {
                    match accepted {
                        Some(peer) => self.handle_accepted(peer),
                        None => return,
                    }
                }
                discovered = self.discovered_peers_receiver.recv() => {
                    match discovered {
                        Some(peer) => self.handle_discovered(peer).await,
                        None => return,
                    }
                }
            }
        }
    }

    async fn handle_discovered(&mut self, outbound: OutboundConnect) {
        if let Some(permit) =
            self.throttle.permit_for_outbound(outbound.addr, outbound.attempt > 0).await
        {
            let peer_addr = outbound.addr;
            let connector = self.connector.clone();
            let canceller = self.canceller.clone();
            let reconnect_reporter = self.reconnect_reporter.clone();
            task::spawn_local(async move {
                let result = canceller
                    .run_until_cancelled(outgoing_pwp_connection(
                        outbound,
                        &*connector,
                        permit,
                        reconnect_reporter,
                    ))
                    .await;
                if let Some(Err(e)) = result {
                    log!(e, "Outgoing peer connection to {peer_addr} failed: {e}");
                }
            });
        }
    }

    fn handle_accepted(&mut self, inbound: InboundConnect) {
        if let Some(permit) = self.throttle.permit_for_inbound(inbound.addr) {
            let peer_addr = inbound.addr;
            let connector = self.connector.clone();
            let canceller = self.canceller.clone();
            let reconnect_reporter = self.reconnect_reporter.clone();
            task::spawn_local(async move {
                let result = canceller
                    .run_until_cancelled(incoming_pwp_connection(
                        inbound,
                        &*connector,
                        permit,
                        reconnect_reporter,
                    ))
                    .await;
                if let Some(Err(e)) = result {
                    log!(e, "Incoming peer connection from {peer_addr} failed: {e}");
                }
            });
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
    mut permit: ConnectPermit,
    reconnect_reporter: mpsc::Sender<OutboundConnect>,
) -> io::Result<()> {
    log::debug!("{connect:?} initiated");
    let connect_deadline = Instant::now() + with_jitter(connector.connect_retry_interval());

    let fatal_error = Cell::new(None);
    let non_fatal_error = Cell::new(None);

    let tcp_connect = async {
        match connector.outbound_connect_and_handshake(connect.addr, connect_deadline).await {
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
            .outbound_utp_connect_and_handshake(connect.addr, connect_deadline)
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

    // Try both TCP and uTP connections, prioritizing TCP. If both fail, return the non-fatal error if any,
    // otherwise the fatal error.
    let connect_result = select! {
        biased;
        Some(tcp_connection) = tcp_connect => {
            Ok(tcp_connection)
        }
        Some(utp_connection) = utp_connect => {
            Ok(utp_connection)
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

    let connection = match connect_result {
        Ok(connection) => connection,
        Err(e) => {
            if connect.attempt < connector.max_connect_retries() && !is_fatal_error(&e) {
                permit.release_slot();
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

    let run_result = connector.run_connection(connect.origin, connection).await;

    // Fatal error means we disconnected the peer intentionally, and
    // <5s since connect means peer probably didn't like our handshake.
    // If none of the above is the case, we'll try to reconnect
    if let Err(e) = &run_result
        && !is_fatal_error(e)
        && connected_time.elapsed() > sec!(5)
    {
        log::warn!("Peer {} disconnected: {e}. Reconnecting in 1s...", connect.addr);
        permit.release_slot();
        // wait 1 sec for ConnectionIoDriver to stop and the remote to receive our RST
        time::sleep(sec!(1)).await;
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
    mut permit: ConnectPermit,
    reconnect_reporter: mpsc::Sender<OutboundConnect>,
) -> io::Result<()> {
    log::debug!("{connect:?} accepted");
    let connect_deadline = Instant::now() + with_jitter(connector.connect_retry_interval());

    let connection = match connect.data {
        InboundData::Tcp(stream) => {
            connector
                .inbound_connect_and_handshake(connect.addr, connect_deadline, stream)
                .await?
        }
        InboundData::Utp(data) => {
            connector
                .inbound_utp_connect_and_handshake(connect.addr, connect_deadline, data)
                .await?
        }
    };

    log::debug!("Inbound connection from {} succeeded", connect.addr);

    let run_result = connector.run_connection(PeerOrigin::Listener, connection).await;

    // Fatal error means we disconnected the peer intentionally
    if let Err(e) = &run_result
        && !is_fatal_error(e)
    {
        log::warn!("Peer {} disconnected: {e}. Reconnecting in 1s...", connect.addr);
        permit.release_slot();
        // wait 1 sec for ConnectionIoDriver to stop and the remote to receive our RST
        time::sleep(sec!(1)).await;

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
    use futures_util::FutureExt;
    use mockall::predicate::eq;
    use rstest::rstest;
    use rstest_reuse::{self, *};
    use std::future::pending;
    use std::net::Ipv4Addr;
    use std::sync::Arc;
    use tokio::runtime::UnhandledPanic;
    use tokio::time::{sleep, sleep_until};

    fn addr(port: u16) -> SocketAddr {
        (Ipv4Addr::LOCALHOST, port).into()
    }

    /// ```no_run
    /// run_in_local_set! { }
    /// ```
    macro_rules! run_in_local_set {
        ($($tokens:tt)+) => {{
            task::LocalSet::new()
                .unhandled_panic(UnhandledPanic::ShutdownRuntime)
                .run_until(async move {
                    $($tokens)+

                    sleep(Duration::MAX).await;
                }).await;
        }};
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
    #[tokio::test(start_paused = true)]
    async fn test_outbound_connect_retry_on_error(error_kind: io::ErrorKind) {
        let start_time = Instant::now();
        let peer_addr = addr(1);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |&addr, _deadline| addr == peer_addr && Instant::now() == start_time)
            .returning(move |_, _deadline| async move { Err(io::Error::from(error_kind)) }.boxed());
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |&addr, _deadline| {
                addr == peer_addr && Instant::now() == start_time + sec!(10)
            })
            .returning(move |_, _deadline| async move { Err(io::Error::from(error_kind)) }.boxed());
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |&addr, _deadline| {
                addr == peer_addr && Instant::now() == start_time + sec!(20)
            })
            .returning(move |_, _deadline| async move { Err(io::Error::from(error_kind)) }.boxed());

        run_in_local_set! {
            let (reporter, ctrl) = connect_control(move |_| connector);
            task::spawn_local(ctrl.run());

            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
            sleep(sec!(1)).await;
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Dht).await);
            sleep(sec!(1)).await;
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Pex).await);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_outbound_connect_retry_on_timeout() {
        let start_time = Instant::now();
        let peer_addr = addr(1);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |&addr, _deadline| addr == peer_addr && Instant::now() == start_time)
            .returning(move |_, deadline| {
                async move {
                    sleep_until(deadline).await;
                    Err(io::Error::from(io::ErrorKind::TimedOut))
                }
                .boxed()
            });
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |&addr, _deadline| {
                addr == peer_addr && Instant::now() == start_time + sec!(10)
            })
            .returning(move |_, deadline| {
                async move {
                    sleep_until(deadline).await;
                    Err(io::Error::from(io::ErrorKind::TimedOut))
                }
                .boxed()
            });
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |&addr, _deadline| {
                addr == peer_addr && Instant::now() == start_time + sec!(20)
            })
            .returning(move |_, deadline| {
                async move {
                    sleep_until(deadline).await;
                    Err(io::Error::from(io::ErrorKind::TimedOut))
                }
                .boxed()
            });

        run_in_local_set! {
            let (reporter, ctrl) = connect_control(move |_| connector);
            task::spawn_local(ctrl.run());

            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
            sleep(sec!(1)).await;
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Dht).await);
            sleep(sec!(1)).await;
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Pex).await);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_successful_connect_after_retry() {
        let start_time = Instant::now();
        let peer_addr = addr(1);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |&addr, _deadline| addr == peer_addr && Instant::now() == start_time)
            .returning(move |_, deadline| {
                async move {
                    sleep_until(deadline).await;
                    Err(io::Error::from(io::ErrorKind::TimedOut))
                }
                .boxed()
            });
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |&addr, _deadline| {
                addr == peer_addr && Instant::now() == start_time + sec!(10)
            })
            .returning(move |_, deadline| {
                async move {
                    sleep_until(deadline).await;
                    Ok(43)
                }
                .boxed()
            });
        connector
            .expect_run_connection()
            .once()
            .withf(move |origin, c| {
                *origin == PeerOrigin::Tracker
                    && *c == 43
                    && Instant::now() == start_time + sec!(20)
            })
            .returning(|_, _| pending::<io::Result<()>>().boxed());

        run_in_local_set! {
            let (reporter, ctrl) = connect_control(move |_| connector);
            task::spawn_local(ctrl.run());

            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
            sleep(sec!(1)).await;
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Dht).await);
            sleep(sec!(1)).await;
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Pex).await);
        }
    }

    #[apply(for_fatal_errors)]
    #[tokio::test(start_paused = true)]
    async fn test_outbound_connect_no_retry(error_kind: io::ErrorKind) {
        run_in_local_set! {
            let start_time = Instant::now();
            let peer_addr = addr(1);

            let mut connector = MockPeerConnector::new();
            connector.expect_max_connections().return_const(100usize);
            connector.expect_connect_retry_interval().return_const(sec!(10));
            connector.expect_max_connect_retries().return_const(2usize);

            connector
                .expect_outbound_connect_and_handshake()
                .once()
                .withf(move |&addr, _deadline| addr == peer_addr && Instant::now() == start_time)
                .returning(move |_, _deadline| {
                    async move { Err(io::Error::from(error_kind)) }.boxed()
                });

            connector
                .expect_outbound_utp_connect_and_handshake()
                .once()
                .withf(move |&addr, _deadline| addr == peer_addr && Instant::now() == start_time)
                .returning(move |_, _deadline| {
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
    }

    #[apply(for_retriable_errors)]
    #[tokio::test(start_paused = true)]
    async fn test_run_connection_reconnect(error_kind: io::ErrorKind) {
        run_in_local_set! {
            let peer_addr = addr(1);

            let mut connector = MockPeerConnector::new();
            connector.expect_max_connections().return_const(100usize);
            connector.expect_connect_retry_interval().return_const(sec!(10));
            connector.expect_max_connect_retries().return_const(2usize);
            connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _| {
                std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
            });

            connector
                .expect_outbound_connect_and_handshake()
                .times(9)
                .returning(|_, _deadline| {
                    async move { Ok(42) }.boxed()
                });
            connector
                .expect_run_connection()
                .times(9)
                .with(eq(PeerOrigin::Tracker), eq(42))
                .returning(move |_, _| {
                    async move {
                        sleep(sec!(6)).await;
                        Err(io::Error::from(error_kind))
                    }.boxed()
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
    }

    #[apply(for_fatal_errors)]
    #[tokio::test(start_paused = true)]
    async fn test_run_connection_no_reconnect_on_fatal_error(error_kind: io::ErrorKind) {
        let peer_addr = addr(1);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .returning(|_, _| async move { Ok(42) }.boxed());
        connector
            .expect_run_connection()
            .once()
            .with(eq(PeerOrigin::Tracker), eq(42))
            .returning(move |_, _| {
                async move {
                    sleep(sec!(20)).await;
                    Err(io::Error::from(error_kind))
                }
                .boxed()
            });

        run_in_local_set! {
            let (reporter, ctrl) = connect_control(move |_| connector);
            task::spawn_local(ctrl.run());

            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
            sleep(sec!(1)).await;
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Dht).await);
            sleep(sec!(1)).await;
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Pex).await);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_run_connection_no_reconnect_if_short_lived() {
        let peer_addr = addr(1);

        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .returning(|_, _| async move { Ok(42) }.boxed());
        connector
            .expect_run_connection()
            .once()
            .with(eq(PeerOrigin::Tracker), eq(42))
            .returning(move |_, _| {
                async move {
                    sleep(sec!(5)).await;
                    Err(io::Error::from(io::ErrorKind::UnexpectedEof))
                }
                .boxed()
            });

        run_in_local_set! {
            let (reporter, ctrl) = connect_control(move |_| connector);
            task::spawn_local(ctrl.run());

            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
            sleep(sec!(1)).await;
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Dht).await);
            sleep(sec!(1)).await;
            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Pex).await);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_run_concurrent_connections_up_to_capacity() {
        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        for port in 1..=100 {
            let addr = addr(port);

            connector
                .expect_outbound_connect_and_handshake()
                .once()
                .with(eq(addr), mockall::predicate::always())
                .returning(move |_, _deadline| async move { Ok(port as i32) }.boxed());
            connector
                .expect_run_connection()
                .once()
                .with(eq(PeerOrigin::Tracker), eq(port as i32))
                .returning(|_, _| pending::<io::Result<()>>().boxed());
        }

        run_in_local_set! {
            let (reporter, ctrl) = connect_control(move |_| connector);
            task::spawn_local(ctrl.run());

            for port in 1..=100 {
                let peer_addr = addr(port);
                task::yield_now().await;
                assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).now_or_never().unwrap());
                task::yield_now().await;
                assert!(reporter.report_discovered(peer_addr, PeerOrigin::Dht).now_or_never().unwrap());
                task::yield_now().await;
                assert!(reporter.report_discovered(peer_addr, PeerOrigin::Pex).now_or_never().unwrap());
            }
            task::yield_now().await;
            assert!(reporter.report_discovered(addr(12345), PeerOrigin::Pex).now_or_never().is_none());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_release_capacity_when_retrying() {
        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(1usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        for port in 1..=100 {
            let addr = addr(port);
            connector
                .expect_outbound_connect_and_handshake()
                .once()
                .with(eq(addr), mockall::predicate::always())
                .returning(move |_, _deadline| {
                    async move { Err(io::ErrorKind::AddrNotAvailable.into()) }.boxed()
                });
        }

        run_in_local_set! {
            let (reporter, ctrl) = connect_control(move |_| connector);
            task::spawn_local(ctrl.run());

            for port in 1..=100 {
                let peer_addr = addr(port);
                task::yield_now().await;
                assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).now_or_never().unwrap());
                task::yield_now().await;
                assert!(reporter.report_discovered(peer_addr, PeerOrigin::Dht).now_or_never().unwrap());
                task::yield_now().await;
                assert!(reporter.report_discovered(peer_addr, PeerOrigin::Pex).now_or_never().unwrap());
            }
            drop(reporter);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_outbound_connect_explicit_cancellation() {
        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        let token = Arc::new(());
        let peer_addr = addr(42);
        let token_clone = token.clone();
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |addr, _deadline| addr == &peer_addr)
            .return_once(move |_, _deadline| {
                async move {
                    let _token = token_clone;
                    pending::<io::Result<i32>>().await
                }
                .boxed()
            });

        run_in_local_set! {
            let canceller = CancellationToken::new();
            let (reporter, ctrl) = connect_control(move |_| connector);
            task::spawn_local(canceller.clone().run_until_cancelled_owned(ctrl.run()));

            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
            task::yield_now().await;
            assert_eq!(Arc::strong_count(&token), 2);

            canceller.cancel();
            task::yield_now().await;
            assert_eq!(Arc::strong_count(&token), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_outbound_connect_implicit_cancellation() {
        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        let token = Arc::new(());
        let peer_addr = addr(42);
        let token_clone = token.clone();
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |addr, _deadline| addr == &peer_addr)
            .return_once(move |_, _deadline| {
                async move {
                    let _token = token_clone;
                    pending::<io::Result<i32>>().await
                }
                .boxed()
            });

        run_in_local_set! {
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
    }

    #[tokio::test(start_paused = true)]
    async fn test_run_connection_explicit_cancellation() {
        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        let peer_addr = addr(123);
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |addr, _deadline| addr == &peer_addr)
            .returning(move |_, _| async move { Ok(42) }.boxed());

        let token = Arc::new(());
        let token_clone = token.clone();
        connector
            .expect_run_connection()
            .once()
            .with(eq(PeerOrigin::Tracker), eq(42))
            .return_once(move |_, _| {
                async move {
                    let _token = token_clone;
                    pending::<io::Result<()>>().await
                }
                .boxed()
            });

        run_in_local_set! {
            let canceller = CancellationToken::new();
            let (reporter, ctrl) = connect_control(move |_| connector);
            task::spawn_local(canceller.clone().run_until_cancelled_owned(ctrl.run()));

            assert!(reporter.report_discovered(peer_addr, PeerOrigin::Tracker).await);
            task::yield_now().await;
            assert_eq!(Arc::strong_count(&token), 2);

            canceller.cancel();
            task::yield_now().await;
            assert_eq!(Arc::strong_count(&token), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_run_connection_implicit_cancellation() {
        let mut connector = MockPeerConnector::new();
        connector.expect_max_connections().return_const(100usize);
        connector.expect_connect_retry_interval().return_const(sec!(10));
        connector.expect_max_connect_retries().return_const(2usize);
        connector.expect_outbound_utp_connect_and_handshake().returning(move |_, _| {
            std::future::ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))).boxed()
        });

        let peer_addr = addr(123);
        connector
            .expect_outbound_connect_and_handshake()
            .once()
            .withf(move |addr, _deadline| addr == &peer_addr)
            .returning(move |_, _| async move { Ok(42) }.boxed());

        let token = Arc::new(());
        let token_clone = token.clone();
        connector
            .expect_run_connection()
            .once()
            .with(eq(PeerOrigin::Tracker), eq(42))
            .return_once(move |_, _| {
                async move {
                    let _token = token_clone;
                    pending::<io::Result<()>>().await
                }
                .boxed()
            });

        run_in_local_set! {
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
    }
}
