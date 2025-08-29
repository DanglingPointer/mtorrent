use derive_more::Debug;
use futures_util::StreamExt;
use local_async_utils::prelude::*;
use mtorrent_core::pwp::{DownloadChannels, ExtendedChannels, PeerOrigin, UploadChannels};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;
use std::{cmp, io};
use tokio::task;
use tokio::time::{self, Instant};
use tokio::{net::TcpStream, select, sync::mpsc};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq, Hash))]
pub struct DiscoveredPeer {
    pub addr: SocketAddr,
    pub origin: PeerOrigin,
    pub reconnects_left: Option<usize>,
}

#[derive(Debug)]
pub struct AcceptedPeer {
    pub addr: SocketAddr,
    pub stream: TcpStream,
}

pub trait ConnectHandler {
    fn max_connections(&self) -> usize;
    fn handle_discovered_peer(&mut self, peer: DiscoveredPeer, permit: ConnectPermit);
    fn handle_accepted_peer(&mut self, peer: AcceptedPeer, permit: ConnectPermit);
}

pub fn connect_control<H: ConnectHandler + 'static>(
    handler_factory: impl FnOnce(&PeerReporter) -> H,
) -> (PeerReporter, ConnectThrottle<H>) {
    let (discovered_tx, discovered_rx) = mpsc::channel(1);
    let (accepted_tx, accepted_rx) = mpsc::channel(1);
    let (disconnect_tx, disconnect_rx) = local_channel::channel();
    let reporter = PeerReporter {
        discovered_reporter: discovered_tx,
        accepted_reporter: accepted_tx,
    };
    let handler = handler_factory(&reporter);
    (
        reporter,
        ConnectThrottle {
            connected_peers: HashSet::with_capacity(cmp::min(512, handler.max_connections())),
            discovered_peers_receiver: discovered_rx,
            accepted_peers_receiver: accepted_rx,
            disconnected_peers_receiver: disconnect_rx,
            disconnect_reporter: disconnect_tx,
            handler,
        },
    )
}

#[derive(Clone)]
pub struct PeerReporter {
    discovered_reporter: mpsc::Sender<DiscoveredPeer>,
    accepted_reporter: mpsc::Sender<AcceptedPeer>,
}

impl PeerReporter {
    pub async fn report_discovered_new(&self, addr: SocketAddr, origin: PeerOrigin) -> bool {
        self.report_discovered(DiscoveredPeer {
            addr,
            origin,
            reconnects_left: None,
        })
        .await
    }

    pub async fn report_discovered(&self, peer: DiscoveredPeer) -> bool {
        self.discovered_reporter.send(peer).await.is_ok()
    }

    pub async fn report_accepted(&self, peer: AcceptedPeer) -> bool {
        self.accepted_reporter.send(peer).await.is_ok()
    }
}

#[derive(Debug)]
pub struct ConnectPermit {
    addr: SocketAddr,
    #[debug(skip)]
    disconnect_reporter: local_channel::Sender<SocketAddr>,
}

impl Drop for ConnectPermit {
    fn drop(&mut self) {
        if !self.disconnect_reporter.is_closed() {
            self.disconnect_reporter.send(self.addr);
        }
    }
}

pub struct ConnectThrottle<H> {
    connected_peers: HashSet<SocketAddr>,

    discovered_peers_receiver: mpsc::Receiver<DiscoveredPeer>,
    accepted_peers_receiver: mpsc::Receiver<AcceptedPeer>,

    disconnected_peers_receiver: local_channel::Receiver<SocketAddr>,
    disconnect_reporter: local_channel::Sender<SocketAddr>,

    handler: H,
}

impl<H: ConnectHandler> ConnectThrottle<H> {
    pub async fn run(mut self) {
        loop {
            select! {
                biased;
                disconnected = self.disconnected_peers_receiver.next() => {
                    match disconnected {
                        Some(peer) => self.on_peer_disconnected(peer),
                        None => unreachable!(),
                    }
                }
                accepted = self.accepted_peers_receiver.recv() => {
                    match accepted {
                        Some(peer) => self.on_peer_accepted(peer),
                        None => return,
                    }
                }
                discovered = self.discovered_peers_receiver.recv() => {
                    match discovered {
                        Some(peer) => self.on_peer_discovered(peer).await,
                        None => return,
                    }
                }
            }
        }
    }

    fn on_peer_accepted(&mut self, peer: AcceptedPeer) {
        if self.connected_peers.len() < self.handler.max_connections() {
            self.connected_peers.insert(peer.addr);
            let permit = ConnectPermit {
                addr: peer.addr,
                disconnect_reporter: self.disconnect_reporter.clone(),
            };
            self.handler.handle_accepted_peer(peer, permit);
        } else {
            log::warn!("Incoming connection from {} rejected", peer.addr);
        }
    }

    fn on_peer_disconnected(&mut self, peer: SocketAddr) {
        self.connected_peers.remove(&peer);
    }

    async fn on_peer_discovered(&mut self, peer: DiscoveredPeer) {
        while self.connected_peers.len() == self.handler.max_connections() {
            let Some(disconnected) = self.disconnected_peers_receiver.next().await else {
                return;
            };
            self.on_peer_disconnected(disconnected);
        }
        if self.connected_peers.insert(peer.addr) {
            let permit = ConnectPermit {
                addr: peer.addr,
                disconnect_reporter: self.disconnect_reporter.clone(),
            };
            self.handler.handle_discovered_peer(peer, permit);
        } else {
            log::debug!("Ignoring discovered peer {} (already connected)", peer.addr);
        }
    }
}

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

// ------------------------------------------------------------------------------------------------

pub(super) trait PeerConnector {
    const CONNECT_TIMEOUT: Duration;

    const MAX_CONNECT_RETRIES: usize;

    fn outbound_connect_and_handshake(
        &self,
        peer: DiscoveredPeer,
    ) -> impl Future<Output = io::Result<(DownloadChannels, UploadChannels, Option<ExtendedChannels>)>>;

    fn inbound_connect_and_handshake(
        &self,
        peer: AcceptedPeer,
    ) -> impl Future<Output = io::Result<(DownloadChannels, UploadChannels, Option<ExtendedChannels>)>>;

    fn run_connection(
        &self,
        origin: PeerOrigin,
        download_chans: DownloadChannels,
        upload_chans: UploadChannels,
        extended_chans: Option<ExtendedChannels>,
    ) -> impl Future<Output = io::Result<()>>;

    fn schedule_reconnect(&self, peer: DiscoveredPeer) -> impl Future<Output = ()>;
}

fn can_retry_connect(e: &io::Error, attempts_left: usize) -> bool {
    attempts_left > 0
        && matches!(
            e.kind(),
            io::ErrorKind::ConnectionRefused
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::UnexpectedEof
                | io::ErrorKind::TimedOut
                | io::ErrorKind::Interrupted
                // the 2 below happen when the preliminary connection hasn't exited yet:
                | io::ErrorKind::AddrInUse
                | io::ErrorKind::AddrNotAvailable
        )
}

async fn outgoing_pwp_connection<C: PeerConnector>(
    peer: DiscoveredPeer,
    connector: &C,
    permit: ConnectPermit,
) -> io::Result<()> {
    log::debug!("Connecting to {peer:?}");
    let connect_deadline = Instant::now() + C::CONNECT_TIMEOUT;

    let connect_result =
        time::timeout_at(connect_deadline, connector.outbound_connect_and_handshake(peer.clone()))
            .await
            .unwrap_or_else(|e| Err(io::Error::from(e)));

    let reconnects_left = peer.reconnects_left.unwrap_or(C::MAX_CONNECT_RETRIES);
    let (download_chans, upload_chans, extended_chans) = match connect_result {
        Ok(channels) => channels,
        Err(e) => {
            if can_retry_connect(&e, reconnects_left) {
                time::sleep_until(connect_deadline).await;
                drop(permit);
                connector
                    .schedule_reconnect(DiscoveredPeer {
                        reconnects_left: Some(reconnects_left - 1),
                        ..peer
                    })
                    .await;
            }
            return Err(e);
        }
    };

    log::debug!("Successful outgoing connection to {peer:?}");
    let connected_time = Instant::now();

    let run_result = connector
        .run_connection(peer.origin, download_chans, upload_chans, extended_chans)
        .await;

    if let Err(e) = &run_result
        && e.kind() != io::ErrorKind::Other
        && connected_time.elapsed() > sec!(5)
    {
        // ErrorKind::Other means we disconnected the peer intentionally, and
        // <5s since connect means peer probably didn't like our handshake.
        // Since none of the above is the case, let's try to reconnect in a sec
        log::warn!("Peer {} disconnected: {e}. Reconnecting in 1s...", peer.addr);
        time::sleep(sec!(1)).await;
        drop(permit);
        connector.schedule_reconnect(peer).await;
    }

    run_result
}

async fn incoming_pwp_connection<C: PeerConnector>(
    peer: AcceptedPeer,
    connector: &C,
    permit: ConnectPermit,
) -> io::Result<()> {
    log::debug!("Connecting to {peer:?}");
    let peer_addr = peer.addr;
    let connect_deadline = Instant::now() + C::CONNECT_TIMEOUT;

    let (download_chans, upload_chans, extended_chans) =
        time::timeout_at(connect_deadline, connector.inbound_connect_and_handshake(peer)).await??;

    log::debug!("Successful incoming connection from {peer_addr}");

    let run_result = connector
        .run_connection(PeerOrigin::Listener, download_chans, upload_chans, extended_chans)
        .await;
    if let Err(e) = &run_result
        && e.kind() != io::ErrorKind::Other
    {
        // ErrorKind::Other means we disconnected the peer intentionally
        log::warn!("Peer {peer_addr} disconnected: {e}. Reconnecting...");
        drop(permit);
        connector
            .schedule_reconnect(DiscoveredPeer {
                addr: peer_addr,
                origin: PeerOrigin::Listener,
                reconnects_left: Some(C::MAX_CONNECT_RETRIES),
            })
            .await;
    }
    run_result
}

pub struct CancellingConnectHandler<C> {
    pub connector: C,
    pub max_connections: usize,
    pub canceller: CancellationToken,
}

macro_rules! log {
    ($e:expr, $($arg:tt)+) => {{
        let lvl = if $e.kind() == io::ErrorKind::Other {
            log::Level::Error
        } else {
            log::Level::Debug
        };
        log::log!(lvl, $($arg)+);
    }}
}

impl<C: PeerConnector + Clone + 'static> ConnectHandler for CancellingConnectHandler<C> {
    fn max_connections(&self) -> usize {
        self.max_connections
    }

    fn handle_discovered_peer(&mut self, peer: DiscoveredPeer, permit: ConnectPermit) {
        let peer_addr = peer.addr;
        let data = self.connector.clone();
        let canceller = self.canceller.clone();
        task::spawn_local(async move {
            let result = canceller
                .run_until_cancelled(outgoing_pwp_connection(peer, &data, permit))
                .await;
            if let Some(Err(e)) = result {
                log!(e, "Outgoing peer connection to {peer_addr} failed: {e}");
            }
        });
    }

    fn handle_accepted_peer(&mut self, peer: AcceptedPeer, permit: ConnectPermit) {
        let peer_addr = peer.addr;
        let data = self.connector.clone();
        let canceller = self.canceller.clone();
        task::spawn_local(async move {
            let result = canceller
                .run_until_cancelled(incoming_pwp_connection(peer, &data, permit))
                .await;
            if let Some(Err(e)) = result {
                log!(e, "Incoming peer connection from {peer_addr} failed: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mtorrent_core::pwp::{Handshake, channels_from_mock};
    use std::{cell::Cell, rc::Rc, str::FromStr};
    use tokio::{io, sync::mpsc::error::TryRecvError, task};

    macro_rules! addr {
        ($addr:literal) => {
            SocketAddr::from_str($addr).unwrap()
        };
    }

    struct MockHandler(tokio::sync::mpsc::UnboundedSender<(DiscoveredPeer, ConnectPermit)>);
    impl ConnectHandler for MockHandler {
        fn handle_discovered_peer(&mut self, peer: DiscoveredPeer, permit: ConnectPermit) {
            self.0.send((peer, permit)).unwrap();
        }

        fn handle_accepted_peer(&mut self, _peer: AcceptedPeer, _permit: ConnectPermit) {
            unimplemented!()
        }

        fn max_connections(&self) -> usize {
            2
        }
    }

    #[tokio::test]
    async fn test_connect_ctrl_ignores_duplicates_and_respects_budget() {
        task::LocalSet::new()
            .run_until(async {
                let (tx, mut rx) = mpsc::unbounded_channel();
                let (reporter, throttle) = connect_control(|_| MockHandler(tx));
                task::spawn_local(throttle.run());

                // discover peer 1
                let peer1 = DiscoveredPeer {
                    addr: addr!("1.1.1.1:1111"),
                    origin: PeerOrigin::Other,
                    reconnects_left: Some(3),
                };
                assert!(reporter.report_discovered(peer1.clone()).await);

                // get permit for peer 1
                tokio::task::yield_now().await;
                let (peer, permit1) = rx.try_recv().unwrap();
                assert_eq!(peer.addr, peer1.addr);

                // re-discover peer 1
                assert!(reporter.report_discovered(peer1.clone()).await);

                // no permit
                tokio::task::yield_now().await;
                let e = rx.try_recv().unwrap_err();
                assert_eq!(e, TryRecvError::Empty);

                // discover peer 2
                let peer2 = DiscoveredPeer {
                    addr: addr!("1.1.1.1:2222"),
                    origin: PeerOrigin::Other,
                    reconnects_left: Some(3),
                };
                assert!(reporter.report_discovered(peer2.clone()).await);

                // get permit for peer 2
                tokio::task::yield_now().await;
                let (peer, permit2) = rx.try_recv().unwrap();
                assert_eq!(peer.addr, peer2.addr);

                // disconnect peer 1 and re-discover it again
                drop(permit1);
                assert!(reporter.report_discovered(peer1.clone()).await);

                // get permit for peer 1
                tokio::task::yield_now().await;
                let (peer, _permit1) = rx.try_recv().unwrap();
                assert_eq!(peer.addr, peer1.addr);

                // discover peer 3
                let peer3 = DiscoveredPeer {
                    addr: addr!("1.1.1.1:3333"),
                    origin: PeerOrigin::Other,
                    reconnects_left: Some(3),
                };
                assert!(reporter.report_discovered(peer3.clone()).await);

                // no bugdet no permit
                tokio::task::yield_now().await;
                let e = rx.try_recv().unwrap_err();
                assert_eq!(e, TryRecvError::Empty);

                // disconnect peer 2 and get permit for peer 3
                drop(permit2);
                tokio::task::yield_now().await;
                let (peer, _permit3) = rx.try_recv().unwrap();
                assert_eq!(peer.addr, peer3.addr);

                // no pending permits
                tokio::task::yield_now().await;
                let e = rx.try_recv().unwrap_err();
                assert_eq!(e, TryRecvError::Empty);
            })
            .await;
    }

    fn mock_permit() -> ConnectPermit {
        let (tx, _rx) = local_channel::channel();
        ConnectPermit {
            addr: addr!("127.0.0.1:8080"),
            disconnect_reporter: tx,
        }
    }

    #[derive(Default)]
    struct MockPeerConnector {
        expected_discovered: Cell<Option<DiscoveredPeer>>,
        on_discovered_error: Cell<Option<io::Error>>,
        on_discovered_timeout: Cell<bool>,

        expected_accepted: Cell<Option<AcceptedPeer>>,
        on_accepted_error: Cell<Option<io::Error>>,
        on_accepted_timeout: Cell<bool>,

        expected_connection_origin: Cell<Option<PeerOrigin>>,
        connection_run_error: Cell<Option<io::Error>>,
        run_connection_duration: Cell<Option<Duration>>,

        expected_reconnect: Cell<Option<DiscoveredPeer>>,
    }
    impl Drop for MockPeerConnector {
        fn drop(&mut self) {
            assert!(self.expected_discovered.take().is_none());
            assert!(self.on_discovered_error.take().is_none());
            assert!(!self.on_discovered_timeout.replace(false));

            assert!(self.expected_accepted.take().is_none());
            assert!(self.on_accepted_error.take().is_none());
            assert!(!self.on_accepted_timeout.replace(false));

            assert!(self.expected_connection_origin.take().is_none());
            assert!(self.connection_run_error.take().is_none());
            assert!(self.run_connection_duration.replace(None).is_none());

            assert!(self.expected_reconnect.take().is_none());
        }
    }
    impl MockPeerConnector {
        fn expect_discovered(&self, peer: DiscoveredPeer, error: Option<io::Error>) {
            self.expected_discovered.set(Some(peer));
            self.on_discovered_error.set(error);
        }
        fn on_discovered_timeout(&self) {
            self.on_discovered_timeout.set(true);
        }
        #[expect(dead_code)]
        fn expect_accepted(&self, peer: AcceptedPeer, error: Option<io::Error>) {
            self.expected_accepted.set(Some(peer));
            self.on_accepted_error.set(error);
        }
        #[expect(dead_code)]
        fn on_accepted_timeout(&self) {
            self.on_accepted_timeout.set(true);
        }
        fn expect_run_connection(&self, origin: PeerOrigin, error: Option<io::Error>) {
            self.expected_connection_origin.set(Some(origin));
            self.connection_run_error.set(error);
        }
        fn run_connection_for(&self, duration: Duration) {
            self.run_connection_duration.set(Some(duration));
        }
        fn expect_reconnect(&self, peer: DiscoveredPeer) {
            self.expected_reconnect.set(Some(peer));
        }
        fn expect_no_reconnect(&self) {
            self.expected_reconnect.set(None);
        }
    }
    impl PeerConnector for MockPeerConnector {
        const CONNECT_TIMEOUT: Duration = sec!(15);

        const MAX_CONNECT_RETRIES: usize = 3;

        fn outbound_connect_and_handshake(
            &self,
            peer: DiscoveredPeer,
        ) -> impl Future<Output = io::Result<(DownloadChannels, UploadChannels, Option<ExtendedChannels>)>>
        {
            let expected = self.expected_discovered.take();
            let error = self.on_discovered_error.take();
            async move {
                let peer_addr = peer.addr;
                assert_eq!(Some(peer), expected);

                if self.on_discovered_timeout.replace(false) {
                    std::future::pending::<()>().await;
                }
                if let Some(e) = error {
                    Err(e)
                } else {
                    Ok(channels_from_mock(peer_addr, Handshake::default(), false, io::empty()))
                }
            }
        }
        fn inbound_connect_and_handshake(
            &self,
            peer: AcceptedPeer,
        ) -> impl Future<Output = io::Result<(DownloadChannels, UploadChannels, Option<ExtendedChannels>)>>
        {
            let expected = self.expected_accepted.take();
            let error = self.on_accepted_error.take();
            async move {
                assert_eq!(peer.addr, expected.unwrap().addr);

                if self.on_accepted_timeout.replace(false) {
                    std::future::pending::<()>().await;
                }
                if let Some(e) = error {
                    Err(e)
                } else {
                    Ok(channels_from_mock(peer.addr, Handshake::default(), false, io::empty()))
                }
            }
        }
        fn run_connection(
            &self,
            origin: PeerOrigin,
            _download_chans: DownloadChannels,
            _upload_chans: UploadChannels,
            _extended_chans: Option<ExtendedChannels>,
        ) -> impl Future<Output = io::Result<()>> {
            let expected = self.expected_connection_origin.take();
            let error = self.connection_run_error.take();
            async move {
                assert_eq!(Some(origin), expected);
                if let Some(duration) = self.run_connection_duration.replace(None) {
                    time::sleep(duration).await;
                }
                if let Some(e) = error { Err(e) } else { Ok(()) }
            }
        }
        fn schedule_reconnect(&self, peer: DiscoveredPeer) -> impl Future<Output = ()> {
            let expected = self.expected_reconnect.take();
            async move {
                assert_eq!(Some(peer), expected);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_outbound_retry_on_connect_error() {
        let connector = MockPeerConnector::default();

        let retriable_errors = [
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::UnexpectedEof,
            io::ErrorKind::TimedOut,
            io::ErrorKind::AddrInUse,
            io::ErrorKind::AddrNotAvailable,
            io::ErrorKind::Interrupted,
        ];

        for error_kind in retriable_errors {
            // with 1 retry left
            let peer = DiscoveredPeer {
                addr: addr!("127.0.0.1:8080"),
                origin: PeerOrigin::Other,
                reconnects_left: Some(1),
            };
            connector.expect_discovered(peer.clone(), Some(io::Error::from(error_kind)));
            connector.expect_reconnect(DiscoveredPeer {
                reconnects_left: Some(0),
                ..peer.clone()
            });

            let error = outgoing_pwp_connection(peer.clone(), &connector, mock_permit())
                .await
                .unwrap_err();
            assert_eq!(error.kind(), error_kind);

            // with no retries left
            let peer = DiscoveredPeer {
                reconnects_left: Some(0),
                ..peer
            };
            connector.expect_discovered(peer.clone(), Some(io::Error::from(error_kind)));
            connector.expect_no_reconnect();

            let error = outgoing_pwp_connection(peer.clone(), &connector, mock_permit())
                .await
                .unwrap_err();
            assert_eq!(error.kind(), error_kind);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_no_outbound_retry_on_connect_error() {
        let connector = MockPeerConnector::default();

        let non_retriable_errors = [
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::AlreadyExists,
            io::ErrorKind::InvalidInput,
            io::ErrorKind::InvalidData,
            io::ErrorKind::Other,
        ];

        for error_kind in non_retriable_errors {
            let peer = DiscoveredPeer {
                addr: addr!("127.0.0.1:8080"),
                origin: PeerOrigin::Other,
                reconnects_left: Some(1),
            };
            connector.expect_discovered(peer.clone(), Some(io::Error::from(error_kind)));
            connector.expect_no_reconnect();

            let error = outgoing_pwp_connection(peer.clone(), &connector, mock_permit())
                .await
                .unwrap_err();
            assert_eq!(error.kind(), error_kind);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_run_connection_reconnect() {
        let connector = MockPeerConnector::default();

        let retriable_errors = [
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::UnexpectedEof,
            io::ErrorKind::TimedOut,
            io::ErrorKind::AddrInUse,
            io::ErrorKind::AddrNotAvailable,
            io::ErrorKind::Interrupted,
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::AlreadyExists,
            io::ErrorKind::InvalidInput,
            io::ErrorKind::InvalidData,
        ];

        for error_kind in retriable_errors {
            let peer = DiscoveredPeer {
                addr: addr!("127.0.0.1:8080"),
                origin: PeerOrigin::Pex,
                reconnects_left: Some(1),
            };
            connector.expect_discovered(peer.clone(), None);
            connector.expect_run_connection(PeerOrigin::Pex, Some(io::Error::from(error_kind)));
            connector.run_connection_for(sec!(6));
            connector.expect_reconnect(peer.clone());

            let error = outgoing_pwp_connection(peer.clone(), &connector, mock_permit())
                .await
                .unwrap_err();
            assert_eq!(error.kind(), error_kind);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_run_connection_no_reconnect_on_error_other() {
        let connector = MockPeerConnector::default();

        let error_kind = io::ErrorKind::Other;

        let peer = DiscoveredPeer {
            addr: addr!("127.0.0.1:8080"),
            origin: PeerOrigin::Pex,
            reconnects_left: Some(1),
        };
        connector.expect_discovered(peer.clone(), None);
        connector.expect_run_connection(PeerOrigin::Pex, Some(io::Error::from(error_kind)));
        connector.run_connection_for(sec!(6));
        connector.expect_no_reconnect();

        let error = outgoing_pwp_connection(peer.clone(), &connector, mock_permit())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), error_kind);
    }

    #[tokio::test(start_paused = true)]
    async fn test_run_connection_no_reconnect_if_short_lived() {
        let connector = MockPeerConnector::default();

        let error_kind = io::ErrorKind::UnexpectedEof;

        let peer = DiscoveredPeer {
            addr: addr!("127.0.0.1:8080"),
            origin: PeerOrigin::Pex,
            reconnects_left: Some(1),
        };
        connector.expect_discovered(peer.clone(), None);
        connector.expect_run_connection(PeerOrigin::Pex, Some(io::Error::from(error_kind)));
        connector.run_connection_for(sec!(5));
        connector.expect_no_reconnect();

        let error = outgoing_pwp_connection(peer.clone(), &connector, mock_permit())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), error_kind);
    }

    #[tokio::test(start_paused = true)]
    async fn test_outbound_connect_timeout() {
        task::LocalSet::new()
            .run_until(async {
                let connector = Rc::new(MockPeerConnector::default());

                let peer = DiscoveredPeer {
                    addr: addr!("127.0.0.1:8080"),
                    origin: PeerOrigin::Pex,
                    reconnects_left: Some(1),
                };

                connector.expect_discovered(peer.clone(), None);
                connector.on_discovered_timeout();
                connector.expect_no_reconnect();

                let task_handle = task::spawn_local({
                    let connector = connector.clone();
                    let peer = peer.clone();
                    async move { outgoing_pwp_connection(peer, &*connector, mock_permit()).await }
                });
                assert!(!task_handle.is_finished());

                time::sleep(sec!(15)).await;
                assert!(!task_handle.is_finished());

                connector.expect_reconnect(DiscoveredPeer {
                    reconnects_left: Some(0),
                    ..peer.clone()
                });
                task::yield_now().await;
                assert!(task_handle.is_finished());
                let error = task_handle.await.unwrap().unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::TimedOut);
            })
            .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_outbound_connect_retry_interval() {
        task::LocalSet::new()
            .run_until(async {
                let connector = Rc::new(MockPeerConnector::default());

                let peer = DiscoveredPeer {
                    addr: addr!("127.0.0.1:8080"),
                    origin: PeerOrigin::Pex,
                    reconnects_left: Some(1),
                };

                connector.expect_discovered(
                    peer.clone(),
                    Some(io::Error::from(io::ErrorKind::UnexpectedEof)),
                );
                connector.expect_no_reconnect();

                let task_handle = task::spawn_local({
                    let connector = connector.clone();
                    let peer = peer.clone();
                    async move { outgoing_pwp_connection(peer, &*connector, mock_permit()).await }
                });
                assert!(!task_handle.is_finished());

                time::sleep(sec!(15)).await;
                assert!(!task_handle.is_finished());

                connector.expect_reconnect(DiscoveredPeer {
                    reconnects_left: Some(0),
                    ..peer.clone()
                });
                task::yield_now().await;
                assert!(task_handle.is_finished());
                let error = task_handle.await.unwrap().unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
            })
            .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_run_connection_reconnect_interval() {
        task::LocalSet::new()
            .run_until(async {
                let connector = Rc::new(MockPeerConnector::default());

                let peer = DiscoveredPeer {
                    addr: addr!("127.0.0.1:8080"),
                    origin: PeerOrigin::Pex,
                    reconnects_left: Some(1),
                };

                connector.expect_discovered(peer.clone(), None);
                connector.expect_run_connection(
                    PeerOrigin::Pex,
                    Some(io::Error::from(io::ErrorKind::UnexpectedEof)),
                );
                connector.run_connection_for(sec!(6));
                connector.expect_no_reconnect();

                let task_handle = task::spawn_local({
                    let connector = connector.clone();
                    let peer = peer.clone();
                    async move { outgoing_pwp_connection(peer, &*connector, mock_permit()).await }
                });
                assert!(!task_handle.is_finished());

                time::sleep(sec!(6 + 1)).await;
                assert!(!task_handle.is_finished());

                connector.expect_reconnect(peer);
                task::yield_now().await;
                assert!(task_handle.is_finished());
                let error = task_handle.await.unwrap().unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
            })
            .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_default_max_retry_attempts() {
        let connector = MockPeerConnector::default();

        let error_kind = io::ErrorKind::ConnectionRefused;

        let mut peer = DiscoveredPeer {
            addr: addr!("127.0.0.1:8080"),
            origin: PeerOrigin::Other,
            reconnects_left: None,
        };

        for i in 1..MockPeerConnector::MAX_CONNECT_RETRIES {
            let modified_peer = DiscoveredPeer {
                reconnects_left: Some(MockPeerConnector::MAX_CONNECT_RETRIES - i),
                ..peer.clone()
            };
            connector.expect_discovered(peer.clone(), Some(io::Error::from(error_kind)));
            connector.expect_reconnect(modified_peer.clone());

            let error = outgoing_pwp_connection(peer.clone(), &connector, mock_permit())
                .await
                .unwrap_err();
            assert_eq!(error.kind(), error_kind);
            peer = modified_peer;
        }

        let peer = DiscoveredPeer {
            reconnects_left: Some(0),
            ..peer
        };
        connector.expect_discovered(peer.clone(), Some(io::Error::from(error_kind)));
        connector.expect_no_reconnect();

        let error = outgoing_pwp_connection(peer.clone(), &connector, mock_permit())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), error_kind);
    }
}
