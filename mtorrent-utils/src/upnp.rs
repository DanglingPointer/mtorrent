use crate::net;
use igd_next;
use local_async_utils::prelude::*;
use std::mem;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio::time::error::Elapsed;
use tokio::{pin, select, task, time};

pub use igd_next::PortMappingProtocol;

/// Errors returned by the UPnP port-mapping task and [`PortMapperHandle`].
#[derive(Error, Debug, Clone)]
pub enum Error {
    /// An error reported by the underlying `igd_next` library.
    #[error("{0}")]
    IgdError(String),
    /// The [`PortMapperHandle`]'s background task has exited (or never started).
    #[error("UPnP task is not running")]
    Stopped,
    /// UPnP request didn't complete within a timeout.
    #[error("request timed out")]
    Timeout,
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<igd_next::Error> for Error {
    fn from(e: igd_next::Error) -> Self {
        Self::IgdError(e.to_string())
    }
}

impl From<igd_next::SearchError> for Error {
    fn from(e: igd_next::SearchError) -> Self {
        match e {
            igd_next::SearchError::NoResponseWithinTimeout => Self::Timeout,
            e => Self::IgdError(e.to_string()),
        }
    }
}

impl From<Elapsed> for Error {
    fn from(_: Elapsed) -> Self {
        Self::Timeout
    }
}

impl From<task::JoinError> for Error {
    fn from(_e: task::JoinError) -> Self {
        Self::Stopped
    }
}

/// Launch a background task that creates and maintains a UPnP port mapping on the local
/// gateway, and return a [`PortMapperHandle`] to interact with it. Must be called from within a
/// Tokio runtime context, as the task is spawned via [`tokio::task::spawn`].
///
/// The task renews the mapping periodically until [`PortMapperHandle::shutdown`] is called or the
/// handle is dropped. In either case the task attempts to remove the mapping. Prefer
/// [`PortMapperHandle::shutdown`], which waits for the removal attempt and reports whether the task
/// exited successfully. Dropping the handle only signals the task, so cleanup is best-effort and
/// requires the Tokio runtime to keep running long enough to complete it.
///
/// * `proto` — TCP or UDP mapping.
/// * `internal_port` — the local port that traffic should be forwarded to.
/// * `desired_external_port` — a specific external port to request, or `None` to let the gateway
///   pick one.
/// * `interface` — the local network interface to bind to, or `None` to use the default one.
pub fn launch(
    proto: PortMappingProtocol,
    internal_port: u16,
    desired_external_port: Option<u16>,
    interface: Option<String>,
) -> PortMapperHandle {
    launch_impl(proto, internal_port, desired_external_port, interface, IgdNextGatewayFactory)
}

fn launch_impl<G>(
    proto: PortMappingProtocol,
    internal_port: u16,
    desired_external_port: Option<u16>,
    interface: Option<String>,
    gateway_factory: G,
) -> PortMapperHandle
where
    G: GatewayFactory + Send + 'static,
    G::Gateway: Send + Sync,
{
    let (get_addr_tx, get_addr_rx) = oneshot::channel();
    let (stop_tx, stop_rx) = oneshot::channel();

    let ctx = PortMappingContext {
        proto,
        internal_port,
        desired_external_port,
        interface,
        get_external_addr_tx: get_addr_tx,
        stop_receiver: stop_rx,
    };

    let join_handle = task::spawn(maintain_port_mapping(ctx, gateway_factory));

    PortMapperHandle {
        get_external_addr: GetExternalAddrStatus::InProgress(get_addr_rx),
        exit: Some((stop_tx, join_handle)),
    }
}

/// A handle to a running UPnP port-mapping task.
///
/// Use it to query the external address assigned by the gateway. Call [`Self::shutdown`] to remove
/// the port mapping and wait for the task to exit. Dropping the handle also requests removal, but
/// does not wait for it; if the runtime stops before the background task finishes, the mapping may
/// remain on the gateway until its lease expires.
#[must_use]
pub struct PortMapperHandle {
    get_external_addr: GetExternalAddrStatus,
    exit: Option<(oneshot::Sender<()>, task::JoinHandle<Result<()>>)>,
}

enum GetExternalAddrStatus {
    InProgress(oneshot::Receiver<Result<SocketAddr>>),
    Finished(Result<SocketAddr>),
}

impl PortMapperHandle {
    /// Return the external `SocketAddr` assigned by the gateway.
    ///
    /// Awaits the initial mapping to be created on the first call, then caches the result so
    /// subsequent calls return the same value immediately. If the background task fails to
    /// create the mapping or has already exited, returns the corresponding [`Error`](enum@Error).
    pub async fn get_external_addr(&mut self) -> Result<SocketAddr> {
        let status = mem::replace(
            &mut self.get_external_addr,
            GetExternalAddrStatus::Finished(Err(Error::Stopped)),
        );
        let result = match status {
            GetExternalAddrStatus::InProgress(receiver) => match receiver.await {
                Ok(result) => result,
                Err(_) => Err(Error::Stopped),
            },
            GetExternalAddrStatus::Finished(result) => result,
        };
        self.get_external_addr = GetExternalAddrStatus::Finished(result.clone());
        result
    }

    /// Signal the background task to remove the port mapping from the gateway and wait for it to
    /// exit.
    ///
    /// Returns an error if the background task had already failed, was cancelled, or could not
    /// remove the mapping.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some((stop_sender, join_handle)) = self.exit.take() {
            drop(stop_sender);
            join_handle.await??;
        }
        Ok(())
    }
}

struct PortMappingContext {
    proto: PortMappingProtocol,
    internal_port: u16,
    desired_external_port: Option<u16>,
    interface: Option<String>,

    get_external_addr_tx: oneshot::Sender<Result<SocketAddr>>,
    stop_receiver: oneshot::Receiver<()>,
}

async fn maintain_port_mapping<G: GatewayFactory>(
    ctx: PortMappingContext,
    gateway_factory: G,
) -> Result<()> {
    struct MissedCleanupLogger(Option<(PortMappingProtocol, SocketAddr)>);

    impl Drop for MissedCleanupLogger {
        fn drop(&mut self) {
            if let Some((proto, external_addr)) = self.0.take() {
                log::error!(
                    "UPnP exiting without cleaning up {proto} port mapping at {external_addr}"
                );
            }
        }
    }

    /// Recommended lease duration from <https://upnp.org/specs/gw/UPnP-gw-WANIPConnection-v2-Service.pdf>.
    const PORT_LEASE_DURATION_SEC: u32 = 3600;

    let PortMappingContext {
        proto,
        internal_port,
        desired_external_port,
        interface,
        get_external_addr_tx,
        stop_receiver: canceller,
    } = ctx;

    let port_opener = match PortOpener::new(
        proto,
        internal_port,
        desired_external_port,
        interface.as_deref(),
        PORT_LEASE_DURATION_SEC,
        gateway_factory,
    )
    .await
    {
        Ok(opener) => {
            let external_addr = opener.external_addr;
            log::info!("UPnP: {proto:?} port mapping succeeded, public addr: {external_addr}");
            _ = get_external_addr_tx.send(Ok(external_addr));
            opener
        }
        Err(e) => {
            log::error!("UPnP: {proto:?} port mapping failed: {e}");
            _ = get_external_addr_tx.send(Err(e.clone()));
            return Err(e);
        }
    };

    let mut notifier_guard = MissedCleanupLogger(Some((proto, port_opener.external_addr)));

    // Continuously renew the port mapping until the stop signal fires
    let result = async move {
        let renewal_period = sec!(PORT_LEASE_DURATION_SEC as u64);
        let mut renewal_timer = time::interval_at(Instant::now() + renewal_period, renewal_period);
        renewal_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        pin!(canceller);
        loop {
            select! {
                biased;
                _ = &mut canceller => {
                    port_opener.remove_mapping().await?;
                    break;
                }
                _ = renewal_timer.tick() => {
                    port_opener.renew_mapping(PORT_LEASE_DURATION_SEC).await?;
                }
            }
        }
        Ok(())
    }
    .await;
    notifier_guard.0.take();
    result
}

#[cfg_attr(test, mockall::automock(type Gateway = MockGateway;))]
trait GatewayFactory {
    type Gateway: Gateway;

    fn search(
        &self,
        opts: igd_next::SearchOptions,
    ) -> impl Future<Output = Result<Self::Gateway>> + Send;
}

#[cfg_attr(test, mockall::automock)]
trait Gateway {
    fn get_external_ip(&self, timeout: Duration) -> impl Future<Output = Result<IpAddr>> + Send;

    fn add_port(
        &self,
        proto: PortMappingProtocol,
        external_port: u16,
        local_addr: SocketAddr,
        lease_duration_sec: u32,
        description: &str,
        timeout: Duration,
    ) -> impl Future<Output = Result<()>> + Send;

    fn add_any_port(
        &self,
        proto: PortMappingProtocol,
        local_addr: SocketAddr,
        lease_duration_sec: u32,
        description: &str,
        timeout: Duration,
    ) -> impl Future<Output = Result<u16>> + Send;

    fn remove_port(
        &self,
        proto: PortMappingProtocol,
        external_port: u16,
        timeout: Duration,
    ) -> impl Future<Output = Result<()>> + Send;
}

struct IgdNextGatewayFactory;

type IgdNextGateway = igd_next::aio::Gateway<igd_next::aio::tokio::Tokio>;

impl GatewayFactory for IgdNextGatewayFactory {
    type Gateway = IgdNextGateway;

    async fn search(&self, opts: igd_next::SearchOptions) -> Result<Self::Gateway> {
        Ok(igd_next::aio::tokio::search_gateway(opts).await?)
    }
}

impl Gateway for IgdNextGateway {
    async fn get_external_ip(&self, timeout: Duration) -> Result<IpAddr> {
        with_timeout(timeout, igd_next::aio::Gateway::get_external_ip(self)).await
    }

    async fn add_port(
        &self,
        proto: PortMappingProtocol,
        external_port: u16,
        local_addr: SocketAddr,
        lease_duration_sec: u32,
        description: &str,
        timeout: Duration,
    ) -> Result<()> {
        with_timeout(
            timeout,
            igd_next::aio::Gateway::add_port(
                self,
                proto,
                external_port,
                local_addr,
                lease_duration_sec,
                description,
            ),
        )
        .await
    }

    async fn add_any_port(
        &self,
        proto: PortMappingProtocol,
        local_addr: SocketAddr,
        lease_duration_sec: u32,
        description: &str,
        timeout: Duration,
    ) -> Result<u16> {
        with_timeout(
            timeout,
            igd_next::aio::Gateway::add_any_port(
                self,
                proto,
                local_addr,
                lease_duration_sec,
                description,
            ),
        )
        .await
    }

    async fn remove_port(
        &self,
        proto: PortMappingProtocol,
        external_port: u16,
        timeout: Duration,
    ) -> Result<()> {
        with_timeout(timeout, igd_next::aio::Gateway::remove_port(self, proto, external_port)).await
    }
}

async fn with_timeout<T, E: Into<igd_next::Error>>(
    timeout: Duration,
    f: impl Future<Output = std::result::Result<T, E>>,
) -> Result<T> {
    let ret = time::timeout(timeout, f).await?.map_err(Into::into)?;
    Ok(ret)
}

/// Utility for creating and maintaining a port mapping on the local gateway via UPnP.
struct PortOpener<G: GatewayFactory> {
    gateway: G::Gateway,
    internal_addr: SocketAddr,
    external_addr: SocketAddr,
    proto: PortMappingProtocol,
}

impl<G: GatewayFactory> PortOpener<G> {
    const GATEWAY_CALL_TIMEOUT: Duration = sec!(2);

    /// Create a TCP or UDP port mapping that will be valid for
    /// `lease_duration_sec` seconds and return a `PortOpener` that maintains it.
    ///
    /// If `desired_external_port` is not specified, the gateway will assign an arbitrary external
    /// port number.
    /// If `interface` is not specified, the first active network adapter with a non-loopback IPv4
    /// address will be used.
    async fn new(
        proto: PortMappingProtocol,
        internal_port: u16,
        desired_external_port: Option<u16>,
        interface: Option<&str>,
        lease_duration_sec: u32,
        gateway_factory: G,
    ) -> Result<Self> {
        // get our IP on the local network
        let internal_ip = if let Some(iface) = interface {
            net::get_bind_addr_v4(Some(iface)).into()
        } else {
            net::get_local_addr(|addr| {
                addr.is_ipv4() && !addr.is_loopback() && !addr.is_unspecified()
            })
            .unwrap_or(Ipv4Addr::BROADCAST.into())
        };
        let internal_addr = SocketAddr::new(internal_ip, internal_port);

        // see if the gateway supports UPnP
        let gateway = gateway_factory
            .search(igd_next::SearchOptions {
                timeout: Some(sec!(5)),
                bind_addr: (internal_ip, 0).into(),
                ..Default::default()
            })
            .await?;

        // create port mapping and get our external IP and port
        let public_ip = gateway.get_external_ip(Self::GATEWAY_CALL_TIMEOUT).await?;
        let public_port = if let Some(desired_port) = desired_external_port {
            gateway
                .add_port(
                    proto,
                    desired_port,
                    internal_addr,
                    lease_duration_sec,
                    "",
                    Self::GATEWAY_CALL_TIMEOUT,
                )
                .await?;
            desired_port
        } else {
            gateway
                .add_any_port(
                    proto,
                    internal_addr,
                    lease_duration_sec,
                    "",
                    Self::GATEWAY_CALL_TIMEOUT,
                )
                .await?
        };
        let external_addr = SocketAddr::new(public_ip, public_port);

        Ok(Self {
            gateway,
            internal_addr,
            external_addr,
            proto,
        })
    }

    async fn remove_mapping(&self) -> Result<()> {
        let proto = self.proto;
        let external_port = self.external_addr.port();
        let result =
            self.gateway.remove_port(proto, external_port, Self::GATEWAY_CALL_TIMEOUT).await;
        match &result {
            Ok(()) => {
                log::info!("UPnP: port mapping deleted ({proto}:{external_port})");
            }
            Err(e) => {
                log::warn!("UPnP: failed to delete port mapping ({proto}:{external_port}): {e}");
            }
        }
        result
    }

    async fn renew_mapping(&self, lease_duration_sec: u32) -> Result<()> {
        let proto = self.proto;
        let external_port = self.external_addr.port();
        let mut attempts_left = 5;
        let result = loop {
            let result = self
                .gateway
                .add_port(
                    proto,
                    external_port,
                    self.internal_addr,
                    lease_duration_sec,
                    "",
                    Self::GATEWAY_CALL_TIMEOUT,
                )
                .await;
            attempts_left -= 1;
            if attempts_left == 0 || result.is_ok() {
                break result;
            }
            // Sometimes renewal fails because previous lease hasn't expired yet. Wait a bit and try
            // again
            time::sleep(millisec!(100)).await;
        };
        match &result {
            Ok(()) => {
                log::info!("UPnP: port mapping renewed ({proto}:{external_port})");
            }
            Err(e) => {
                log::error!("UPnP: failed to renew port mapping ({proto}:{external_port}): {e}")
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use log::Level;
    use mockall::predicate::{always, eq};
    use std::net::Ipv4Addr;
    use tokio::time;

    fn make_gateway_with_ip(ip: IpAddr) -> MockGateway {
        let mut gw = MockGateway::new();
        gw.expect_get_external_ip().returning(move |_| async move { Ok(ip) }.boxed());
        gw
    }

    fn make_factory_returning(
        gw: impl FnOnce() -> MockGateway + Send + 'static,
    ) -> MockGatewayFactory {
        let mut factory = MockGatewayFactory::new();
        let mut gw_slot = Some(gw());
        factory.expect_search().once().returning(move |_opts| {
            let gw = gw_slot.take().expect("search called more than once");
            async move { Ok(gw) }.boxed()
        });
        factory
    }

    #[tokio::test(start_paused = true)]
    async fn test_creates_mapping_with_desired_port_and_removes_on_cancel() {
        _ = simple_logger::init_with_level(Level::Debug);

        let internal_port = 12345u16;
        let desired_external_port = 54321u16;
        let external_ip: IpAddr = Ipv4Addr::new(203, 0, 113, 1).into();

        let factory = make_factory_returning(move || {
            let mut gw = make_gateway_with_ip(external_ip);
            gw.expect_add_port()
                .once()
                .with(
                    eq(PortMappingProtocol::TCP),
                    eq(desired_external_port),
                    always(),
                    always(),
                    always(),
                    always(),
                )
                .returning(|_, _, _, _, _, _| async { Ok(()) }.boxed());
            gw.expect_remove_port()
                .once()
                .with(eq(PortMappingProtocol::TCP), eq(desired_external_port), always())
                .returning(|_, _, _| async { Ok(()) }.boxed());
            gw
        });

        let mut handle = launch_impl(
            PortMappingProtocol::TCP,
            internal_port,
            Some(desired_external_port),
            None,
            factory,
        );

        let external_addr = handle.get_external_addr().await.unwrap();
        assert_eq!(external_addr, SocketAddr::new(external_ip, desired_external_port));
        // Repeated calls must keep returning the same address, not `Err(Stopped)`.
        let external_addr = handle.get_external_addr().await.unwrap();
        assert_eq!(external_addr, SocketAddr::new(external_ip, desired_external_port));
        let external_addr = handle.get_external_addr().await.unwrap();
        assert_eq!(external_addr, SocketAddr::new(external_ip, desired_external_port));

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_creates_mapping_with_any_port() {
        _ = simple_logger::init_with_level(Level::Debug);

        let internal_port = 12345u16;
        let assigned_port = 40000u16;
        let external_ip: IpAddr = Ipv4Addr::new(203, 0, 113, 2).into();

        let factory = make_factory_returning(move || {
            let mut gw = make_gateway_with_ip(external_ip);
            gw.expect_add_any_port()
                .once()
                .with(eq(PortMappingProtocol::UDP), always(), always(), always(), always())
                .returning(move |_, _, _, _, _| async move { Ok(assigned_port) }.boxed());
            gw.expect_remove_port()
                .once()
                .with(eq(PortMappingProtocol::UDP), eq(assigned_port), always())
                .returning(|_, _, _| async { Ok(()) }.boxed());
            gw
        });

        let mut handle = launch_impl(PortMappingProtocol::UDP, internal_port, None, None, factory);

        let external_addr = handle.get_external_addr().await.unwrap();
        assert_eq!(external_addr, SocketAddr::new(external_ip, assigned_port));

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_search_failure_is_reported_via_handle() {
        _ = simple_logger::init_with_level(Level::Debug);

        let mut factory = MockGatewayFactory::new();
        factory.expect_search().once().returning(|_opts| {
            async { Err(igd_next::SearchError::InvalidResponse.into()) }.boxed()
        });

        let mut handle = launch_impl(PortMappingProtocol::TCP, 12345, None, None, factory);

        let result = handle.get_external_addr().await;
        assert!(matches!(result, Err(Error::IgdError(_))));
        // Repeated calls must keep returning the same error, not `Err(Stopped)`.
        let result = handle.get_external_addr().await;
        assert!(matches!(result, Err(Error::IgdError(_))));
        let result = handle.get_external_addr().await;
        assert!(matches!(result, Err(Error::IgdError(_))));

        assert!(matches!(handle.shutdown().await, Err(Error::IgdError(_))));
    }

    #[tokio::test(start_paused = true)]
    async fn test_mapping_is_renewed_periodically() {
        _ = simple_logger::init_with_level(Level::Debug);

        let internal_port = 12345u16;
        let desired_external_port = 54321u16;
        let external_ip: IpAddr = Ipv4Addr::new(203, 0, 113, 3).into();

        let factory = make_factory_returning(move || {
            let mut gw = make_gateway_with_ip(external_ip);
            // initial add_port + 2 renewals
            gw.expect_add_port()
                .times(3)
                .with(
                    eq(PortMappingProtocol::TCP),
                    eq(desired_external_port),
                    always(),
                    always(),
                    always(),
                    always(),
                )
                .returning(|_, _, _, _, _, _| async { Ok(()) }.boxed());
            gw.expect_remove_port().once().returning(|_, _, _| async { Ok(()) }.boxed());
            gw
        });

        let mut handle = launch_impl(
            PortMappingProtocol::TCP,
            internal_port,
            Some(desired_external_port),
            None,
            factory,
        );

        let external_addr = handle.get_external_addr().await.unwrap();
        assert_eq!(external_addr, SocketAddr::new(external_ip, desired_external_port));

        // renewal interval is 3600s; advance past two renewals
        time::sleep(sec!(3601)).await;
        time::sleep(sec!(3601)).await;

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_renewal_retries_after_100ms() {
        _ = simple_logger::init_with_level(Level::Debug);

        let internal_port = 12345u16;
        let desired_external_port = 54321u16;
        let external_ip: IpAddr = Ipv4Addr::new(203, 0, 113, 4).into();

        // Reference point for asserting when each renewal attempt happens. The renewal timer fires
        // one period (3600s) after the mapper starts, then each retry adds a 100ms backoff.
        let start = Instant::now();

        let factory = make_factory_returning(move || {
            let mut gw = make_gateway_with_ip(external_ip);

            // Expectations are matched in the order they are added. The initial mapping succeeds,
            // then the first two renewal attempts fail and the third succeeds.
            gw.expect_add_port()
                .once()
                .returning(|_, _, _, _, _, _| async { Ok(()) }.boxed());

            gw.expect_add_port().once().returning(move |_, _, _, _, _, _| {
                assert_eq!(Instant::now() - start, sec!(3600));
                async { Err(igd_next::Error::from(igd_next::AddPortError::PortInUse).into()) }
                    .boxed()
            });

            gw.expect_add_port().once().returning(move |_, _, _, _, _, _| {
                assert_eq!(Instant::now() - start, sec!(3600) + millisec!(100));
                async { Err(igd_next::Error::from(igd_next::AddPortError::PortInUse).into()) }
                    .boxed()
            });

            gw.expect_add_port().once().returning(move |_, _, _, _, _, _| {
                assert_eq!(Instant::now() - start, sec!(3600) + millisec!(200));
                async { Ok(()) }.boxed()
            });

            gw.expect_remove_port().once().returning(|_, _, _| async { Ok(()) }.boxed());
            gw
        });

        let mut handle = launch_impl(
            PortMappingProtocol::TCP,
            internal_port,
            Some(desired_external_port),
            None,
            factory,
        );

        let external_addr = handle.get_external_addr().await.unwrap();
        assert_eq!(external_addr, SocketAddr::new(external_ip, desired_external_port));

        // advance past one renewal, which internally retries add_port with 100ms backoffs
        time::sleep(sec!(3601)).await;

        handle.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_renewal_gives_up_after_5_attempts() {
        _ = simple_logger::init_with_level(Level::Debug);

        let internal_port = 12345u16;
        let desired_external_port = 54321u16;
        let external_ip: IpAddr = Ipv4Addr::new(203, 0, 113, 5).into();

        let factory = make_factory_returning(move || {
            let mut gw = make_gateway_with_ip(external_ip);

            // initial mapping succeeds
            gw.expect_add_port()
                .once()
                .returning(|_, _, _, _, _, _| async { Ok(()) }.boxed());
            // all 5 renewal attempts fail
            gw.expect_add_port().times(5).returning(|_, _, _, _, _, _| {
                async { Err(igd_next::Error::from(igd_next::AddPortError::PortInUse).into()) }
                    .boxed()
            });
            // Task exits with an error before shutdown is signalled, so `remove_port` is never
            // called.
            gw
        });

        let mut handle = launch_impl(
            PortMappingProtocol::TCP,
            internal_port,
            Some(desired_external_port),
            None,
            factory,
        );

        let external_addr = handle.get_external_addr().await.unwrap();
        assert_eq!(external_addr, SocketAddr::new(external_ip, desired_external_port));

        // advance past one renewal; all attempts fail so the task exits with an error
        time::sleep(sec!(3601)).await;

        // `shutdown` still works even though the task has already exited on its own; the task's
        // renewal error propagates through.
        assert!(matches!(handle.shutdown().await, Err(Error::IgdError(_))));
    }

    #[ignore]
    #[tokio::test]
    async fn test_async_port_opener() {
        simple_logger::init_with_level(Level::Debug).unwrap();

        let internal_port = 12345;
        let port_opener = PortOpener::<IgdNextGatewayFactory>::new(
            PortMappingProtocol::TCP,
            internal_port,
            None,
            None,
            60,
            IgdNextGatewayFactory,
        )
        .await
        .unwrap_or_else(|e| panic!("Failed to create PortOpener: {e}"));
        log::info!("port opener created, external ip: {}", port_opener.external_addr);
        time::sleep(sec!(1)).await;
        drop(port_opener);
        log::info!("port opener dropped");
        time::sleep(sec!(1)).await;
    }
}
