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
use tokio::{pin, select, time};

pub use igd_next::PortMappingProtocol;

/// Errors returned by [`PortMapper`] and [`PortMapperHandle`].
#[derive(Error, Debug, Clone)]
pub enum Error {
    /// An error reported by the underlying `igd_next` library.
    #[error("{0}")]
    IgdError(String),
    /// The [`PortMapper`] task has exited (or never started).
    #[error("UPnP task is not running")]
    Stopped,
    /// UPnP request didn't complete within a timeout.
    #[error("request timed out")]
    Timeout,
}

impl From<igd_next::Error> for Error {
    fn from(e: igd_next::Error) -> Self {
        Self::IgdError(e.to_string())
    }
}

impl From<Elapsed> for Error {
    fn from(_: Elapsed) -> Self {
        Self::Timeout
    }
}

/// Create a [`PortMapper`] task and a matching [`PortMapperHandle`].
///
/// The returned [`PortMapper`] must be driven via [`PortMapper::run`] to actually
/// create and maintain the port mapping on the local UPnP gateway. Dropping the
/// [`PortMapperHandle`] signals the task to remove the mapping and exit.
///
/// * `proto` — TCP or UDP mapping.
/// * `internal_port` — the local port that traffic should be forwarded to.
/// * `desired_external_port` — a specific external port to request, or `None` to let the gateway
///   pick one.
/// * `interface` — the local network interface to bind to, or `None` to use the default one.
pub fn init(
    proto: PortMappingProtocol,
    internal_port: u16,
    desired_external_port: Option<u16>,
    interface: Option<String>,
) -> (PortMapperHandle, PortMapper) {
    let (get_addr_tx, get_addr_rx) = oneshot::channel();
    let (cancel_tx, cancel_rx) = oneshot::channel();

    (
        PortMapperHandle {
            get_external_addr: GetExternalAddrStatus::InProgress(get_addr_rx),
            _canceller: cancel_tx,
        },
        PortMapper {
            proto,
            internal_port,
            desired_external_port,
            interface,
            get_external_addr_tx: get_addr_tx,
            canceller: cancel_rx,
        },
    )
}

/// A handle to a running [`PortMapper`] task.
///
/// Use it to query the external address assigned by the gateway. Dropping the
/// handle asks the task to remove the port mapping and exit.
pub struct PortMapperHandle {
    get_external_addr: GetExternalAddrStatus,
    _canceller: oneshot::Sender<()>,
}

/// The background task that creates the UPnP port mapping and renews it
/// periodically until its [`PortMapperHandle`] is dropped.
///
/// Obtained from [`init`]; call [`PortMapper::run`] to execute it.
pub struct PortMapper {
    proto: PortMappingProtocol,
    internal_port: u16,
    desired_external_port: Option<u16>,
    interface: Option<String>,

    get_external_addr_tx: oneshot::Sender<Result<SocketAddr, Error>>,
    canceller: oneshot::Receiver<()>,
}

enum GetExternalAddrStatus {
    InProgress(oneshot::Receiver<Result<SocketAddr, Error>>),
    Finished(Result<SocketAddr, Error>),
}

impl PortMapperHandle {
    /// Return the external `SocketAddr` assigned by the gateway. Will block forever if the
    /// [`PortMapper`] task hasn't been launched.
    ///
    /// Awaits the initial mapping to be created on the first call, then caches the result so
    /// subsequent calls return the same value immediately. If the [`PortMapper`] fails to create
    /// the mapping or has been dropped, returns the corresponding [`Error`](enum@Error).
    pub async fn get_external_addr(&mut self) -> Result<SocketAddr, Error> {
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
}

impl PortMapper {
    /// Create the port mapping on the local UPnP gateway and keep renewing it
    /// until the matching [`PortMapperHandle`] is dropped, at which point the
    /// mapping is removed and this future resolves.
    ///
    /// Returns an [`Error`](enum@Error) if the initial mapping cannot be created or if a
    /// later renewal fails.
    pub async fn run(self) -> Result<(), Error> {
        self.run_impl(IgdNextGatewayFactory).await
    }

    async fn run_impl<G: GatewayFactory>(self, gateway_factory: G) -> Result<(), Error> {
        /// Recommended lease duration from <https://upnp.org/specs/gw/UPnP-gw-WANIPConnection-v2-Service.pdf>.
        pub const PORT_LEASE_DURATION_SEC: u32 = 3600;

        let Self {
            proto,
            internal_port,
            desired_external_port,
            interface,
            get_external_addr_tx,
            canceller,
        } = self;

        let port_opener = match PortOpener::new(
            proto,
            internal_port,
            desired_external_port,
            interface.as_deref(),
            PORT_LEASE_DURATION_SEC,
            gateway_factory,
        )
        .await
        .map_err(Error::from)
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

        let renewal_period = sec!(PORT_LEASE_DURATION_SEC as u64);
        let mut renewal_timer = time::interval_at(Instant::now() + renewal_period, renewal_period);
        renewal_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        // Continuously renew the port mapping until the handle is dropped
        pin!(canceller);
        loop {
            select! {
                biased;
                _ = &mut canceller => {
                    port_opener.remove_mapping().await;
                    break;
                }
                _ = renewal_timer.tick() => {
                    port_opener.renew_mapping(PORT_LEASE_DURATION_SEC).await?;
                }
            }
        }

        Ok(())
    }
}

#[cfg_attr(test, mockall::automock(type Gateway = MockGateway;))]
trait GatewayFactory {
    type Gateway: Gateway;

    fn search(
        &self,
        opts: igd_next::SearchOptions,
    ) -> impl Future<Output = Result<Self::Gateway, igd_next::SearchError>>;
}

#[cfg_attr(test, mockall::automock)]
trait Gateway {
    fn get_external_ip(&self)
    -> impl Future<Output = Result<IpAddr, igd_next::GetExternalIpError>>;

    fn add_port(
        &self,
        proto: PortMappingProtocol,
        external_port: u16,
        local_addr: SocketAddr,
        lease_duration_sec: u32,
        description: &str,
    ) -> impl Future<Output = Result<(), igd_next::AddPortError>>;

    fn add_any_port(
        &self,
        proto: PortMappingProtocol,
        local_addr: SocketAddr,
        lease_duration_sec: u32,
        description: &str,
    ) -> impl Future<Output = Result<u16, igd_next::AddAnyPortError>>;

    fn remove_port(
        &self,
        proto: PortMappingProtocol,
        external_port: u16,
    ) -> impl Future<Output = Result<(), igd_next::RemovePortError>>;
}

struct IgdNextGatewayFactory;

type IgdNextGateway = igd_next::aio::Gateway<igd_next::aio::tokio::Tokio>;

impl GatewayFactory for IgdNextGatewayFactory {
    type Gateway = IgdNextGateway;

    async fn search(
        &self,
        opts: igd_next::SearchOptions,
    ) -> Result<Self::Gateway, igd_next::SearchError> {
        igd_next::aio::tokio::search_gateway(opts).await
    }
}

impl Gateway for IgdNextGateway {
    async fn get_external_ip(&self) -> Result<IpAddr, igd_next::GetExternalIpError> {
        igd_next::aio::Gateway::get_external_ip(self).await
    }

    async fn add_port(
        &self,
        proto: PortMappingProtocol,
        external_port: u16,
        local_addr: SocketAddr,
        lease_duration_sec: u32,
        description: &str,
    ) -> Result<(), igd_next::AddPortError> {
        igd_next::aio::Gateway::add_port(
            self,
            proto,
            external_port,
            local_addr,
            lease_duration_sec,
            description,
        )
        .await
    }

    async fn add_any_port(
        &self,
        proto: PortMappingProtocol,
        local_addr: SocketAddr,
        lease_duration_sec: u32,
        description: &str,
    ) -> Result<u16, igd_next::AddAnyPortError> {
        igd_next::aio::Gateway::add_any_port(
            self,
            proto,
            local_addr,
            lease_duration_sec,
            description,
        )
        .await
    }

    async fn remove_port(
        &self,
        proto: PortMappingProtocol,
        external_port: u16,
    ) -> Result<(), igd_next::RemovePortError> {
        igd_next::aio::Gateway::remove_port(self, proto, external_port).await
    }
}

/// Utility for creating and maintaining a port mapping on the local gateway via UPnP.
struct PortOpener<G: GatewayFactory> {
    gateway: G::Gateway,
    internal_addr: SocketAddr,
    external_addr: SocketAddr,
    proto: PortMappingProtocol,
}

impl<G: GatewayFactory> PortOpener<G> {
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
    ) -> Result<Self, igd_next::Error> {
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
        let public_ip = gateway.get_external_ip().await?;
        let public_port = if let Some(desired_port) = desired_external_port {
            gateway
                .add_port(proto, desired_port, internal_addr, lease_duration_sec, "")
                .await?;
            desired_port
        } else {
            gateway.add_any_port(proto, internal_addr, lease_duration_sec, "").await?
        };
        let external_addr = SocketAddr::new(public_ip, public_port);

        Ok(Self {
            gateway,
            internal_addr,
            external_addr,
            proto,
        })
    }

    async fn remove_mapping(&self) {
        let proto = self.proto;
        let external_port = self.external_addr.port();
        match Self::with_timeout(sec!(1), self.gateway.remove_port(proto, external_port)).await {
            Ok(()) => {
                log::info!("UPnP: port mapping deleted ({proto}:{external_port})");
            }
            Err(e) => {
                log::warn!("UPnP: failed to delete port mapping ({proto}:{external_port}): {e}");
            }
        }
    }

    async fn renew_mapping(&self, lease_duration_sec: u32) -> Result<(), Error> {
        let proto = self.proto;
        let external_port = self.external_addr.port();

        let mut attempts_left = 5;

        let result = loop {
            let result = Self::with_timeout(
                sec!(1),
                self.gateway.add_port(
                    proto,
                    external_port,
                    self.internal_addr,
                    lease_duration_sec,
                    "",
                ),
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

    async fn with_timeout<E: Into<igd_next::Error>>(
        timeout: Duration,
        f: impl Future<Output = Result<(), E>>,
    ) -> Result<(), Error> {
        time::timeout(timeout, f).await?.map_err(Into::into)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use log::Level;
    use mockall::predicate::{always, eq};
    use std::net::Ipv4Addr;
    use tokio::{task, time};

    fn make_gateway_with_ip(ip: IpAddr) -> MockGateway {
        let mut gw = MockGateway::new();
        gw.expect_get_external_ip()
            .returning(move || async move { Ok(ip) }.boxed_local());
        gw
    }

    fn make_factory_returning(gw: impl FnOnce() -> MockGateway + 'static) -> MockGatewayFactory {
        let mut factory = MockGatewayFactory::new();
        let mut gw_slot = Some(gw());
        factory.expect_search().once().returning(move |_opts| {
            let gw = gw_slot.take().expect("search called more than once");
            async move { Ok(gw) }.boxed_local()
        });
        factory
    }

    #[tokio::test(flavor = "local", start_paused = true)]
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
                )
                .returning(|_, _, _, _, _| async { Ok(()) }.boxed_local());
            gw.expect_remove_port()
                .once()
                .with(eq(PortMappingProtocol::TCP), eq(desired_external_port))
                .returning(|_, _| async { Ok(()) }.boxed_local());
            gw
        });

        let (mut handle, mapper) =
            init(PortMappingProtocol::TCP, internal_port, Some(desired_external_port), None);

        let run_task = task::spawn_local(mapper.run_impl(factory));

        let external_addr = handle.get_external_addr().await.unwrap();
        assert_eq!(external_addr, SocketAddr::new(external_ip, desired_external_port));
        // Repeated calls must keep returning the same address, not `Err(Stopped)`.
        let external_addr = handle.get_external_addr().await.unwrap();
        assert_eq!(external_addr, SocketAddr::new(external_ip, desired_external_port));
        let external_addr = handle.get_external_addr().await.unwrap();
        assert_eq!(external_addr, SocketAddr::new(external_ip, desired_external_port));

        drop(handle);

        task::yield_now().await;
        run_task.now_or_never().unwrap().unwrap().unwrap();
    }

    #[tokio::test(flavor = "local", start_paused = true)]
    async fn test_creates_mapping_with_any_port() {
        _ = simple_logger::init_with_level(Level::Debug);

        let internal_port = 12345u16;
        let assigned_port = 40000u16;
        let external_ip: IpAddr = Ipv4Addr::new(203, 0, 113, 2).into();

        let factory = make_factory_returning(move || {
            let mut gw = make_gateway_with_ip(external_ip);
            gw.expect_add_any_port()
                .once()
                .with(eq(PortMappingProtocol::UDP), always(), always(), always())
                .returning(move |_, _, _, _| async move { Ok(assigned_port) }.boxed_local());
            gw.expect_remove_port()
                .once()
                .with(eq(PortMappingProtocol::UDP), eq(assigned_port))
                .returning(|_, _| async { Ok(()) }.boxed_local());
            gw
        });

        let (mut handle, mapper) = init(PortMappingProtocol::UDP, internal_port, None, None);

        let run_task = task::spawn_local(mapper.run_impl(factory));

        let external_addr = handle.get_external_addr().await.unwrap();
        assert_eq!(external_addr, SocketAddr::new(external_ip, assigned_port));

        drop(handle);

        task::yield_now().await;
        run_task.now_or_never().unwrap().unwrap().unwrap();
    }

    #[tokio::test(flavor = "local", start_paused = true)]
    async fn test_search_failure_is_reported_via_handle() {
        _ = simple_logger::init_with_level(Level::Debug);

        let mut factory = MockGatewayFactory::new();
        factory
            .expect_search()
            .once()
            .returning(|_opts| async { Err(igd_next::SearchError::InvalidResponse) }.boxed_local());

        let (mut handle, mapper) = init(PortMappingProtocol::TCP, 12345, None, None);

        let run_task = task::spawn_local(mapper.run_impl(factory));

        let result = handle.get_external_addr().await;
        assert!(matches!(result, Err(Error::IgdError(_))));
        // Repeated calls must keep returning the same error, not `Err(Stopped)`.
        let result = handle.get_external_addr().await;
        assert!(matches!(result, Err(Error::IgdError(_))));
        let result = handle.get_external_addr().await;
        assert!(matches!(result, Err(Error::IgdError(_))));

        assert!(run_task.now_or_never().unwrap().unwrap().is_err());
    }

    #[tokio::test(flavor = "local", start_paused = true)]
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
                )
                .returning(|_, _, _, _, _| async { Ok(()) }.boxed_local());
            gw.expect_remove_port().once().returning(|_, _| async { Ok(()) }.boxed_local());
            gw
        });

        let (mut handle, mapper) =
            init(PortMappingProtocol::TCP, internal_port, Some(desired_external_port), None);

        let run_task = task::spawn_local(mapper.run_impl(factory));

        let external_addr = handle.get_external_addr().await.unwrap();
        assert_eq!(external_addr, SocketAddr::new(external_ip, desired_external_port));

        // renewal interval is 3600s; advance past two renewals
        time::sleep(sec!(3601)).await;
        time::sleep(sec!(3601)).await;

        drop(handle);
        task::yield_now().await;
        run_task.now_or_never().unwrap().unwrap().unwrap();
    }

    #[tokio::test(flavor = "local", start_paused = true)]
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
                .returning(|_, _, _, _, _| async { Ok(()) }.boxed_local());

            gw.expect_add_port().once().returning(move |_, _, _, _, _| {
                assert_eq!(Instant::now() - start, sec!(3600));
                async { Err(igd_next::AddPortError::PortInUse) }.boxed_local()
            });

            gw.expect_add_port().once().returning(move |_, _, _, _, _| {
                assert_eq!(Instant::now() - start, sec!(3600) + millisec!(100));
                async { Err(igd_next::AddPortError::PortInUse) }.boxed_local()
            });

            gw.expect_add_port().once().returning(move |_, _, _, _, _| {
                assert_eq!(Instant::now() - start, sec!(3600) + millisec!(200));
                async { Ok(()) }.boxed_local()
            });

            gw.expect_remove_port().once().returning(|_, _| async { Ok(()) }.boxed_local());
            gw
        });

        let (mut handle, mapper) =
            init(PortMappingProtocol::TCP, internal_port, Some(desired_external_port), None);

        let run_task = task::spawn_local(mapper.run_impl(factory));

        let external_addr = handle.get_external_addr().await.unwrap();
        assert_eq!(external_addr, SocketAddr::new(external_ip, desired_external_port));

        // advance past one renewal, which internally retries add_port with 100ms backoffs
        time::sleep(sec!(3601)).await;

        drop(handle);
        task::yield_now().await;
        run_task.now_or_never().unwrap().unwrap().unwrap();
    }

    #[tokio::test(flavor = "local", start_paused = true)]
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
                .returning(|_, _, _, _, _| async { Ok(()) }.boxed_local());
            // all 5 renewal attempts fail
            gw.expect_add_port().times(5).returning(|_, _, _, _, _| {
                async { Err(igd_next::AddPortError::PortInUse) }.boxed_local()
            });
            gw
        });

        let (mut handle, mapper) =
            init(PortMappingProtocol::TCP, internal_port, Some(desired_external_port), None);

        let run_task = task::spawn_local(mapper.run_impl(factory));

        let external_addr = handle.get_external_addr().await.unwrap();
        assert_eq!(external_addr, SocketAddr::new(external_ip, desired_external_port));

        // advance past one renewal; all attempts fail so run_impl returns an error
        time::sleep(sec!(3601)).await;

        task::yield_now().await;
        let result = run_task.now_or_never().unwrap().unwrap();
        assert!(matches!(result, Err(Error::IgdError(_))));

        drop(handle);
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
