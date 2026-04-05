use crate::net;
use igd_next::{SearchOptions, aio};
use local_async_utils::prelude::*;
use std::mem;
use std::net::{Ipv4Addr, SocketAddr};
use tokio::time::sleep;

type AsyncGateway = aio::Gateway<aio::tokio::Tokio>;
type BlockingGateway = igd_next::Gateway;

pub use igd_next::PortMappingProtocol;

pub struct PortOpener {
    gateway: AsyncGateway,
    internal_addr: SocketAddr,
    external_addr: SocketAddr,
    proto: PortMappingProtocol,
}

impl PortOpener {
    /// Recommended lease duration from <https://upnp.org/specs/gw/UPnP-gw-WANIPConnection-v2-Service.pdf>.
    const LEASE_DURATION_SEC: u32 = 3600;

    /// Create a port mapping on the local gateway and return a `PortOpener` that maintains it.
    ///
    /// The mapping will be automatically removed when the `PortOpener` is dropped, but it will not
    /// be renewed unless `run_continuous_renewal()` is called.
    pub async fn new(
        proto: PortMappingProtocol,
        internal_port: u16,
        desired_external_port: Option<u16>,
        interface: Option<&str>,
    ) -> igd_next::Result<Self> {
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
        let gateway = aio::tokio::search_gateway(SearchOptions {
            timeout: Some(sec!(5)),
            bind_addr: (internal_ip, 0).into(),
            ..Default::default()
        })
        .await?;

        // create port mapping and get our external IP and port
        let public_ip = gateway.get_external_ip().await?;
        let public_port = if let Some(desired_port) = desired_external_port {
            gateway
                .add_port(proto, desired_port, internal_addr, Self::LEASE_DURATION_SEC, "")
                .await?;
            desired_port
        } else {
            gateway.add_any_port(proto, internal_addr, Self::LEASE_DURATION_SEC, "").await?
        };
        let external_addr = SocketAddr::new(public_ip, public_port);

        log::debug!("UPnP: port mapping created ({}:{})", proto, external_addr.port());
        Ok(Self {
            gateway,
            internal_addr,
            external_addr,
            proto,
        })
    }

    /// Get the external socket address that was mapped to the internal port.
    pub fn external_ip(&self) -> SocketAddr {
        self.external_addr
    }

    /// Start continuous renewal of the port mapping. The mapping will be automatically removed when
    /// the returned future is dropped.
    pub async fn run_continuous_renewal(self) -> igd_next::Result<()> {
        // renewal interval must be slightly higher than the lease duration because renewing a
        // mapping that hasn't expired yet has no effect
        let renewal_interval = sec!(Self::LEASE_DURATION_SEC as u64) + millisec!(500);
        loop {
            sleep(renewal_interval).await;

            self.gateway
                .add_port(
                    self.proto,
                    self.external_addr.port(),
                    self.internal_addr,
                    Self::LEASE_DURATION_SEC,
                    "",
                )
                .await?;

            log::debug!(
                "UPnP: port mapping renewed ({}:{})",
                self.proto,
                self.external_addr.port()
            );
        }
    }
}

impl Drop for PortOpener {
    /// Remove the port mapping when the `PortOpener` is dropped. Note that this is a blocking
    /// operation because [`AsyncDrop`](https://doc.rust-lang.org/std/future/trait.AsyncDrop.html) is experimental.
    fn drop(&mut self) {
        let gateway = BlockingGateway {
            addr: self.gateway.addr,
            root_url: mem::take(&mut self.gateway.root_url),
            control_url: mem::take(&mut self.gateway.control_url),
            control_schema_url: mem::take(&mut self.gateway.control_schema_url),
            control_schema: mem::take(&mut self.gateway.control_schema),
        };
        match gateway.remove_port(self.proto, self.external_addr.port()) {
            Ok(()) => log::info!(
                "UPnP: port mapping deleted ({}:{})",
                self.proto,
                self.external_addr.port()
            ),
            Err(e) => log::warn!(
                "UPnP: failed to delete port mapping ({}:{}): {}",
                self.proto,
                self.external_addr.port(),
                e
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::Level;
    use tokio::time;

    #[ignore]
    #[tokio::test]
    async fn test_async_port_opener() {
        simple_logger::init_with_level(Level::Debug).unwrap();

        let internal_port = 12345;
        let port_opener = PortOpener::new(PortMappingProtocol::TCP, internal_port, None, None)
            .await
            .unwrap_or_else(|e| panic!("Failed to create PortOpener: {e}"));
        log::info!("port opener created, external ip: {}", port_opener.external_ip());
        time::sleep(sec!(1)).await;
        drop(port_opener);
        log::info!("port opener dropped");
    }
}
