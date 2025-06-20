use igd_next::{aio, PortMappingProtocol, SearchOptions};
use local_async_utils::prelude::*;
use std::net::SocketAddr;

type AsyncGateway = aio::Gateway<aio::tokio::Tokio>;
type BlockingGateway = igd_next::Gateway;

pub struct PortOpener {
    gateway: AsyncGateway,
    internal_addr: SocketAddr,
    external_addr: SocketAddr,
    proto: PortMappingProtocol,
}

impl PortOpener {
    const LEASE_DURATION: std::time::Duration = sec!(300);

    pub async fn new(
        internal_addr: SocketAddr,
        proto: PortMappingProtocol,
        desired_external_port: Option<u16>,
    ) -> igd_next::Result<Self> {
        let gateway = aio::tokio::search_gateway(SearchOptions {
            timeout: Some(sec!(5)),
            ..Default::default()
        })
        .await?;
        let public_ip = gateway.get_external_ip().await?;
        let public_port = if let Some(desired_port) = desired_external_port {
            gateway
                .add_port(
                    proto,
                    desired_port,
                    internal_addr,
                    Self::LEASE_DURATION.as_secs() as u32,
                    "",
                )
                .await?;
            desired_port
        } else {
            gateway
                .add_any_port(proto, internal_addr, Self::LEASE_DURATION.as_secs() as u32, "")
                .await?
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

    pub fn external_ip(&self) -> SocketAddr {
        self.external_addr
    }

    pub async fn run_continuous_renewal(self) -> igd_next::Result<()> {
        loop {
            tokio::time::sleep(Self::LEASE_DURATION).await;
            self.gateway
                .add_port(
                    self.proto,
                    self.external_addr.port(),
                    self.internal_addr,
                    Self::LEASE_DURATION.as_secs() as u32,
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

fn blocking_gateway(gw: &AsyncGateway) -> BlockingGateway {
    BlockingGateway {
        addr: gw.addr,
        root_url: gw.root_url.clone(),
        control_url: gw.control_url.clone(),
        control_schema_url: gw.control_schema_url.clone(),
        control_schema: gw.control_schema.clone(),
    }
}

impl Drop for PortOpener {
    fn drop(&mut self) {
        let gateway = blocking_gateway(&self.gateway);
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
    use crate::utils::ip;
    use log::Level;
    use std::net::SocketAddrV4;
    use tokio::time;

    #[ignore]
    #[tokio::test]
    async fn test_async_port_opener() {
        simple_logger::init_with_level(Level::Debug).unwrap();

        let local_ip = ip::get_local_addr().unwrap();
        let local_internal_ip = SocketAddrV4::new(local_ip, 23015);
        let port_opener = PortOpener::new(local_internal_ip.into(), PortMappingProtocol::TCP, None)
            .await
            .unwrap_or_else(|e| panic!("Failed to create PortOpener: {e}"));
        log::info!("port opener created, external ip: {}", port_opener.external_ip());
        time::sleep(sec!(1)).await;
        drop(port_opener);
        log::info!("port opener dropped");
    }
}
