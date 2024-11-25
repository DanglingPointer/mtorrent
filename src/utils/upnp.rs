use crate::sec;
use igd_next::{aio, Error, Gateway, PortMappingProtocol, SearchOptions};
use std::io;
use std::net::SocketAddr;
use tokio::time::Instant;

type AsyncGateway = aio::Gateway<aio::tokio::Tokio>;

fn to_blocking_gateway(gw: AsyncGateway) -> Gateway {
    Gateway {
        addr: gw.addr,
        root_url: gw.root_url,
        control_url: gw.control_url,
        control_schema_url: gw.control_schema_url,
        control_schema: gw.control_schema,
    }
}

pub struct PortOpener {
    gateway: Option<AsyncGateway>,
    internal_ip: SocketAddr,
    external_ip: SocketAddr,
    proto: PortMappingProtocol,
    autoclose_at: Instant,
}

impl PortOpener {
    const LEASE_DURATION: std::time::Duration = sec!(300);

    pub async fn new(internal_ip: SocketAddr, proto: PortMappingProtocol) -> Result<Self, Error> {
        let gateway = aio::tokio::search_gateway(SearchOptions {
            timeout: Some(sec!(5)),
            ..Default::default()
        })
        .await?;
        let external_ip = gateway
            .get_any_address(proto, internal_ip, Self::LEASE_DURATION.as_secs() as u32, "")
            .await?;
        log::debug!("UPnP: port mapping created ({}:{})", proto, external_ip.port());
        Ok(Self {
            gateway: Some(gateway),
            internal_ip,
            external_ip,
            proto,
            autoclose_at: Instant::now() + Self::LEASE_DURATION,
        })
    }

    pub fn external_ip(&self) -> SocketAddr {
        self.external_ip
    }

    pub async fn do_continuous_renewal(mut self) -> io::Result<()> {
        loop {
            tokio::time::sleep_until(self.autoclose_at).await;
            self.gateway
                .as_ref()
                .ok_or(io::Error::new(io::ErrorKind::Other, "no gateway"))?
                .add_port(
                    self.proto,
                    self.external_ip.port(),
                    self.internal_ip,
                    Self::LEASE_DURATION.as_secs() as u32,
                    "",
                )
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, Box::new(e)))?;
            self.autoclose_at = Instant::now() + Self::LEASE_DURATION;
            log::debug!("UPnP: port mapping renewed ({}:{})", self.proto, self.external_ip.port());
        }
    }
}

impl Drop for PortOpener {
    fn drop(&mut self) {
        if Instant::now() >= self.autoclose_at {
            return;
        }
        if let Some(async_gw) = self.gateway.take() {
            let blocking_gw = to_blocking_gateway(async_gw);
            match blocking_gw.remove_port(self.proto, self.external_ip.port()) {
                Ok(_) => log::debug!(
                    "UPnP: port mapping deleted ({}:{})",
                    self.proto,
                    self.external_ip.port()
                ),
                Err(e) => log::warn!(
                    "UPnP: failed to delete port mapping ({}:{}): {}",
                    self.proto,
                    self.external_ip.port(),
                    e
                ),
            }
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
        let port_opener = PortOpener::new(local_internal_ip.into(), PortMappingProtocol::TCP)
            .await
            .unwrap();
        log::info!("port opener created, external ip: {}", port_opener.external_ip());
        time::sleep(sec!(1)).await;
        drop(port_opener);
        log::info!("port opener dropped");
    }
}
