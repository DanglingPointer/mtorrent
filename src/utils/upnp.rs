use crate::sec;
use igd::{aio, Error, Gateway, PortMappingProtocol, SearchOptions};
use std::io;
use std::net::SocketAddrV4;
use tokio::time::Instant;

fn to_blocking_gateway(gw: aio::Gateway) -> Gateway {
    Gateway {
        addr: gw.addr,
        root_url: gw.root_url,
        control_url: gw.control_url,
        control_schema_url: gw.control_schema_url,
        control_schema: gw.control_schema,
    }
}

pub struct PortOpener {
    gateway: Option<aio::Gateway>,
    internal_ip: SocketAddrV4,
    external_ip: SocketAddrV4,
    proto: PortMappingProtocol,
    autoclose_at: Instant,
}

impl PortOpener {
    const LEASE_DURATION: std::time::Duration = sec!(300);

    pub async fn new(internal_ip: SocketAddrV4, proto: PortMappingProtocol) -> Result<Self, Error> {
        let gateway = aio::search_gateway(SearchOptions {
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

    pub fn external_ip(&self) -> SocketAddrV4 {
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
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{e}")))?;
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
