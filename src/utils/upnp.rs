use igd::{search_gateway, Error, Gateway, PortMappingProtocol, SearchOptions};
use std::net::SocketAddrV4;
use std::time::{Duration, Instant};

pub struct PortOpener {
    gateway: Gateway,
    external_ip: SocketAddrV4,
    proto: PortMappingProtocol,
    autoclose_at: Instant,
}

impl PortOpener {
    pub fn new(
        internal_ip: SocketAddrV4,
        proto: PortMappingProtocol,
        duration: Duration,
    ) -> Result<Self, Error> {
        let gateway = search_gateway(SearchOptions {
            timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        })?;
        let external_ip =
            gateway.get_any_address(proto, internal_ip, duration.as_secs() as u32, "")?;
        Ok(Self {
            gateway,
            external_ip,
            proto,
            autoclose_at: Instant::now() + duration,
        })
    }
    pub fn external_ip(&self) -> SocketAddrV4 {
        self.external_ip
    }
}

impl Drop for PortOpener {
    fn drop(&mut self) {
        if Instant::now() < self.autoclose_at {
            let _ = self.gateway.remove_port(self.proto, self.external_ip.port());
        }
    }
}
