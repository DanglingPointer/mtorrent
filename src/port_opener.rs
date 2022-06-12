use igd::{search_gateway, Error, Gateway, PortMappingProtocol, SearchOptions};
use std::net::SocketAddrV4;
use std::time::Duration;

pub struct PortOpener {
    gateway: Gateway,
    external_ip: SocketAddrV4,
    proto: PortMappingProtocol,
}

impl PortOpener {
    pub fn new(internal_ip: SocketAddrV4, proto: PortMappingProtocol) -> Result<Self, Error> {
        let gateway = search_gateway(SearchOptions {
            timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        })?;
        let external_ip = gateway.get_any_address(proto, internal_ip, 0, "")?;
        Ok(Self {
            gateway,
            external_ip,
            proto,
        })
    }
    pub fn external_ip(&self) -> SocketAddrV4 {
        self.external_ip
    }
}

impl Drop for PortOpener {
    fn drop(&mut self) {
        let _ = self.gateway.remove_port(self.proto, self.external_ip.port());
    }
}
