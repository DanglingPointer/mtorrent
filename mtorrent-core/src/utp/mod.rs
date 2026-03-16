mod connection;
mod endpoint;
mod protocol;
mod retransmitter;
mod seq;
mod udp;

#[cfg(test)]
mod tests;

use local_async_utils::prelude::*;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

pub use endpoint::{DataStream, Endpoint, InboundConnectData, InboundListener};
pub use udp::UdpDemux;

/// Initialize the uTP subsystem.
pub fn init(socket: UdpSocket) -> (Endpoint, InboundListener, UdpDemux) {
    let (cmd_sender, cmd_receiver) = mpsc::channel(1);
    let (connect_sender, connect_receiver) = local_bounded::channel(64);

    (
        Endpoint::new(cmd_sender),
        InboundListener::new(connect_receiver),
        UdpDemux::new(cmd_receiver, socket, connect_sender),
    )
}
