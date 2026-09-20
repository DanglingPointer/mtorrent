mod connection;
mod handle;
mod protocol;
mod retransmitter;
mod seq;
mod udp;

#[cfg(test)]
mod tests;

use local_async_utils::prelude::*;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

pub use handle::{DataStream, EndpointHandle, InboundConnectData, InboundListener};
pub use udp::IoDriver;

/// Creates a new uTP endpoint using the given UDP socket, returning an [`EndpointHandle`] for
/// creating connections, an [`InboundListener`] for accepting incoming connection attempts, and an
/// [`IoDriver`] that must be run to drive the endpoint's I/O.
pub fn new_endpoint(socket: UdpSocket) -> (EndpointHandle, InboundListener, IoDriver) {
    let (cmd_sender, cmd_receiver) = mpsc::channel(1);
    let (connect_sender, connect_receiver) = local_bounded::channel(64);

    (
        EndpointHandle::new(cmd_sender),
        InboundListener::new(connect_receiver),
        IoDriver::new(cmd_receiver, socket, connect_sender),
    )
}
