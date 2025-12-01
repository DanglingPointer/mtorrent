use crate::fifo_set::BoundedFifoSet;
use local_async_utils::prelude::*;
use std::fmt;
use std::net::SocketAddr;
use std::rc::Rc;

/// Connection slot for a given remote address. No other connections to the same remote address
/// will be allowed until [`ConnectPermit`] goes out of scope. Each permit takes up one capacity slot.
pub struct ConnectPermit {
    addr: SocketAddr,
    connected_peers: Rc<sealed::Set<SocketAddr>>,
    slot: Option<local_semaphore::Permit>,
}

impl ConnectPermit {
    /// Free up the capacity slot used by this permit.
    pub fn release_slot(&mut self) {
        self.slot.take();
    }
}

impl fmt::Debug for ConnectPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectPermit").field("addr", &self.addr).finish()
    }
}

impl Drop for ConnectPermit {
    fn drop(&mut self) {
        self.connected_peers.remove(&self.addr);
    }
}

/// Utility for limiting the number of simultaneous connections and avoiding duplicates.
pub struct ConnectThrottle {
    connected_peers: Rc<sealed::Set<SocketAddr>>,
    known_peers: BoundedFifoSet<SocketAddr>,
    capacity_sem: local_semaphore::Semaphore,
}

impl ConnectThrottle {
    /// Create new throttle that allows up to `max_connections` concurrent permits, and keeps track of
    /// `remembered_peers` last addresses that have been issued a permit.
    pub fn new(max_connections: usize, remembered_peers: usize) -> Self {
        Self {
            connected_peers: Rc::new(sealed::Set::with_capacity(max_connections)),
            known_peers: BoundedFifoSet::new(remembered_peers),
            capacity_sem: local_semaphore::Semaphore::new(max_connections),
        }
    }

    /// Wait for a slot for an outbound connection or return immediately if the maximum capacity hasn't
    /// been reached yet. Returns [`None`] if already connected to `remote_addr`, or was connected until
    /// recently (with the exception of reconnects), otherwise returns a permit for the given remote address.
    pub async fn permit_for_outbound(
        &mut self,
        remote_addr: SocketAddr,
        reconnect: bool,
    ) -> Option<ConnectPermit> {
        let slot = self.capacity_sem.acquire_permit().await;

        if !self.known_peers.insert_or_replace(remote_addr) && !reconnect {
            log::debug!("No connect permit for {remote_addr}: known address and not a reconnect");
            return None;
        }

        if !self.connected_peers.insert(remote_addr) {
            log::debug!("No connect permit for {remote_addr}: already exists");
            return None;
        }

        Some(ConnectPermit {
            addr: remote_addr,
            connected_peers: self.connected_peers.clone(),
            slot: Some(slot),
        })
    }

    /// Get an immediate permit for an inbound connection. Returns [`None`] if the maximum
    /// capacity has been reached, otherwise returns a permit for the given remote address.
    pub fn permit_for_inbound(&mut self, remote_addr: SocketAddr) -> Option<ConnectPermit> {
        if let Some(slot) = self.capacity_sem.try_acquire_permit() {
            self.connected_peers.insert(remote_addr);
            self.known_peers.insert_or_replace(remote_addr);
            Some(ConnectPermit {
                addr: remote_addr,
                connected_peers: self.connected_peers.clone(),
                slot: Some(slot),
            })
        } else {
            log::warn!("Incoming connection from {remote_addr} rejected");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use tokio_test::{assert_pending, assert_ready, task::spawn};

    macro_rules! addr {
        ($addr:literal) => {
            SocketAddr::from_str($addr).unwrap()
        };
    }

    #[test]
    fn test_respect_capacity() {
        let mut ct = ConnectThrottle::new(2, 100);

        let permit1 =
            assert_ready!(spawn(ct.permit_for_outbound(addr!("1.1.1.1:1111"), false)).poll())
                .unwrap();
        assert_eq!(permit1.addr, addr!("1.1.1.1:1111"));

        let permit2 = ct.permit_for_inbound(addr!("1.1.1.1:2222")).unwrap();
        assert_eq!(permit2.addr, addr!("1.1.1.1:2222"));

        let no_inbound_permit = ct.permit_for_inbound(addr!("1.1.1.1:3333"));
        assert!(no_inbound_permit.is_none());

        let mut outbound_permit2_fut = spawn(ct.permit_for_outbound(addr!("1.1.1.1:3333"), false));
        assert_pending!(outbound_permit2_fut.poll());

        drop(permit2);
        let mut permit3 = assert_ready!(outbound_permit2_fut.poll()).unwrap();
        assert_eq!(permit3.addr, addr!("1.1.1.1:3333"));
        drop(outbound_permit2_fut);

        drop(permit1);
        let permit4 = ct.permit_for_inbound(addr!("1.1.1.1:4444")).unwrap();
        assert_eq!(permit4.addr, addr!("1.1.1.1:4444"));

        let no_inbound_permit = ct.permit_for_inbound(addr!("1.1.1.1:5555"));
        assert!(no_inbound_permit.is_none());

        permit3.release_slot();
        let permit5 = ct.permit_for_inbound(addr!("1.1.1.1:5555")).unwrap();
        assert_eq!(permit5.addr, addr!("1.1.1.1:5555"));
    }

    #[test]
    fn test_ignore_outbound_duplicates() {
        let mut ct = ConnectThrottle::new(10, 100);

        let permit1 =
            assert_ready!(spawn(ct.permit_for_outbound(addr!("1.1.1.1:1111"), false)).poll())
                .unwrap();
        assert_eq!(permit1.addr, addr!("1.1.1.1:1111"));

        let no_outbound_dup =
            assert_ready!(spawn(ct.permit_for_outbound(addr!("1.1.1.1:1111"), false)).poll());
        assert!(no_outbound_dup.is_none());

        let inbound_dup_permit = ct.permit_for_inbound(addr!("1.1.1.1:1111")).unwrap();
        assert_eq!(inbound_dup_permit.addr, addr!("1.1.1.1:1111"));

        let permit2 =
            assert_ready!(spawn(ct.permit_for_outbound(addr!("1.1.1.1:2222"), false)).poll())
                .unwrap();
        assert_eq!(permit2.addr, addr!("1.1.1.1:2222"));
    }

    #[test]
    fn test_remember_disconnected_peers() {
        let mut ct = ConnectThrottle::new(10, 100);

        let permit1 =
            assert_ready!(spawn(ct.permit_for_outbound(addr!("1.1.1.1:1111"), false)).poll())
                .unwrap();
        assert_eq!(permit1.addr, addr!("1.1.1.1:1111"));

        drop(permit1);
        let no_outbound_dup =
            assert_ready!(spawn(ct.permit_for_outbound(addr!("1.1.1.1:1111"), false)).poll());
        assert!(no_outbound_dup.is_none());

        let permit1_reconnect =
            assert_ready!(spawn(ct.permit_for_outbound(addr!("1.1.1.1:1111"), true)).poll())
                .unwrap();
        assert_eq!(permit1_reconnect.addr, addr!("1.1.1.1:1111"));
    }
}
