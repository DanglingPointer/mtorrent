use crate::fifo_set::BoundedFifoSet;
use local_async_utils::prelude::*;
use std::fmt;
use std::net::SocketAddr;
use std::rc::Rc;

/// A utility for tracking connected and known peers, allowing for connection permits to be granted
/// or denied based on whether a peer is already connected or known.
#[derive(Debug)]
pub struct ConnectRecorder {
    connected_peers: Rc<sealed::Set<SocketAddr>>,
    recent_peers: BoundedFifoSet<SocketAddr>,
}

/// A record of a connected peer, which will automatically remove the peer from the connected set
/// when dropped.
pub struct ConnectRecord {
    addr: SocketAddr,
    connected_peers: Rc<sealed::Set<SocketAddr>>,
}

impl fmt::Debug for ConnectRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectRecord").field("addr", &self.addr).finish()
    }
}

impl Drop for ConnectRecord {
    fn drop(&mut self) {
        self.connected_peers.remove(&self.addr);
    }
}

impl ConnectRecorder {
    /// Creates a new [`ConnectRecorder`] with the specified number of recent peers to track. Once
    /// the number of recent peers exceeds this limit, the oldest recent peer will be forgotten.
    pub fn new(max_recent_peers: usize) -> Self {
        Self {
            connected_peers: Rc::new(sealed::Set::new()),
            recent_peers: BoundedFifoSet::new(max_recent_peers),
        }
    }

    /// Creates a new connection record for the specified remote address. If the remote address is
    /// already connected or it was recently seen and it's not a reconnect, it will be rejected and
    /// `None` will be returned. Otherwise, it will be added to the connected set and a
    /// [`ConnectRecord`] will be returned.
    pub fn create_record(
        &mut self,
        remote_addr: SocketAddr,
        reconnect: bool,
    ) -> Option<ConnectRecord> {
        if !self.recent_peers.insert_or_replace(remote_addr) && !reconnect {
            log::debug!("No connect permit for {remote_addr}: recent address and not a reconnect");
            return None;
        }

        if !self.connected_peers.insert(remote_addr) {
            log::debug!("No connect permit for {remote_addr}: already exists");
            return None;
        }

        Some(ConnectRecord {
            addr: remote_addr,
            connected_peers: self.connected_peers.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_connect_recorder() {
        let mut recorder = ConnectRecorder::new(2);
        let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), 1234);
        let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(5, 6, 7, 8)), 5678);
        let addr3 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(9, 10, 11, 12)), 9101);

        // Create a record for addr1
        let record1 = recorder.create_record(addr1, false);
        assert!(record1.is_some());

        // Create a record for addr2
        let record2 = recorder.create_record(addr2, false);
        assert!(record2.is_some());

        // Attempt to create a record for addr1 again (should fail because it's already connected)
        let record1_again = recorder.create_record(addr1, false);
        assert!(record1_again.is_none());

        drop(record1);

        // Attempt to create a record for addr1 again (should fail because it's still in recent
        // peers)
        let record1_again = recorder.create_record(addr1, false);
        assert!(record1_again.is_none());

        // Create a record for addr3 (should evict addr1 from recent peers)
        let record3 = recorder.create_record(addr3, false);
        assert!(record3.is_some());

        // Now addr1 should be allowed again since it was evicted from recent peers
        let record1_after_eviction = recorder.create_record(addr1, false);
        assert!(record1_after_eviction.is_some());

        drop(record3);

        // Attempt to create a record for addr3 again (should fail because it's still in recent
        // peers)
        let record3_again = recorder.create_record(addr3, false);
        assert!(record3_again.is_none());

        // Now test reconnecting addr3
        let record3_reconnect = recorder.create_record(addr3, true);
        assert!(record3_reconnect.is_some());
    }
}
