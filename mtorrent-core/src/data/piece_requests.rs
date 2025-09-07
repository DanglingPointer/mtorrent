use core::fmt;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

/// Keeps track of outstanding requests with piece-level granularity (as opposed to blocks).
#[derive(Default, Debug)]
pub struct PendingRequests {
    piece_requested_from: HashMap<usize, HashSet<SocketAddr>>,
}

impl PendingRequests {
    /// Add a record of a new request sent to `peer` asking for the piece index `piece`.
    pub fn add(&mut self, piece: usize, peer: &SocketAddr) {
        self.piece_requested_from.entry(piece).or_default().insert(*peer);
    }

    /// Forget all pending requests asking peers for the piece index `piece`.
    pub fn clear_requests_of(&mut self, piece: usize) {
        self.piece_requested_from.remove(&piece);
    }

    /// Forget all pending requests sent to `peer`.
    pub fn clear_requests_to(&mut self, peer: &SocketAddr) {
        for peers in self.piece_requested_from.values_mut() {
            peers.remove(peer);
        }
    }

    /// Check presense of any pending requests asking for the piece index `piece`.
    pub fn is_piece_requested(&self, piece: usize) -> bool {
        self.piece_requested_from.get(&piece).is_some_and(|peers| !peers.is_empty())
    }

    /// Check if a request asking for the piece index `piece` has been sent to `peer`.
    pub fn is_piece_requested_from(&self, peer: &SocketAddr, piece: usize) -> bool {
        self.piece_requested_from.get(&piece).is_some_and(|peers| peers.contains(peer))
    }
}

impl fmt::Display for PendingRequests {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let requested_pieces =
            self.piece_requested_from.iter().filter(|(_, peers)| !peers.is_empty()).count();
        let request_count = self.piece_requested_from.values().flatten().count();
        write!(f, "pieces/requests={requested_pieces}/{request_count}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    #[test]
    fn test_pending_requests_from_single_peer() {
        let peer = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666));
        let mut pr = PendingRequests::default();

        pr.add(42, &peer);
        pr.add(43, &peer);
        pr.add(44, &peer);
        assert!(pr.is_piece_requested(42));
        assert!(pr.is_piece_requested(43));
        assert!(pr.is_piece_requested(44));

        pr.clear_requests_of(43);
        assert!(pr.is_piece_requested(42));
        assert!(!pr.is_piece_requested(43));
        assert!(pr.is_piece_requested(44));

        pr.clear_requests_to(&peer);
        assert!(!pr.is_piece_requested(42));
        assert!(!pr.is_piece_requested(43));
        assert!(!pr.is_piece_requested(44));
    }

    #[test]
    fn test_pending_requests_from_multiple_peers() {
        let peer1 = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666));
        let peer2 = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6667));
        let peer3 = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6668));
        let mut pr = PendingRequests::default();

        pr.add(42, &peer1);
        pr.add(42, &peer2);
        pr.add(42, &peer3);
        assert!(pr.is_piece_requested(42));

        pr.clear_requests_to(&peer2);
        assert!(pr.is_piece_requested(42));

        pr.clear_requests_of(42);
        assert!(!pr.is_piece_requested(42));
    }
}
