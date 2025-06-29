use super::u160::U160;
use local_async_utils::prelude::*;
use sha1_smol::Sha1;
use std::collections::{HashSet, VecDeque};
use std::net::{IpAddr, SocketAddr, SocketAddrV4};
use std::time::Duration;
use tokio::time::Instant;

#[derive(PartialEq, Eq, Hash, Clone)]
struct Peer {
    info_hash: U160,
    addr: SocketAddr,
}

/// A FIFO queue with each entry containing a peer's address and the info hash of their torrent.
pub struct PeerTable {
    ringbuf: VecDeque<Peer>,
    set: HashSet<Peer>, // for performance
}

impl PeerTable {
    const MAX_CAPACITY: usize = 1024 * 4;

    pub fn new() -> Self {
        Self {
            ringbuf: VecDeque::with_capacity(Self::MAX_CAPACITY),
            set: HashSet::with_capacity(Self::MAX_CAPACITY + 1),
        }
    }

    pub fn add_record(&mut self, info_hash: U160, peer_addr: SocketAddr) {
        let peer = Peer {
            info_hash,
            addr: peer_addr,
        };
        if self.set.insert(peer.clone()) {
            if self.ringbuf.len() == Self::MAX_CAPACITY {
                let popped = self.ringbuf.pop_front().unwrap();
                self.set.remove(&popped);
            }
            self.ringbuf.push_back(peer);
        }
    }

    pub fn get_peers(&self, info_hash: U160) -> impl Iterator<Item = &SocketAddr> + '_ {
        self.ringbuf
            .iter()
            .rev()
            .filter_map(move |peer| (peer.info_hash == info_hash).then_some(&peer.addr))
    }

    pub fn get_ipv4_peers(&self, info_hash: U160) -> impl Iterator<Item = &SocketAddrV4> + '_ {
        self.ringbuf.iter().rev().filter_map(move |peer| match &peer.addr {
            SocketAddr::V4(addr) if peer.info_hash == info_hash => Some(addr),
            _ => None,
        })
    }
}

struct Secret {
    birthtime: Instant,
    data: u32,
}

pub struct TokenManager {
    current_secret: Option<Secret>,
    previous_secret: Option<Secret>,
}

impl TokenManager {
    const SECRET_MAX_AGE_FOR_TX: Duration = min!(5);
    const SECRET_MAX_AGE_FOR_RX: Duration = min!(10);

    pub fn new() -> Self {
        Self {
            current_secret: None,
            previous_secret: None,
        }
    }

    pub fn generate_token_for(&mut self, addr: &SocketAddr) -> Vec<u8> {
        fn random_secret() -> Secret {
            Secret {
                birthtime: Instant::now(),
                data: rand::random(),
            }
        }

        self.remove_expired_secrets();

        let secret = self.current_secret.get_or_insert_with(random_secret).data.to_be_bytes();
        let hashed_addr = hashed_socketaddr(addr);

        let mut token = Vec::with_capacity(size_of_val(&hashed_addr) + size_of_val(&secret));
        token.extend_from_slice(&secret);
        token.extend_from_slice(&hashed_addr);
        token
    }

    pub fn validate_token_from(&mut self, addr: &SocketAddr, token: &[u8]) -> bool {
        fn secret_matches(known_secret: &Option<Secret>, secret: u32) -> bool {
            known_secret.as_ref().is_some_and(|known| known.data == secret)
        }

        // validate hash
        let secret = match token.split_last_chunk::<20>() {
            Some((secret, hashed_addr)) if hashed_addr == &hashed_socketaddr(addr) => secret,
            _ => return false,
        };

        // validate secret
        self.remove_expired_secrets();
        match secret.try_into().map(u32::from_be_bytes) {
            Ok(secret) => {
                secret_matches(&self.current_secret, secret)
                    || secret_matches(&self.previous_secret, secret)
            }
            Err(_) => false,
        }
    }

    fn remove_expired_secrets(&mut self) {
        fn exceeded_age(secret: &Option<Secret>, max_age: Duration) -> bool {
            secret.as_ref().is_some_and(|secret| secret.birthtime.elapsed() > max_age)
        }

        if exceeded_age(&self.current_secret, Self::SECRET_MAX_AGE_FOR_TX) {
            assert!(
                self.previous_secret
                    .as_ref()
                    .is_none_or(|s| s.birthtime.elapsed() > Self::SECRET_MAX_AGE_FOR_RX)
            );
            self.previous_secret = self.current_secret.take();
        }
        if exceeded_age(&self.previous_secret, Self::SECRET_MAX_AGE_FOR_RX) {
            self.previous_secret = None;
        }
    }
}

fn hashed_socketaddr(addr: &SocketAddr) -> [u8; 20] {
    let mut sha1 = Sha1::new();
    match addr.ip() {
        IpAddr::V4(ip) => sha1.update(&ip.octets()),
        IpAddr::V6(ip) => sha1.update(&ip.octets()),
    }
    sha1.update(&addr.port().to_be_bytes());
    sha1.digest().bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use tokio::time;

    #[tokio::test(start_paused = true)]
    async fn test_token_validation() {
        let start_time = Instant::now();

        let mut token_mgr = TokenManager::new();
        let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 12345);

        let token = token_mgr.generate_token_for(&addr);
        assert!(token_mgr.validate_token_from(&addr, &token));
        assert!(token_mgr.current_secret.is_some());
        assert!(token_mgr.previous_secret.is_none());

        time::sleep_until(start_time + TokenManager::SECRET_MAX_AGE_FOR_TX + sec!(1)).await;
        assert!(token_mgr.validate_token_from(&addr, &token));
        assert!(token_mgr.current_secret.is_none());
        assert!(token_mgr.previous_secret.is_some());

        time::sleep_until(start_time + TokenManager::SECRET_MAX_AGE_FOR_RX).await;
        assert!(token_mgr.validate_token_from(&addr, &token));
        assert!(token_mgr.current_secret.is_none());
        assert!(token_mgr.previous_secret.is_some());

        time::sleep(sec!(1)).await;
        assert!(!token_mgr.validate_token_from(&addr, &token));
        assert!(token_mgr.current_secret.is_none());
        assert!(token_mgr.previous_secret.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn test_token_generation() {
        let mut token_mgr = TokenManager::new();
        let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 12345);

        let token = token_mgr.generate_token_for(&addr);
        assert_eq!(
            token[4..],
            [
                220, 73, 150, 214, 127, 172, 14, 88, 116, 128, 204, 9, 126, 208, 218, 243, 230, 59,
                3, 166
            ]
        );

        time::sleep(TokenManager::SECRET_MAX_AGE_FOR_TX).await;
        let new_token = token_mgr.generate_token_for(&addr);
        assert_eq!(token, new_token);
        assert!(token_mgr.current_secret.is_some());
        assert!(token_mgr.previous_secret.is_none());

        time::sleep(sec!(1)).await;
        let new_token = token_mgr.generate_token_for(&addr);
        assert_ne!(token, new_token);
        assert!(token_mgr.current_secret.is_some());
        assert!(token_mgr.previous_secret.is_some());

        assert!(token_mgr.validate_token_from(&addr, &token));
        assert!(token_mgr.validate_token_from(&addr, &new_token));
    }

    #[test]
    fn test_valid_max_age_constants() {
        assert!(TokenManager::SECRET_MAX_AGE_FOR_TX * 2 >= TokenManager::SECRET_MAX_AGE_FOR_RX);
    }

    #[test]
    fn test_peer_table_rotation() {
        let mut pt = PeerTable::new();
        for port in 0..PeerTable::MAX_CAPACITY + 10 {
            pt.add_record(
                [0u8; 20].into(),
                SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port as u16),
            );
        }
        assert_eq!(pt.ringbuf.len(), PeerTable::MAX_CAPACITY);
        assert_eq!(pt.set.len(), PeerTable::MAX_CAPACITY);
        assert_eq!(pt.ringbuf.front().unwrap().addr.port(), 10u16);
        assert_eq!(pt.ringbuf.back().unwrap().addr.port(), (PeerTable::MAX_CAPACITY + 9) as u16);

        // insert the same peer multiple times
        pt.add_record([1u8; 20].into(), "1.1.1.1:12345".parse().unwrap());
        pt.add_record([1u8; 20].into(), "1.1.1.1:12345".parse().unwrap());
        pt.add_record([1u8; 20].into(), "1.1.1.1:12345".parse().unwrap());

        assert_eq!(pt.ringbuf.len(), PeerTable::MAX_CAPACITY);
        assert_eq!(pt.set.len(), PeerTable::MAX_CAPACITY);
        assert_eq!(pt.ringbuf.front().unwrap().addr.port(), 11u16);
        assert_eq!(pt.ringbuf.back().unwrap().addr.port(), 12345);
        assert_eq!(pt.get_peers([0u8; 20].into()).count(), PeerTable::MAX_CAPACITY - 1);
        assert_eq!(pt.get_peers([1u8; 20].into()).count(), 1);

        // reinsert a peer that has been removed
        pt.add_record([0u8; 20].into(), SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0));

        assert_eq!(pt.ringbuf.len(), PeerTable::MAX_CAPACITY);
        assert_eq!(pt.set.len(), PeerTable::MAX_CAPACITY);
        assert_eq!(pt.ringbuf.front().unwrap().addr.port(), 12u16);
        assert_eq!(pt.ringbuf.back().unwrap().addr.port(), 0);
        assert_eq!(pt.get_peers([0u8; 20].into()).count(), PeerTable::MAX_CAPACITY - 1);
        assert_eq!(pt.get_peers([1u8; 20].into()).count(), 1);
    }
}
