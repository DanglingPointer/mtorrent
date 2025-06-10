use super::u160::U160;
use local_async_utils::prelude::*;
use sha1_smol::Sha1;
use std::collections::{HashMap, HashSet};
use std::iter;
use std::net::{IpAddr, SocketAddr, SocketAddrV4};
use std::time::Duration;
use tokio::time::Instant;

pub struct PeerTable {
    info_hash_to_peers: HashMap<U160, HashSet<SocketAddr>>,
}

impl PeerTable {
    pub fn new() -> Self {
        Self {
            info_hash_to_peers: Default::default(),
        }
    }

    pub fn add_record(&mut self, info_hash: &U160, peer_addr: SocketAddr) {
        self.info_hash_to_peers.entry(*info_hash).or_default().insert(peer_addr);
    }

    pub fn get_peers(&self, info_hash: &U160) -> Box<dyn Iterator<Item = &SocketAddr> + '_> {
        if let Some(peers) = self.info_hash_to_peers.get(info_hash) {
            Box::new(peers.iter())
        } else {
            Box::new(iter::empty())
        }
    }

    pub fn get_ipv4_peers(&self, info_hash: &U160) -> impl Iterator<Item = &SocketAddrV4> + '_ {
        self.get_peers(info_hash).filter_map(|addr| match addr {
            SocketAddr::V4(addr) => Some(addr),
            SocketAddr::V6(_) => None,
        })
    }
}

struct Secret {
    birthtime: Instant,
    data: u32,
}

pub struct TokenManager {
    usable_secret: Option<Secret>,
    acceptable_secret: Option<Secret>,
}

impl TokenManager {
    const SECRET_MAX_USABLE_AGE: Duration = min!(5);
    const SECRET_MAX_ACCEPTABLE_AGE: Duration = min!(10);

    pub fn new() -> Self {
        Self {
            usable_secret: None,
            acceptable_secret: None,
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

        let secret = self.usable_secret.get_or_insert_with(random_secret).data.to_be_bytes();
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
                secret_matches(&self.usable_secret, secret)
                    || secret_matches(&self.acceptable_secret, secret)
            }
            Err(_) => false,
        }
    }

    fn remove_expired_secrets(&mut self) {
        fn exceeded_age(secret: &Option<Secret>, max_age: Duration) -> bool {
            secret.as_ref().is_some_and(|secret| secret.birthtime.elapsed() > max_age)
        }

        if exceeded_age(&self.usable_secret, Self::SECRET_MAX_USABLE_AGE) {
            assert!(!self
                .acceptable_secret
                .as_ref()
                .is_some_and(|s| s.birthtime.elapsed() <= Self::SECRET_MAX_ACCEPTABLE_AGE));
            self.acceptable_secret = self.usable_secret.take();
        }
        if exceeded_age(&self.acceptable_secret, Self::SECRET_MAX_ACCEPTABLE_AGE) {
            self.acceptable_secret = None;
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
        assert!(token_mgr.usable_secret.is_some());
        assert!(token_mgr.acceptable_secret.is_none());

        time::sleep_until(start_time + TokenManager::SECRET_MAX_USABLE_AGE + sec!(1)).await;
        assert!(token_mgr.validate_token_from(&addr, &token));
        assert!(token_mgr.usable_secret.is_none());
        assert!(token_mgr.acceptable_secret.is_some());

        time::sleep_until(start_time + TokenManager::SECRET_MAX_ACCEPTABLE_AGE).await;
        assert!(token_mgr.validate_token_from(&addr, &token));
        assert!(token_mgr.usable_secret.is_none());
        assert!(token_mgr.acceptable_secret.is_some());

        time::sleep(sec!(1)).await;
        assert!(!token_mgr.validate_token_from(&addr, &token));
        assert!(token_mgr.usable_secret.is_none());
        assert!(token_mgr.acceptable_secret.is_none());
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

        time::sleep(TokenManager::SECRET_MAX_USABLE_AGE).await;
        let new_token = token_mgr.generate_token_for(&addr);
        assert_eq!(token, new_token);
        assert!(token_mgr.usable_secret.is_some());
        assert!(token_mgr.acceptable_secret.is_none());

        time::sleep(sec!(1)).await;
        let new_token = token_mgr.generate_token_for(&addr);
        assert_ne!(token, new_token);
        assert!(token_mgr.usable_secret.is_some());
        assert!(token_mgr.acceptable_secret.is_some());

        assert!(token_mgr.validate_token_from(&addr, &token));
        assert!(token_mgr.validate_token_from(&addr, &new_token));
    }

    #[test]
    fn test_valid_max_age_constants() {
        assert!(TokenManager::SECRET_MAX_USABLE_AGE * 2 >= TokenManager::SECRET_MAX_ACCEPTABLE_AGE);
    }
}
