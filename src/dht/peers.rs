use super::u160::U160;
use local_async_utils::min;
use sha1_smol::Sha1;
use std::collections::{BTreeMap, HashMap, HashSet};
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

pub struct TokenManager {
    secrets: BTreeMap<Instant, u32>,
}

impl TokenManager {
    const SECRET_MAX_AGE: Duration = min!(5);
    const SECRET_GOOD_AGE: Duration = min!(4);

    pub fn new() -> Self {
        Self {
            secrets: [(Instant::now(), rand::random())].into(),
        }
    }

    pub fn generate_token_for(&mut self, addr: &SocketAddr) -> Vec<u8> {
        let secret = match self.secrets.last_entry() {
            Some(entry) if entry.key().elapsed() <= Self::SECRET_GOOD_AGE => {
                entry.get().to_be_bytes()
            }
            _ => {
                self.remove_expired_secrets();
                let new_secret: u32 = rand::random();
                self.secrets.insert(Instant::now(), new_secret);
                new_secret.to_be_bytes()
            }
        };

        let hashed_addr = hashed_socketaddr(addr);

        let mut token = Vec::with_capacity(size_of_val(&hashed_addr) + size_of_val(&secret));
        token.extend_from_slice(&secret);
        token.extend_from_slice(&hashed_addr);
        token
    }

    pub fn validate_token_from(&mut self, addr: &SocketAddr, token: &[u8]) -> bool {
        // validate hash
        let secret = match token.split_last_chunk::<20>() {
            Some((secret, hashed_addr)) if hashed_addr == &hashed_socketaddr(addr) => secret,
            _ => return false,
        };

        // validate secret
        self.remove_expired_secrets();
        match secret.try_into().map(u32::from_be_bytes) {
            Ok(secret) => self.secrets.values().any(|known_secret| known_secret == &secret),
            Err(_) => false,
        }
    }

    fn remove_expired_secrets(&mut self) {
        while let Some(entry) = self.secrets.first_entry() {
            if entry.key().elapsed() > Self::SECRET_MAX_AGE {
                entry.remove();
            } else {
                break;
            }
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
    use local_async_utils::sec;
    use std::net::Ipv4Addr;
    use tokio::time;

    #[tokio::test(start_paused = true)]
    async fn test_token_validation() {
        let mut token_mgr = TokenManager::new();
        let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 12345);

        let token = token_mgr.generate_token_for(&addr);
        assert!(token_mgr.validate_token_from(&addr, &token));

        time::sleep(TokenManager::SECRET_MAX_AGE).await;
        assert!(token_mgr.validate_token_from(&addr, &token));

        time::sleep(sec!(1)).await;
        assert!(!token_mgr.validate_token_from(&addr, &token));
        assert_eq!(token_mgr.secrets.len(), 0);
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

        time::sleep(TokenManager::SECRET_MAX_AGE - min!(1)).await;
        let new_token = token_mgr.generate_token_for(&addr);
        assert_eq!(token, new_token);

        time::sleep(sec!(1)).await;
        let new_token = token_mgr.generate_token_for(&addr);
        assert_ne!(token, new_token);

        assert!(token_mgr.validate_token_from(&addr, &token));
        assert!(token_mgr.validate_token_from(&addr, &new_token));
    }
}
