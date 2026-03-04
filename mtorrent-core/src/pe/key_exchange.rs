use crypto_bigint::modular::ConstMontyForm;
use crypto_bigint::{Random, U768, const_monty_params};
use rand::rng;

const_monty_params!(
    DhPrime,
    U768,
    "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A63A36210000000000090563"
);

fn random_local_secret() -> U768 {
    U768::random_from_rng(&mut rng())
}

/// (2 ^ local_secret) % dh_prime
fn local_pubkey(local_secret: &U768) -> U768 {
    const TWO: U768 = U768::from_u8(2);
    ConstMontyForm::<DhPrime, _>::new(&TWO).pow(local_secret).retrieve()
}

/// (remote_pubkey ^ local_secret) % dh_prime
fn shared_secret(local_secret: &U768, remote_pubkey: &U768) -> U768 {
    ConstMontyForm::<DhPrime, _>::new(remote_pubkey).pow(local_secret).retrieve()
}

/// Finite Field Diffie–Hellman key exchange.
pub struct DhKeyExchange {
    local_secret: U768,
    local_pubkey: U768,
}

impl Default for DhKeyExchange {
    fn default() -> Self {
        let local_secret = random_local_secret();
        let local_pubkey = local_pubkey(&local_secret);
        Self {
            local_secret,
            local_pubkey,
        }
    }
}

impl DhKeyExchange {
    pub const KEY_SIZE: usize = U768::BYTES;

    pub fn local_pubkey(&self) -> &U768 {
        &self.local_pubkey
    }

    pub fn into_shared_secret(self, remote_pubkey: &U768) -> U768 {
        shared_secret(&self.local_secret, remote_pubkey)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crypto_bigint::modular::ConstMontyParams;

    #[test]
    fn test_dh_prime() {
        let dh_prime: U768 = U768::from_str_radix_vartime(
            "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A63A36210000000000090563",
            16,
        ).unwrap();

        assert_eq!(&dh_prime, DhPrime::PARAMS.modulus());

        for _ in 0..100 {
            let r = U768::random_from_rng(&mut rng());
            assert_eq!(ConstMontyForm::<DhPrime, _>::new(&r).retrieve(), r % dh_prime);
        }
    }

    #[test]
    fn test_dh_a_and_b_shared_secret() {
        let a_secret = random_local_secret();
        let b_secret = random_local_secret();

        let a_pubkey = local_pubkey(&a_secret);
        let b_pubkey = local_pubkey(&b_secret);

        let a_shared = shared_secret(&a_secret, &b_pubkey);
        let b_shared = shared_secret(&b_secret, &a_pubkey);

        assert_eq!(a_shared, b_shared);
    }
}
