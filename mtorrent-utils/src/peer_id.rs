use core::fmt;
use rand::Rng;
use std::ops::Deref;

#[derive(Clone, Copy)]
pub struct PeerId([u8; 20]); // immutable wrapper

impl PeerId {
    pub fn generate_new() -> Self {
        const PREFIX: &[u8] = concat!(
            "-mt0",
            env!("CARGO_PKG_VERSION_MAJOR"),
            env!("CARGO_PKG_VERSION_MINOR"),
            env!("CARGO_PKG_VERSION_PATCH"),
        )
        .as_bytes();

        let mut ret = [0u8; 20];
        ret[..PREFIX.len()].copy_from_slice(PREFIX);

        for b in &mut ret[PREFIX.len()..] {
            *b = rand::rng().random_range(33..127); // non-control characters and not space
        }
        Self(ret)
    }
}

impl From<&[u8; 20]> for PeerId {
    fn from(value: &[u8; 20]) -> Self {
        Self(*value)
    }
}

impl From<[u8; 20]> for PeerId {
    fn from(value: [u8; 20]) -> Self {
        Self(value)
    }
}

impl Deref for PeerId {
    type Target = [u8; 20];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        String::from_utf8_lossy(&self.0).fmt(f)
    }
}
