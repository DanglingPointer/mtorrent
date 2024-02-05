use core::fmt;
use rand::Rng;
use std::ops::Deref;

#[derive(Clone, Copy)]
pub struct PeerId([u8; 20]); // immutable wrapper

impl PeerId {
    pub fn generate_new() -> Self {
        let mut ret = [0u8; 20];
        let maj = str::parse::<u8>(env!("CARGO_PKG_VERSION_MAJOR")).unwrap_or(b'x');
        let min = str::parse::<u8>(env!("CARGO_PKG_VERSION_MINOR")).unwrap_or(b'x');
        let pat = str::parse::<u8>(env!("CARGO_PKG_VERSION_PATCH")).unwrap_or(b'x');

        let s = format!("-mt0{}{}{}-", maj, min, pat).into_bytes();
        ret[..s.len()].copy_from_slice(&s);

        for b in &mut ret[8..] {
            *b = rand::thread_rng().gen_range(32..127); // non-control characters
        }
        Self(ret)
    }
}

impl From<&[u8; 20]> for PeerId {
    fn from(value: &[u8; 20]) -> Self {
        Self(*value)
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
