use super::error::Error;
use bitvec::prelude::*;
use derive_more::{Debug, Deref};
use mtorrent_utils::benc;
use std::ops::BitXor;
use std::str;
use std::{fmt, iter};

/// 160-bit unsigned integer that supports bitwise XOR and comparisons.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Deref)]
#[debug("{self}")]
pub(crate) struct U160(BitArray<[u8; 20], Msb0>);

impl str::FromStr for U160 {
    type Err = String;

    fn from_str(hex_str: &str) -> Result<Self, Self::Err> {
        if hex_str.len() != 40 {
            return Err(format!("input length is {}, expected 40", hex_str.len()));
        }
        let mut bytes = [0u8; 20];
        for (src, dest) in iter::zip(hex_str.as_bytes().chunks_exact(2), bytes.iter_mut()) {
            let src_str = str::from_utf8(src).map_err(|e| e.to_string())?;
            *dest = u8::from_str_radix(src_str, 16).map_err(|e| e.to_string())?;
        }
        Ok(U160::from(bytes))
    }
}

impl From<[u8; 20]> for U160 {
    fn from(value: [u8; 20]) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for U160 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0.data {
            write!(f, "{byte:02x?}")?;
        }
        Ok(())
    }
}

impl BitXor<Self> for U160 {
    type Output = Self;

    fn bitxor(self, rhs: Self) -> Self::Output {
        Self(self.0.bitxor(rhs.0))
    }
}

impl From<U160> for benc::Element {
    fn from(value: U160) -> Self {
        benc::Element::ByteString(value.0.data.into())
    }
}

impl TryFrom<benc::Element> for U160 {
    type Error = Error;

    fn try_from(value: benc::Element) -> Result<Self, Self::Error> {
        match value {
            benc::Element::ByteString(bytes) => {
                let bytes: [u8; 20] = bytes.try_into().map_err(|e: Vec<u8>| {
                    Error::ParseError(format!("U160 requires 20 bytes, not {}", e.len()).into())
                })?;
                Ok(bytes.into())
            }
            _ => Err(Error::ParseError("U160 not a byte string".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_u160() {
        let mut lhs = U160::from([0b1000_0000; 20]);
        let mut rhs = U160::from([0b0100_0000; 20]);
        assert!(lhs > rhs);

        rhs.0.data[0] = u8::MAX;
        lhs.0.data[1] = u8::MAX;
        assert!(lhs < rhs);
    }

    #[test]
    fn test_first_nonzero_bit() {
        let val = U160::from([0b0010_0000; 20]).0;
        assert_eq!(Some(2), val.first_one());
    }

    #[test]
    fn test_print_u160() {
        assert_eq!(U160::from([0x0f; 20]).to_string(), "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f");
    }
}
