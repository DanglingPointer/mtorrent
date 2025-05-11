use super::error::Error;
use crate::utils::benc;
use bitvec::prelude::*;
use derive_more::Debug;
use std::fmt;
use std::ops::BitXor;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[debug("{self}")]
pub struct U160(pub(super) BitArray<[u8; 20], Msb0>);

impl From<[u8; 20]> for U160 {
    fn from(value: [u8; 20]) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for U160 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0.data {
            write!(f, "{byte:x?}")?;
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
                let bytes: [u8; 20] =
                    bytes.try_into().map_err(|_| Error::ParseError("U160 not 20 bytes long"))?;
                Ok(bytes.into())
            }
            _ => Err(Error::ParseError("U160 not a byte string")),
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
}
