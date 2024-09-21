use super::error::Error;
use crate::utils::benc;
use std::{fmt, iter, ops};

/// 160-bit unsigned integer in big endian that can be XOR'ed and compared.
#[derive(PartialEq, PartialOrd, Eq, Ord)]
pub struct U160([u8; 20]);

impl AsRef<[u8]> for U160 {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<U160> for benc::Element {
    fn from(value: U160) -> Self {
        benc::Element::ByteString(value.0.into())
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

impl From<[u8; 20]> for U160 {
    fn from(value: [u8; 20]) -> Self {
        Self(value)
    }
}

impl From<&[u8; 20]> for U160 {
    fn from(value: &[u8; 20]) -> Self {
        Self(*value)
    }
}

impl ops::BitXor for U160 {
    type Output = Self;

    fn bitxor(self, rhs: Self) -> Self::Output {
        let mut tmp = [0u8; 20];
        for ((left, right), out) in iter::zip(self.0, rhs.0).zip(&mut tmp) {
            *out = left ^ right;
        }
        tmp.into()
    }
}

impl ops::BitXorAssign for U160 {
    fn bitxor_assign(&mut self, rhs: Self) {
        for (self_byte, other_byte) in iter::zip(&mut self.0, rhs.0) {
            *self_byte ^= other_byte;
        }
    }
}

impl fmt::Debug for U160 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "\\x{byte:X?}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_u160() {
        let mut a = [0u8; 20];
        a[0] = 1u8;
        let a: U160 = a.into();

        let mut b = [0u8; 20];
        b[19] = 1u8;
        let b: U160 = b.into();

        assert!(a > b);
    }

    #[test]
    fn test_xor_u160() {
        let a: U160 = [0b10101010; 20].into();
        let b: U160 = [0b01010101; 20].into();

        assert_eq!(a ^ b, [0xff; 20].into());
    }

    #[test]
    fn test_xor_assign_u160() {
        let mut a: U160 = [0b10101010; 20].into();
        a ^= [0b01010101; 20].into();

        assert_eq!(a, [0xff; 20].into());
    }
}
