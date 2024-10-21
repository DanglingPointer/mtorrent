use super::error::Error;
use crate::utils::benc;
use bitvec::prelude::*;
use std::{fmt, iter, ops};

/// 160-bit unsigned integer in big endian that can be XOR'ed, compared and bit-shifted.
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

impl ops::ShrAssign<usize> for U160 {
    fn shr_assign(&mut self, rhs: usize) {
        self.0.as_mut_bits::<Msb0>().shift_right(rhs);
    }
}

impl ops::Shr<usize> for U160 {
    type Output = Self;

    fn shr(mut self, rhs: usize) -> Self::Output {
        self >>= rhs;
        self
    }
}

impl ops::ShlAssign<usize> for U160 {
    fn shl_assign(&mut self, rhs: usize) {
        self.0.as_mut_bits::<Msb0>().shift_left(rhs);
    }
}

impl ops::Shl<usize> for U160 {
    type Output = Self;

    fn shl(mut self, rhs: usize) -> Self::Output {
        self <<= rhs;
        self
    }
}

impl ops::Not for U160 {
    type Output = Self;

    fn not(mut self) -> Self::Output {
        for byte in &mut self.0 {
            *byte = !*byte;
        }
        U160(self.0)
    }
}

impl ops::AddAssign for U160 {
    fn add_assign(&mut self, rhs: Self) {
        let mut carry = false;
        for (left, right) in iter::zip(&mut self.0, rhs.0).rev() {
            let (sum, new_carry) = left.overflowing_add(right);
            *left = sum.wrapping_add(carry as u8);
            carry = new_carry;
        }
    }
}

impl ops::Add for U160 {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        self += rhs;
        self
    }
}

impl ops::SubAssign for U160 {
    fn sub_assign(&mut self, rhs: Self) {
        use std::ops::AddAssign;
        const ONE: U160 = const {
            let mut one = [0u8; 20];
            one[19] = 1;
            U160(one)
        };
        self.add_assign(!rhs);
        self.add_assign(ONE);
    }
}

impl ops::Sub for U160 {
    type Output = Self;

    fn sub(mut self, rhs: Self) -> Self::Output {
        self -= rhs;
        self
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

    #[test]
    fn test_shr_u160() {
        let a: U160 = [0b00100001; 20].into();
        let mut expected = [0b10010000; 20];
        expected[0] = 0b00010000;
        assert_eq!(a >> 1, expected.into());
    }

    #[test]
    fn test_shl_u160() {
        let a: U160 = [0b10000100; 20].into();
        let mut expected = [0b00001001; 20];
        expected[19] = 0b00001000;
        assert_eq!(a << 1, expected.into());
    }

    #[test]
    fn test_not_u160() {
        let a: U160 = [0b10101010; 20].into();
        assert_eq!(!a, [0b01010101; 20].into());
    }

    #[test]
    fn test_add_u160() {
        let a: U160 = [0xff; 20].into();
        let b: U160 = [0x01; 20].into();
        let mut expected = [0x01; 20];
        expected[19] = 0x00;

        assert_eq!(a + b, expected.into());
    }

    #[test]
    fn test_sub_u160() {
        let a: U160 = [0xff; 20].into();
        let b: U160 = [0x01; 20].into();

        assert_eq!(a - b, [0xfe; 20].into());
    }

    #[test]
    fn test_sub_u160_with_borrow() {
        let mut a = [0x00; 20];
        a[0] = 0x01;
        let a: U160 = a.into();

        let mut b = [0x00; 20];
        b[19] = 0x01;
        let b: U160 = b.into();

        let mut expected = [0xff; 20];
        expected[0] = 0x00;

        assert_eq!(a - b, expected.into());
    }
}
