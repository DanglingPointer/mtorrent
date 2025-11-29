use derive_more::{Display, From};
use std::cmp::Ordering;
use std::ops::{Add, AddAssign, Sub, SubAssign};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Display, From)]
pub struct Seq(u16);

impl Seq {
    pub const ZERO: Self = Seq(0);
    pub const ONE: Self = Seq(1);

    pub fn increment(&mut self) {
        *self += Self::ONE;
    }
}

#[cfg_attr(not(test), expect(dead_code))]
#[inline]
pub fn seq(seq_nr: u16) -> Seq {
    Seq(seq_nr)
}

impl From<Seq> for u16 {
    fn from(value: Seq) -> Self {
        value.0
    }
}

impl Add for Seq {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Seq(self.0.wrapping_add(rhs.0))
    }
}

impl Sub for Seq {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Seq(self.0.wrapping_sub(rhs.0))
    }
}

impl AddAssign for Seq {
    fn add_assign(&mut self, rhs: Self) {
        self.0 = self.0.wrapping_add(rhs.0);
    }
}

impl SubAssign for Seq {
    fn sub_assign(&mut self, rhs: Self) {
        self.0 = self.0.wrapping_sub(rhs.0);
    }
}

impl PartialOrd for Seq {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        fn first_before_second(first: Seq, second: Seq) -> bool {
            let Seq(distance) = second - first;
            distance <= u16::MAX / 2
        }

        if *self == *other {
            return Some(Ordering::Equal);
        }

        if first_before_second(*self, *other) && !first_before_second(*other, *self) {
            return Some(Ordering::Less);
        }

        if first_before_second(*other, *self) && !first_before_second(*self, *other) {
            return Some(Ordering::Greater);
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seq_comparisons() {
        let seq1 = Seq(u16::MAX);
        let seq2 = Seq(0);
        assert!(seq1 < seq2);
        assert!(seq2 > seq1);

        let seq1 = Seq(u16::MAX - 1);
        let seq2 = Seq(u16::MAX);
        assert!(seq1 < seq2);
        assert!(seq2 > seq1);

        let seq1 = Seq(0);
        let seq2 = Seq(u16::MAX / 2);
        assert!(seq1 < seq2);
        assert!(seq2 > seq1);

        let seq1 = Seq(u16::MAX / 2 + 1);
        let seq2 = Seq(0);
        assert_eq!(seq1.partial_cmp(&seq2), None);
        assert_eq!(seq2.partial_cmp(&seq1), None);
    }
}
