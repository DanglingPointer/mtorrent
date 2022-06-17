use crate::storage::Error;
use bitvec::prelude::*;
use std::collections::BTreeMap;
use std::rc::Rc;

pub struct PieceKeeper {
    pieces: Vec<[u8; 20]>,
    piece_length: usize,
}

impl PieceKeeper {
    pub fn new<'a, I: Iterator<Item = &'a [u8]>>(piece_it: I, piece_length: usize) -> Self {
        fn to_20_byte_array(slice: &[u8]) -> [u8; 20] {
            let mut ret = [0u8; 20];
            ret.copy_from_slice(slice);
            ret
        }
        PieceKeeper {
            pieces: piece_it.map(to_20_byte_array).collect(),
            piece_length,
        }
    }

    pub fn global_offset(
        &self,
        piece_index: usize,
        in_piece_offset: usize,
        length: usize,
    ) -> Result<usize, Error> {
        if self.pieces.get(piece_index).is_none() || in_piece_offset + length > self.piece_length {
            Err(Error::InvalidLocation)
        } else {
            Ok(self.piece_length * piece_index + in_piece_offset)
        }
    }

    pub fn index_of_piece(&self, hash: &[u8; 20]) -> Result<usize, Error> {
        self.pieces.iter().position(|e| e == hash).ok_or(Error::InvalidLocation)
    }

    pub fn hash_of_piece(&self, piece_index: usize) -> Result<&[u8; 20], Error> {
        self.pieces.get(piece_index).ok_or(Error::InvalidLocation)
    }
}

pub struct Accountant {
    pieces: Rc<PieceKeeper>,
    blocks_start_end: BTreeMap<usize, usize>,
    total_bytes: usize,
}

impl Accountant {
    pub fn new(pieces: Rc<PieceKeeper>) -> Self {
        Accountant {
            pieces,
            blocks_start_end: BTreeMap::new(),
            total_bytes: 0,
        }
    }

    pub fn submit_block(&mut self, global_offset: usize, length: usize) {
        let start = global_offset;
        let mut end = global_offset + length;

        while let Some(next_block) = self.blocks_start_end.range_mut(global_offset..).next() {
            let (next_start, next_end) = { (*next_block.0, *next_block.1) };
            if next_start > end {
                break;
            }
            if next_end > end {
                end = next_end;
            }
            self.blocks_start_end.remove(&next_start);
            self.total_bytes -= next_end - next_start;
        }

        if let Some(prev_block) = self.blocks_start_end.range_mut(..global_offset).last() {
            let (_prev_start, prev_end) = prev_block;
            if *prev_end >= start {
                if end > *prev_end {
                    self.total_bytes += end - *prev_end;
                    *prev_end = end;
                }
                return;
            }
        }

        self.blocks_start_end.insert(start, end);
        self.total_bytes += end - start;
    }

    pub fn submit_piece(&mut self, piece_index: usize) -> bool {
        if piece_index >= self.pieces.pieces.len() {
            return false;
        }
        let piece_length = self.pieces.piece_length;
        let offset = self
            .pieces
            .global_offset(piece_index, 0, piece_length)
            .expect("This should never happen");
        self.submit_block(offset, piece_length);
        true
    }

    pub fn submit_bitfield(&mut self, bitfield: &BitVec<u8, Msb0>) -> bool {
        if bitfield.len() < self.pieces.pieces.len() {
            return false;
        }
        for (piece_index, is_piece_present) in bitfield.iter().enumerate() {
            if *is_piece_present {
                self.submit_piece(piece_index);
            }
        }
        true
    }

    pub fn max_block_length_at(&self, global_offset: usize) -> Option<usize> {
        if let Some((_start, end)) = self.blocks_start_end.range(..=global_offset).last() {
            if *end > global_offset {
                Some(*end - global_offset)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn has_exact_block_at(&self, global_offset: usize, length: usize) -> bool {
        if let Some(block_length) = self.max_block_length_at(global_offset) {
            block_length >= length
        } else {
            false
        }
    }

    pub fn generate_bitfield(&self) -> BitVec<u8, Msb0> {
        let mut bitfield = BitVec::<u8, Msb0>::repeat(false, self.pieces.pieces.len());
        let piece_length = self.pieces.piece_length;
        for (piece_index, mut is_piece_present) in bitfield.iter_mut().enumerate() {
            let global_offset = self
                .pieces
                .global_offset(piece_index, 0, piece_length)
                .expect("This should never happen");
            if self.has_exact_block_at(global_offset, piece_length) {
                is_piece_present.set(true);
            }
        }
        bitfield
    }

    pub fn accounted_bytes(&self) -> usize {
        self.total_bytes
    }

    pub fn missing_bytes(&self) -> usize {
        self.pieces.pieces.len() * self.pieces.piece_length - self.total_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter;

    #[test]
    fn test_accountant_submit_one_block() {
        let p = Rc::new(PieceKeeper::new(iter::empty(), 3));
        let mut a = Accountant::new(p);
        a.submit_block(10, 10);

        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&20), a.blocks_start_end.get(&10));
        assert_eq!(10, a.accounted_bytes());
    }

    #[test]
    fn test_accountant_merge_into_preceding_block() {
        let p = Rc::new(PieceKeeper::new(iter::empty(), 3));
        let mut a = Accountant::new(p);
        a.submit_block(10, 10);
        a.submit_block(20, 10);

        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&30), a.blocks_start_end.get(&10));
        assert_eq!(20, a.accounted_bytes());
    }

    #[test]
    fn test_accountant_merge_overlapping_into_preceding_block() {
        let p = Rc::new(PieceKeeper::new(iter::empty(), 3));
        let mut a = Accountant::new(p);
        a.submit_block(10, 10);
        a.submit_block(15, 15);

        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&30), a.blocks_start_end.get(&10));
        assert_eq!(20, a.accounted_bytes());
    }

    #[test]
    fn test_accountant_merge_into_following_block() {
        let p = Rc::new(PieceKeeper::new(iter::empty(), 3));
        let mut a = Accountant::new(p);
        a.submit_block(10, 10);
        a.submit_block(0, 10);

        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&20), a.blocks_start_end.get(&0));
        assert_eq!(20, a.accounted_bytes());
    }

    #[test]
    fn test_accountant_merge_overlapping_into_following_block() {
        let p = Rc::new(PieceKeeper::new(iter::empty(), 3));
        let mut a = Accountant::new(p);
        a.submit_block(10, 10);
        a.submit_block(0, 15);

        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&20), a.blocks_start_end.get(&0));
        assert_eq!(20, a.accounted_bytes());
    }

    #[test]
    fn test_accountant_replace_overlapping_block() {
        let p = Rc::new(PieceKeeper::new(iter::empty(), 3));
        let mut a = Accountant::new(p);
        a.submit_block(10, 10);
        a.submit_block(5, 20);

        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&25), a.blocks_start_end.get(&5));
        assert_eq!(20, a.accounted_bytes());
    }

    #[test]
    fn test_accountant_ignore_overlapping_block() {
        let p = Rc::new(PieceKeeper::new(iter::empty(), 3));
        let mut a = Accountant::new(p);
        a.submit_block(5, 20);
        a.submit_block(10, 10);

        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&25), a.blocks_start_end.get(&5));
        assert_eq!(20, a.accounted_bytes());
    }

    #[test]
    fn test_accountant_merge_with_following_and_preceding_blocks() {
        let p = Rc::new(PieceKeeper::new(iter::empty(), 3));
        let mut a = Accountant::new(p);
        a.submit_block(10, 5);
        a.submit_block(0, 5);

        assert_eq!(2, a.blocks_start_end.len());
        assert_eq!(Some(&5), a.blocks_start_end.get(&0));
        assert_eq!(Some(&15), a.blocks_start_end.get(&10));
        assert_eq!(10, a.accounted_bytes());

        a.submit_block(5, 5);

        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&15), a.blocks_start_end.get(&0));
        assert_eq!(15, a.accounted_bytes());
    }

    #[test]
    fn test_accountant_merge_with_overlapping_following_and_preceding_blocks() {
        let p = Rc::new(PieceKeeper::new(iter::empty(), 3));
        let mut a = Accountant::new(p);
        a.submit_block(10, 5);
        a.submit_block(0, 5);

        a.submit_block(2, 10);

        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&15), a.blocks_start_end.get(&0));
        assert_eq!(15, a.accounted_bytes());
    }

    #[test]
    fn test_accountant_block_length_with_one_block() {
        let p = Rc::new(PieceKeeper::new(iter::empty(), 3));
        let mut a = Accountant::new(p);
        a.submit_block(10, 10);

        assert_eq!(None, a.max_block_length_at(9));
        assert_eq!(Some(10), a.max_block_length_at(10));
        assert_eq!(Some(9), a.max_block_length_at(11));
        assert_eq!(Some(1), a.max_block_length_at(19));
        assert_eq!(None, a.max_block_length_at(20));
    }

    #[test]
    fn test_accountant_block_length_with_two_blocks() {
        let p = Rc::new(PieceKeeper::new(iter::empty(), 3));
        let mut a = Accountant::new(p);
        a.submit_block(10, 10);
        a.submit_block(30, 10);

        assert_eq!(Some(1), a.max_block_length_at(19));
        for pos in 20..30 {
            assert_eq!(None, a.max_block_length_at(pos), "pos={}", pos);
        }
        assert_eq!(Some(10), a.max_block_length_at(30));
        assert_eq!(Some(9), a.max_block_length_at(31));
        assert_eq!(Some(1), a.max_block_length_at(39));
        assert_eq!(None, a.max_block_length_at(40));
    }

    #[test]
    fn test_accountant_has_exact_block_with_one_block() {
        let p = Rc::new(PieceKeeper::new(iter::empty(), 3));
        let mut a = Accountant::new(p);
        a.submit_block(10, 10);

        for len in 0..=10 {
            assert!(!a.has_exact_block_at(9, len), "len={}", len);
            assert!(a.has_exact_block_at(10, len), "len={}", len);
        }
        assert!(a.has_exact_block_at(11, 9));
        assert!(!a.has_exact_block_at(11, 10));

        assert!(a.has_exact_block_at(19, 1));
        assert!(!a.has_exact_block_at(19, 2));
    }
}
