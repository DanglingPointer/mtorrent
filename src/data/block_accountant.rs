use crate::data::{Error, PieceInfo};
use crate::pwp::{Bitfield, BlockInfo};
use core::fmt;
use std::collections::BTreeMap;
use std::rc::Rc;

pub struct BlockAccountant {
    pieces: Rc<PieceInfo>,
    blocks_start_end: BTreeMap<usize, usize>,
    total_bytes: usize,
}

impl BlockAccountant {
    pub fn new(pieces: Rc<PieceInfo>) -> Self {
        BlockAccountant {
            pieces,
            blocks_start_end: BTreeMap::new(),
            total_bytes: 0,
        }
    }

    pub fn submit_block(&mut self, block_info: &BlockInfo) -> Result<usize, Error> {
        let result = self.pieces.global_offset(
            block_info.piece_index,
            block_info.in_piece_offset,
            block_info.block_length,
        );
        if let Ok(global_offset) = result {
            self.submit_block_internal(global_offset, block_info.block_length);
        }
        result
    }

    fn submit_block_internal(&mut self, global_offset: usize, length: usize) {
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

    #[allow(dead_code)]
    pub fn submit_piece(&mut self, piece_index: usize) -> bool {
        let piece_length = self.pieces.piece_len(piece_index);
        if let Ok(offset) = self.pieces.global_offset(piece_index, 0, piece_length) {
            self.submit_block_internal(offset, piece_length);
            true
        } else {
            false
        }
    }

    #[allow(dead_code)]
    pub fn submit_bitfield(&mut self, bitfield: &Bitfield) -> bool {
        if bitfield.len() < self.pieces.piece_count() {
            return false;
        }
        for (piece_index, is_piece_present) in bitfield.iter().enumerate() {
            if *is_piece_present {
                self.submit_piece(piece_index);
            }
        }
        true
    }

    pub fn remove_piece(&mut self, piece_index: usize) {
        let piece_length = self.pieces.piece_len(piece_index);
        if let Ok(global_offset) = self.pieces.global_offset(piece_index, 0, piece_length) {
            self.remove_block_internal(global_offset, piece_length);
        }
    }

    fn remove_block_internal(&mut self, global_offset: usize, length: usize) {
        let start = global_offset;
        let end = global_offset + length;

        if let Some(prev_block) = self.blocks_start_end.range_mut(..global_offset).last() {
            let (_prev_start, prev_end) = prev_block;
            let prev_end_copy = *prev_end;
            if *prev_end > start {
                self.total_bytes -= *prev_end - start;
                *prev_end = start;
            }
            if prev_end_copy > end {
                self.blocks_start_end.insert(end, prev_end_copy);
                self.total_bytes += prev_end_copy - end;
            }
        }

        while let Some(next_block) = self.blocks_start_end.range_mut(global_offset..).next() {
            let (next_start, next_end) = { (*next_block.0, *next_block.1) };
            if next_start >= end {
                break;
            }
            self.blocks_start_end.remove(&next_start);
            self.total_bytes -= next_end - next_start;
            if next_end > end {
                self.blocks_start_end.insert(end, next_end);
                self.total_bytes += next_end - end;
            }
        }
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

    pub fn has_exact_block(&self, block_info: &BlockInfo) -> bool {
        if let Ok(global_offset) = self.pieces.global_offset(
            block_info.piece_index,
            block_info.in_piece_offset,
            block_info.block_length,
        ) {
            self.has_exact_block_at(global_offset, block_info.block_length)
        } else {
            false
        }
    }

    pub fn has_piece(&self, piece_index: usize) -> bool {
        let piece_len = self.pieces.piece_len(piece_index);
        if let Ok(global_offset) = self.pieces.global_offset(piece_index, 0, piece_len) {
            self.has_exact_block_at(global_offset, piece_len)
        } else {
            false
        }
    }

    pub fn generate_bitfield(&self) -> Bitfield {
        let mut bitfield = Bitfield::repeat(false, self.pieces.piece_count());
        for (piece_index, mut is_piece_present) in bitfield.iter_mut().enumerate() {
            if self.has_piece(piece_index) {
                is_piece_present.set(true);
            }
        }
        bitfield
    }

    pub fn accounted_bytes(&self) -> usize {
        self.total_bytes
    }

    pub fn missing_bytes(&self) -> usize {
        self.pieces.total_len() - self.total_bytes
    }
}

impl fmt::Display for BlockAccountant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bitfield = self.generate_bitfield();
        let piece_count = bitfield.iter().filter(|bit| *bit == true).count();
        write!(f, "pieces={}/{}", piece_count, bitfield.len())?;
        write!(
            f,
            " bytes={}/{}",
            self.accounted_bytes(),
            self.accounted_bytes() + self.missing_bytes()
        )?;
        write!(f, " fragments={}", self.blocks_start_end.len())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter;

    fn piece_info() -> Rc<PieceInfo> {
        Rc::new(PieceInfo::new(iter::empty(), 3, 256))
    }

    #[test]
    fn test_accountant_submit_one_block() {
        let p = piece_info();
        let mut a = BlockAccountant::new(p);
        a.submit_block_internal(10, 10);

        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&20), a.blocks_start_end.get(&10));
        assert_eq!(10, a.accounted_bytes());
    }

    #[test]
    fn test_accountant_merge_into_preceding_block() {
        let p = piece_info();
        let mut a = BlockAccountant::new(p);
        a.submit_block_internal(10, 10);
        a.submit_block_internal(20, 10);

        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&30), a.blocks_start_end.get(&10));
        assert_eq!(20, a.accounted_bytes());
    }

    #[test]
    fn test_accountant_merge_overlapping_into_preceding_block() {
        let p = piece_info();
        let mut a = BlockAccountant::new(p);
        a.submit_block_internal(10, 10);
        a.submit_block_internal(15, 15);

        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&30), a.blocks_start_end.get(&10));
        assert_eq!(20, a.accounted_bytes());
    }

    #[test]
    fn test_accountant_merge_into_following_block() {
        let p = piece_info();
        let mut a = BlockAccountant::new(p);
        a.submit_block_internal(10, 10);
        a.submit_block_internal(0, 10);

        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&20), a.blocks_start_end.get(&0));
        assert_eq!(20, a.accounted_bytes());
    }

    #[test]
    fn test_accountant_merge_overlapping_into_following_block() {
        let p = piece_info();
        let mut a = BlockAccountant::new(p);
        a.submit_block_internal(10, 10);
        a.submit_block_internal(0, 15);

        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&20), a.blocks_start_end.get(&0));
        assert_eq!(20, a.accounted_bytes());
    }

    #[test]
    fn test_accountant_replace_overlapping_block() {
        let p = piece_info();
        let mut a = BlockAccountant::new(p);
        a.submit_block_internal(10, 10);
        a.submit_block_internal(5, 20);

        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&25), a.blocks_start_end.get(&5));
        assert_eq!(20, a.accounted_bytes());
    }

    #[test]
    fn test_accountant_ignore_overlapping_block() {
        let p = piece_info();
        let mut a = BlockAccountant::new(p);
        a.submit_block_internal(5, 20);
        a.submit_block_internal(10, 10);

        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&25), a.blocks_start_end.get(&5));
        assert_eq!(20, a.accounted_bytes());
    }

    #[test]
    fn test_accountant_merge_with_following_and_preceding_blocks() {
        let p = piece_info();
        let mut a = BlockAccountant::new(p);
        a.submit_block_internal(10, 5);
        a.submit_block_internal(0, 5);

        assert_eq!(2, a.blocks_start_end.len());
        assert_eq!(Some(&5), a.blocks_start_end.get(&0));
        assert_eq!(Some(&15), a.blocks_start_end.get(&10));
        assert_eq!(10, a.accounted_bytes());

        a.submit_block_internal(5, 5);

        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&15), a.blocks_start_end.get(&0));
        assert_eq!(15, a.accounted_bytes());
    }

    #[test]
    fn test_accountant_merge_with_overlapping_following_and_preceding_blocks() {
        let p = piece_info();
        let mut a = BlockAccountant::new(p);
        a.submit_block_internal(10, 5);
        a.submit_block_internal(0, 5);

        a.submit_block_internal(2, 10);

        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&15), a.blocks_start_end.get(&0));
        assert_eq!(15, a.accounted_bytes());
    }

    #[test]
    fn test_accountant_block_length_with_one_block() {
        let p = piece_info();
        let mut a = BlockAccountant::new(p);
        a.submit_block_internal(10, 10);

        assert_eq!(None, a.max_block_length_at(9));
        assert_eq!(Some(10), a.max_block_length_at(10));
        assert_eq!(Some(9), a.max_block_length_at(11));
        assert_eq!(Some(1), a.max_block_length_at(19));
        assert_eq!(None, a.max_block_length_at(20));
    }

    #[test]
    fn test_accountant_block_length_with_two_blocks() {
        let p = piece_info();
        let mut a = BlockAccountant::new(p);
        a.submit_block_internal(10, 10);
        a.submit_block_internal(30, 10);

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
        let p = piece_info();
        let mut a = BlockAccountant::new(p);
        a.submit_block_internal(10, 10);

        for len in 0..=10 {
            assert!(!a.has_exact_block_at(9, len), "len={}", len);
            assert!(a.has_exact_block_at(10, len), "len={}", len);
        }
        assert!(a.has_exact_block_at(11, 9));
        assert!(!a.has_exact_block_at(11, 10));

        assert!(a.has_exact_block_at(19, 1));
        assert!(!a.has_exact_block_at(19, 2));
    }

    #[test]
    fn test_accountant_remove_exact_block() {
        let p = piece_info();
        let mut a = BlockAccountant::new(p);

        // given
        a.blocks_start_end.insert(0, 5);
        a.blocks_start_end.insert(10, 15);
        a.blocks_start_end.insert(20, 25);
        a.total_bytes = 15;

        // when
        a.remove_block_internal(10, 5);

        // then
        assert_eq!(2, a.blocks_start_end.len());
        assert_eq!(Some(&5), a.blocks_start_end.get(&0));
        assert_eq!(Some(&25), a.blocks_start_end.get(&20));
        assert_eq!(10, a.total_bytes);
    }

    #[test]
    fn test_accountant_shrink_block_from_tail_end() {
        let p = piece_info();
        let mut a = BlockAccountant::new(p);

        // given
        a.blocks_start_end.insert(0, 10);
        a.total_bytes = 10;

        // when
        a.remove_block_internal(5, 5);

        // then
        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&5), a.blocks_start_end.get(&0));
        assert_eq!(5, a.total_bytes);
    }

    #[test]
    fn test_accountant_shrink_block_from_head_end() {
        let p = piece_info();
        let mut a = BlockAccountant::new(p);

        // given
        a.blocks_start_end.insert(0, 10);
        a.total_bytes = 10;

        // when
        a.remove_block_internal(0, 5);

        // then
        assert_eq!(1, a.blocks_start_end.len());
        assert_eq!(Some(&10), a.blocks_start_end.get(&5));
        assert_eq!(5, a.total_bytes);
    }

    #[test]
    fn test_accountant_split_block_into_two() {
        let p = piece_info();
        let mut a = BlockAccountant::new(p);

        // given
        a.blocks_start_end.insert(0, 20);
        a.total_bytes = 20;

        // when
        a.remove_block_internal(5, 10);

        // then
        assert_eq!(2, a.blocks_start_end.len());
        assert_eq!(Some(&5), a.blocks_start_end.get(&0));
        assert_eq!(Some(&20), a.blocks_start_end.get(&15));
        assert_eq!(10, a.total_bytes);
    }

    #[test]
    fn test_accountant_remove_multiple_nonadjacent_blocks() {
        let p = piece_info();
        let mut a = BlockAccountant::new(p);

        // given
        a.blocks_start_end.insert(0, 5);
        a.blocks_start_end.insert(10, 15);
        a.blocks_start_end.insert(20, 25);
        a.blocks_start_end.insert(30, 35);
        a.total_bytes = 20;

        // when
        a.remove_block_internal(8, 20);

        // then
        assert_eq!(2, a.blocks_start_end.len());
        assert_eq!(Some(&5), a.blocks_start_end.get(&0));
        assert_eq!(Some(&35), a.blocks_start_end.get(&30));
        assert_eq!(10, a.total_bytes);
    }

    #[test]
    fn test_accountant_remove_multiple_nonadjacent_blocks_and_shrink() {
        let p = piece_info();
        let mut a = BlockAccountant::new(p);

        // given
        a.blocks_start_end.insert(0, 5);
        a.blocks_start_end.insert(10, 15);
        a.blocks_start_end.insert(20, 25);
        a.blocks_start_end.insert(30, 35);
        a.total_bytes = 20;

        // when
        a.remove_block_internal(4, 27);

        // then
        assert_eq!(2, a.blocks_start_end.len());
        assert_eq!(Some(&4), a.blocks_start_end.get(&0));
        assert_eq!(Some(&35), a.blocks_start_end.get(&31));
        assert_eq!(8, a.total_bytes);
    }
}
