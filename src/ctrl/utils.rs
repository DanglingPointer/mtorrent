use crate::pwp::BlockInfo;
use std::cmp;

const MAX_BLOCK_SIZE: usize = 16384;

#[allow(dead_code)]
pub(crate) fn divide_piece_into_blocks(
    piece_index: usize,
    piece_len: usize,
) -> impl Iterator<Item = BlockInfo> {
    (0..piece_len).step_by(MAX_BLOCK_SIZE).map(move |in_piece_offset| BlockInfo {
        piece_index,
        in_piece_offset,
        block_length: cmp::min(MAX_BLOCK_SIZE, piece_len - in_piece_offset),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_divide_piece_into_multiple_blocks() {
        let mut blocks_it = divide_piece_into_blocks(42, 2 * MAX_BLOCK_SIZE + 100);
        assert_eq!(
            BlockInfo {
                piece_index: 42,
                in_piece_offset: 0,
                block_length: MAX_BLOCK_SIZE,
            },
            blocks_it.next().unwrap()
        );
        assert_eq!(
            BlockInfo {
                piece_index: 42,
                in_piece_offset: MAX_BLOCK_SIZE,
                block_length: MAX_BLOCK_SIZE,
            },
            blocks_it.next().unwrap()
        );
        assert_eq!(
            BlockInfo {
                piece_index: 42,
                in_piece_offset: 2 * MAX_BLOCK_SIZE,
                block_length: 100,
            },
            blocks_it.next().unwrap()
        );
        assert!(blocks_it.next().is_none());
    }

    #[test]
    fn test_piece_fits_in_single_block() {
        let mut blocks_it = divide_piece_into_blocks(42, MAX_BLOCK_SIZE - 100);
        assert_eq!(
            BlockInfo {
                piece_index: 42,
                in_piece_offset: 0,
                block_length: MAX_BLOCK_SIZE - 100,
            },
            blocks_it.next().unwrap()
        );
        assert!(blocks_it.next().is_none());
    }
}
