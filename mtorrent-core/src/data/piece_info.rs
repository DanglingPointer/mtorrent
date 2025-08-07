use crate::data::Error;

pub struct PieceInfo {
    pieces: Vec<[u8; 20]>,
    piece_length: usize,
    total_length: usize,
}

impl PieceInfo {
    pub fn new<'a, I: Iterator<Item = &'a [u8]>>(
        piece_it: I,
        piece_length: usize,
        total_length: usize,
    ) -> Self {
        fn to_20_byte_array(slice: &[u8]) -> [u8; 20] {
            let mut ret = [0u8; 20];
            ret.copy_from_slice(slice);
            ret
        }
        PieceInfo {
            pieces: piece_it.map(to_20_byte_array).collect(),
            piece_length,
            total_length,
        }
    }

    pub fn global_offset(
        &self,
        piece_index: usize,
        in_piece_offset: usize,
        length: usize,
    ) -> Result<usize, Error> {
        let piece_length = self.piece_len(piece_index);
        if self.pieces.get(piece_index).is_none() || in_piece_offset + length > piece_length {
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

    pub fn piece_len(&self, piece_index: usize) -> usize {
        if piece_index + 1 == self.pieces.len() && self.total_length % self.piece_length != 0 {
            self.total_length % self.piece_length
        } else {
            self.piece_length
        }
    }

    pub fn piece_count(&self) -> usize {
        self.pieces.len()
    }

    pub fn total_len(&self) -> usize {
        self.total_length
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_incomplete_piece_is_handled_correctly() {
        let p = PieceInfo::new(std::iter::repeat_n([0u8; 20].as_slice(), 3), 5, 12);
        assert_eq!(3, p.piece_count());
        assert_eq!(5, p.piece_len(0));
        assert_eq!(5, p.piece_len(1));
        assert_eq!(2, p.piece_len(2));
        assert_eq!(10, p.global_offset(2, 0, 2).unwrap());
        assert_eq!(11, p.global_offset(2, 1, 1).unwrap());
        assert!(p.global_offset(2, 1, 2).is_err());
    }
}
