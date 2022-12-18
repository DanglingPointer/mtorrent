use crate::data::Error;

pub struct PieceInfo {
    pieces: Vec<[u8; 20]>,
    piece_length: usize,
}

impl PieceInfo {
    pub fn new<'a, I: Iterator<Item = &'a [u8]>>(piece_it: I, piece_length: usize) -> Self {
        fn to_20_byte_array(slice: &[u8]) -> [u8; 20] {
            let mut ret = [0u8; 20];
            ret.copy_from_slice(slice);
            ret
        }
        PieceInfo {
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

    pub fn piece_len(&self) -> usize {
        self.piece_length
    }

    pub fn piece_count(&self) -> usize {
        self.pieces.len()
    }
}
