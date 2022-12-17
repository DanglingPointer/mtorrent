#![allow(dead_code)] // temp
use bitvec::prelude::*;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;

pub struct PieceAvailability {
    piece_index_to_owners: HashMap<usize, HashSet<SocketAddr>>,
    owner_count_to_piece_indices: BTreeMap<usize, HashSet<usize>>,
}

impl PieceAvailability {
    pub fn new(piece_count: usize) -> Self {
        let indices = (0..piece_count).collect::<HashSet<usize>>();
        Self {
            piece_index_to_owners: indices.iter().map(|index| (*index, HashSet::new())).collect(),
            owner_count_to_piece_indices: BTreeMap::from([(0usize, indices)]),
        }
    }

    pub fn get_rarest_pieces(&self) -> impl Iterator<Item = usize> + '_ {
        self.owner_count_to_piece_indices
            .iter()
            .skip_while(|(count, _indices)| **count == 0usize)
            .flat_map(|(_count, indices)| indices.iter().cloned())
    }

    pub fn get_piece_owners(
        &self,
        piece_index: usize,
    ) -> Option<impl Iterator<Item = &SocketAddr>> {
        self.piece_index_to_owners.get(&piece_index).map(|peers| peers.iter())
    }

    pub fn add_single_record(&mut self, piece_owner: SocketAddr, piece_index: usize) -> bool {
        if let Some(owners) = self.piece_index_to_owners.get_mut(&piece_index) {
            if owners.insert(piece_owner) {
                self.change_owner_count_for_piece(piece_index, |prev_count| prev_count + 1);
            }
            true
        } else {
            false
        }
    }

    pub fn add_bitfield_record(&mut self, peer: SocketAddr, bitfield: &BitVec<u8, Msb0>) {
        for (piece_index, is_available) in bitfield.iter().enumerate() {
            if !*is_available {
                continue;
            }
            if !self.add_single_record(peer, piece_index) {
                break;
            }
        }
    }

    pub fn forget_peer(&mut self, peer: SocketAddr) {
        let mut relevant_pieces = Vec::new();
        for (piece_index, owners) in &mut self.piece_index_to_owners {
            if owners.remove(&peer) {
                relevant_pieces.push(*piece_index);
            }
        }
        for piece_index in relevant_pieces {
            self.change_owner_count_for_piece(piece_index, |prev_count| prev_count - 1);
        }
    }

    pub fn forget_piece(&mut self, piece_index: usize) {
        if self.piece_index_to_owners.remove(&piece_index).is_some() {
            for indices in self.owner_count_to_piece_indices.values_mut() {
                indices.remove(&piece_index);
            }
            self.owner_count_to_piece_indices.retain(|_count, pieces| !pieces.is_empty());
        }
    }

    fn change_owner_count_for_piece<F>(&mut self, piece_index: usize, op: F)
    where
        F: FnOnce(usize) -> usize,
    {
        if let Some((current_owner_count, indices)) = self
            .owner_count_to_piece_indices
            .iter_mut()
            .find_map(|(count, indices)| indices.remove(&piece_index).then_some((*count, indices)))
        {
            if indices.is_empty() {
                self.owner_count_to_piece_indices.remove(&current_owner_count);
            }
            let new_owner_count = op(current_owner_count);
            if let Some(indices) = self.owner_count_to_piece_indices.get_mut(&new_owner_count) {
                indices.insert(piece_index);
            } else {
                self.owner_count_to_piece_indices
                    .insert(new_owner_count, HashSet::from([piece_index]));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    fn ip(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    #[test]
    fn test_add_records_and_get_owners() {
        let mut pa = PieceAvailability::new(4);
        assert!(pa.get_piece_owners(4).is_none());
        assert_eq!(0, pa.get_piece_owners(0).unwrap().count());
        assert_eq!(0, pa.get_piece_owners(1).unwrap().count());
        assert_eq!(0, pa.get_piece_owners(2).unwrap().count());
        assert_eq!(0, pa.get_piece_owners(3).unwrap().count());

        let added = pa.add_single_record(ip(6000), 3);
        assert!(added);
        assert_eq!(0, pa.get_piece_owners(0).unwrap().count());
        assert_eq!(0, pa.get_piece_owners(1).unwrap().count());
        assert_eq!(0, pa.get_piece_owners(2).unwrap().count());
        assert_eq!(HashSet::from([&ip(6000)]), pa.get_piece_owners(3).unwrap().collect());

        let added = pa.add_single_record(ip(6666), 4);
        assert!(!added);

        let added = pa.add_single_record(ip(6000), 2);
        assert!(added);
        assert_eq!(0, pa.get_piece_owners(0).unwrap().count());
        assert_eq!(0, pa.get_piece_owners(1).unwrap().count());
        assert_eq!(HashSet::from([&ip(6000)]), pa.get_piece_owners(2).unwrap().collect());
        assert_eq!(HashSet::from([&ip(6000)]), pa.get_piece_owners(3).unwrap().collect());

        pa.add_bitfield_record(ip(6001), &BitVec::from_bitslice(bits![u8, Msb0; 1, 0, 0, 1]));
        assert_eq!(HashSet::from([&ip(6001)]), pa.get_piece_owners(0).unwrap().collect());
        assert_eq!(0, pa.get_piece_owners(1).unwrap().count());
        assert_eq!(HashSet::from([&ip(6000)]), pa.get_piece_owners(2).unwrap().collect());
        assert_eq!(
            HashSet::from([&ip(6000), &ip(6001)]),
            pa.get_piece_owners(3).unwrap().collect()
        );

        pa.add_bitfield_record(ip(6002), &BitVec::repeat(true, 8));
        assert_eq!(
            HashSet::from([&ip(6001), &ip(6002)]),
            pa.get_piece_owners(0).unwrap().collect()
        );
        assert_eq!(HashSet::from([&ip(6002)]), pa.get_piece_owners(1).unwrap().collect());
        assert_eq!(
            HashSet::from([&ip(6000), &ip(6002)]),
            pa.get_piece_owners(2).unwrap().collect()
        );
        assert_eq!(
            HashSet::from([&ip(6000), &ip(6001), &ip(6002)]),
            pa.get_piece_owners(3).unwrap().collect()
        );
    }

    #[test]
    fn test_add_records_and_get_rarest() {
        let mut pa = PieceAvailability::new(4);
        assert!(pa.get_rarest_pieces().next().is_none());

        pa.add_bitfield_record(ip(6000), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 1, 0]));
        pa.add_bitfield_record(ip(6001), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 0, 0]));
        pa.add_bitfield_record(ip(6002), &BitVec::from_bitslice(bits![u8, Msb0; 1, 0, 0, 0]));
        {
            let mut rarest = pa.get_rarest_pieces();
            assert_eq!(2, rarest.next().unwrap());
            assert_eq!(1, rarest.next().unwrap());
            assert_eq!(0, rarest.next().unwrap());
            assert!(rarest.next().is_none());
        }

        pa.add_bitfield_record(ip(6003), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 1, 1]));
        {
            let mut rarest = pa.get_rarest_pieces();
            assert_eq!(3, rarest.next().unwrap());
            assert_eq!(2, rarest.next().unwrap());
            assert_eq!(1, rarest.next().unwrap());
            assert_eq!(0, rarest.next().unwrap());
            assert!(rarest.next().is_none());
        }

        pa.add_single_record(ip(6002), 1);
        {
            let mut rarest = pa.get_rarest_pieces();
            assert_eq!(3, rarest.next().unwrap());
            assert_eq!(2, rarest.next().unwrap());
            assert_eq!(HashSet::from([0, 1]), rarest.collect());
        }
    }

    #[test]
    fn test_add_records_and_forget_piece() {
        let mut pa = PieceAvailability::new(4);
        pa.add_bitfield_record(ip(6000), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 1, 1]));
        pa.add_bitfield_record(ip(6001), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 1, 0]));
        pa.add_bitfield_record(ip(6002), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 0, 0]));
        pa.add_single_record(ip(6003), 0);

        pa.forget_piece(3);
        assert!(pa.get_piece_owners(3).is_none());
        let mut rarest = pa.get_rarest_pieces();
        assert_eq!(2, rarest.next().unwrap());
        assert_eq!(1, rarest.next().unwrap());
        assert_eq!(0, rarest.next().unwrap());
        assert!(rarest.next().is_none());
    }

    #[test]
    fn test_add_records_and_forget_peer() {
        let mut pa = PieceAvailability::new(4);
        pa.add_bitfield_record(ip(6000), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 1, 1]));
        pa.add_bitfield_record(ip(6001), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 1, 0]));
        pa.add_bitfield_record(ip(6002), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 0, 0]));
        pa.add_single_record(ip(6003), 0);

        pa.forget_peer(ip(6000));
        assert_eq!(0, pa.get_piece_owners(3).unwrap().count());
        assert_eq!(HashSet::from([&ip(6001)]), pa.get_piece_owners(2).unwrap().collect());
        assert_eq!(
            HashSet::from([&ip(6001), &ip(6002)]),
            pa.get_piece_owners(1).unwrap().collect()
        );
        assert_eq!(
            HashSet::from([&ip(6001), &ip(6002), &ip(6003)]),
            pa.get_piece_owners(0).unwrap().collect()
        );
        let mut rarest = pa.get_rarest_pieces();
        assert_eq!(2, rarest.next().unwrap());
        assert_eq!(1, rarest.next().unwrap());
        assert_eq!(0, rarest.next().unwrap());
        assert!(rarest.next().is_none());
    }

    #[test]
    fn test_dont_leak_empty_owner_count_entries() {
        let mut pa = PieceAvailability::new(4);
        assert_eq!(1, pa.owner_count_to_piece_indices.len());

        pa.add_single_record(ip(6000), 0);
        let mut keys = pa.owner_count_to_piece_indices.keys().cloned();
        assert_eq!(0, keys.next().unwrap());
        assert_eq!(1, keys.next().unwrap());
        assert!(keys.next().is_none());

        pa.add_single_record(ip(6001), 0);
        let mut keys = pa.owner_count_to_piece_indices.keys().cloned();
        assert_eq!(0, keys.next().unwrap());
        assert_eq!(2, keys.next().unwrap());
        assert!(keys.next().is_none());

        pa.forget_piece(0);
        let mut keys = pa.owner_count_to_piece_indices.keys().cloned();
        assert_eq!(0, keys.next().unwrap());
        assert!(keys.next().is_none());
    }
}
