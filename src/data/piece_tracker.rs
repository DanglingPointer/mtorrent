use crate::pwp;
use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy, Debug)]
struct PieceIndex(usize);

impl Borrow<usize> for PieceIndex {
    fn borrow(&self) -> &usize {
        &self.0
    }
}

fn available_pieces(bitfield: &pwp::Bitfield) -> impl Iterator<Item = usize> + Clone + '_ {
    bitfield
        .iter()
        .enumerate()
        .filter_map(|(index, bit)| (bit == true).then_some(index))
}

pub struct PieceTracker {
    piece_index_to_owners: HashMap<PieceIndex, HashSet<SocketAddr>>,
    owners_to_piece_indices: HashMap<SocketAddr, pwp::Bitfield>,

    owner_count_to_piece_indices: BTreeMap<usize, HashSet<PieceIndex>>,
    piece_count_to_owners: BTreeMap<usize, HashSet<SocketAddr>>,

    bitfield_factory: Box<dyn Fn() -> pwp::Bitfield>,
}

impl PieceTracker {
    pub fn new(piece_count: usize) -> Self {
        let indices = (0..piece_count).map(PieceIndex).collect::<HashSet<PieceIndex>>();
        Self {
            piece_index_to_owners: indices.iter().map(|index| (*index, HashSet::new())).collect(),
            owners_to_piece_indices: HashMap::new(),
            owner_count_to_piece_indices: BTreeMap::from([(0usize, indices)]),
            piece_count_to_owners: BTreeMap::new(),
            bitfield_factory: Box::new(move || pwp::Bitfield::repeat(false, piece_count)),
        }
    }

    pub fn missing_pieces_rarest_first(&self) -> impl Iterator<Item = usize> + '_ {
        self.owner_count_to_piece_indices
            .iter()
            .skip_while(|(count, _indices)| **count == 0usize)
            .flat_map(|(_count, indices)| indices.iter().map(|i| i.0))
    }

    #[cfg_attr(not(test), expect(dead_code))]
    pub fn get_poorest_peers(&self) -> impl Iterator<Item = &SocketAddr> + Clone {
        self.piece_count_to_owners.values().flat_map(HashSet::iter)
    }

    pub fn get_piece_owners(
        &self,
        piece_index: usize,
    ) -> impl Iterator<Item = &SocketAddr> + Clone {
        self.piece_index_to_owners.get(&piece_index).into_iter().flat_map(HashSet::iter)
    }

    pub fn get_peer_pieces(&self, peer: &SocketAddr) -> impl Iterator<Item = usize> + Clone + '_ {
        self.owners_to_piece_indices.get(peer).into_iter().flat_map(available_pieces)
    }

    pub fn has_peer_piece(&self, peer: &SocketAddr, piece_index: usize) -> bool {
        self.owners_to_piece_indices
            .get(peer)
            .and_then(|pieces| pieces.get(piece_index))
            .is_some_and(|piece_present| piece_present == true)
    }

    pub fn add_single_record(&mut self, piece_owner: &SocketAddr, piece_index: usize) -> bool {
        let piece_index = PieceIndex(piece_index);

        if let Some(piece_owners) = self.piece_index_to_owners.get_mut(&piece_index) {
            let peer_pieces = self
                .owners_to_piece_indices
                .entry(*piece_owner)
                .or_insert_with(&self.bitfield_factory);

            let updated_peer_pieces = !peer_pieces.replace(piece_index.0, true);
            let updated_piece_owners = piece_owners.insert(*piece_owner);
            assert_eq!(updated_piece_owners, updated_peer_pieces, "Inconsistent internal state");

            if updated_peer_pieces {
                self.change_owner_count_for_piece(piece_index, |prev_count| prev_count + 1);
                self.change_piece_count_for_owner(piece_owner, |prev_count| prev_count + 1);
                true
            } else {
                false
            }
        } else {
            // forgotten (i.e. already downloaded) or invalid piece
            false
        }
    }

    pub fn add_bitfield_record(&mut self, peer: &SocketAddr, bitfield: &pwp::Bitfield) {
        for piece_index in available_pieces(bitfield) {
            self.add_single_record(peer, piece_index);
        }
    }

    pub fn forget_peer(&mut self, peer: &SocketAddr) {
        if let Some(pieces) = self.owners_to_piece_indices.remove(peer) {
            for piece_index in available_pieces(&pieces).map(PieceIndex) {
                let owners = self
                    .piece_index_to_owners
                    .get_mut(&piece_index)
                    .expect("Invalid internal state");
                owners.remove(peer);
                self.change_owner_count_for_piece(piece_index, |prev_count| {
                    prev_count.saturating_sub(1)
                });
            }
            let removed =
                self.piece_count_to_owners.iter_mut().find_map(|(piece_count, owners)| {
                    let owner_count = owners.len();
                    owners.remove(peer).then_some((piece_count, owner_count - 1))
                });
            if let Some((&piece_count, 0)) = removed {
                self.piece_count_to_owners.remove(&piece_count);
            }
        }
    }

    pub fn forget_piece(&mut self, piece_index: usize) {
        if let Some(owners) = self.piece_index_to_owners.remove(&piece_index) {
            for owner in owners {
                let pieces =
                    self.owners_to_piece_indices.get_mut(&owner).expect("Invalid internal state");
                pieces.set(piece_index, false);
                self.change_piece_count_for_owner(&owner, |prev_count| {
                    prev_count.saturating_sub(1)
                });
            }
            let removed =
                self.owner_count_to_piece_indices.iter_mut().find_map(|(owner_count, pieces)| {
                    let indices_count = pieces.len();
                    pieces.remove(&piece_index).then_some((owner_count, indices_count - 1))
                });
            if let Some((&owner_count, 0)) = removed {
                self.owner_count_to_piece_indices.remove(&owner_count);
            }
        }
    }

    fn change_owner_count_for_piece<F>(&mut self, piece_index: PieceIndex, op: F)
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
            self.owner_count_to_piece_indices
                .entry(new_owner_count)
                .and_modify(|indices| {
                    indices.insert(piece_index);
                })
                .or_insert_with(|| HashSet::from([piece_index]));
        }
    }

    fn change_piece_count_for_owner<F>(&mut self, peer: &SocketAddr, op: F)
    where
        F: FnOnce(usize) -> usize,
    {
        let current_piece_count = if let Some((current_piece_count, owners)) = self
            .piece_count_to_owners
            .iter_mut()
            .find_map(|(count, owners)| owners.remove(peer).then_some((*count, owners)))
        {
            if owners.is_empty() {
                self.piece_count_to_owners.remove(&current_piece_count);
            }
            current_piece_count
        } else {
            0
        };
        let new_piece_count = op(current_piece_count);
        if new_piece_count > 0 {
            self.piece_count_to_owners
                .entry(new_piece_count)
                .and_modify(|owners| {
                    owners.insert(*peer);
                })
                .or_insert_with(|| HashSet::from([*peer]));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitvec::prelude::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    fn ip(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    #[test]
    fn test_add_records_and_get_owners() {
        let mut pa = PieceTracker::new(4);
        assert!(pa.get_piece_owners(4).next().is_none());
        assert_eq!(0, pa.get_piece_owners(0).count());
        assert_eq!(0, pa.get_piece_owners(1).count());
        assert_eq!(0, pa.get_piece_owners(2).count());
        assert_eq!(0, pa.get_piece_owners(3).count());

        let added = pa.add_single_record(&ip(6000), 3);
        assert!(added);
        let added = pa.add_single_record(&ip(6000), 3);
        assert!(!added);
        assert_eq!(0, pa.get_piece_owners(0).count());
        assert_eq!(0, pa.get_piece_owners(1).count());
        assert_eq!(0, pa.get_piece_owners(2).count());
        assert_eq!(HashSet::from([&ip(6000)]), pa.get_piece_owners(3).collect());

        let added = pa.add_single_record(&ip(6666), 4);
        assert!(!added);

        let added = pa.add_single_record(&ip(6000), 2);
        assert!(added);
        assert_eq!(0, pa.get_piece_owners(0).count());
        assert_eq!(0, pa.get_piece_owners(1).count());
        assert_eq!(HashSet::from([&ip(6000)]), pa.get_piece_owners(2).collect());
        assert_eq!(HashSet::from([&ip(6000)]), pa.get_piece_owners(3).collect());

        pa.add_bitfield_record(&ip(6001), &BitVec::from_bitslice(bits![u8, Msb0; 1, 0, 0, 1]));
        assert_eq!(HashSet::from([&ip(6001)]), pa.get_piece_owners(0).collect());
        assert_eq!(0, pa.get_piece_owners(1).count());
        assert_eq!(HashSet::from([&ip(6000)]), pa.get_piece_owners(2).collect());
        assert_eq!(HashSet::from([&ip(6000), &ip(6001)]), pa.get_piece_owners(3).collect());

        pa.add_bitfield_record(&ip(6002), &BitVec::repeat(true, 8));
        assert_eq!(HashSet::from([&ip(6001), &ip(6002)]), pa.get_piece_owners(0).collect());
        assert_eq!(HashSet::from([&ip(6002)]), pa.get_piece_owners(1).collect());
        assert_eq!(HashSet::from([&ip(6000), &ip(6002)]), pa.get_piece_owners(2).collect());
        assert_eq!(
            HashSet::from([&ip(6000), &ip(6001), &ip(6002)]),
            pa.get_piece_owners(3).collect()
        );
    }

    #[test]
    fn test_add_records_and_get_rarest_and_poorest() {
        let mut pa = PieceTracker::new(4);
        assert!(pa.missing_pieces_rarest_first().next().is_none());

        pa.add_bitfield_record(&ip(6000), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 1, 0]));
        pa.add_bitfield_record(&ip(6001), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 0, 0]));
        pa.add_bitfield_record(&ip(6002), &BitVec::from_bitslice(bits![u8, Msb0; 1, 0, 0, 0]));
        {
            let mut rarest = pa.missing_pieces_rarest_first();
            assert_eq!(2, rarest.next().unwrap());
            assert_eq!(1, rarest.next().unwrap());
            assert_eq!(0, rarest.next().unwrap());
            assert!(rarest.next().is_none());
        }
        {
            let mut poorest = pa.get_poorest_peers();
            assert_eq!(&ip(6002), poorest.next().unwrap());
            assert_eq!(&ip(6001), poorest.next().unwrap());
            assert_eq!(&ip(6000), poorest.next().unwrap());
            assert!(poorest.next().is_none());
        }

        pa.add_bitfield_record(&ip(6003), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 1, 1]));
        {
            let mut rarest = pa.missing_pieces_rarest_first();
            assert_eq!(3, rarest.next().unwrap());
            assert_eq!(2, rarest.next().unwrap());
            assert_eq!(1, rarest.next().unwrap());
            assert_eq!(0, rarest.next().unwrap());
            assert!(rarest.next().is_none());
        }
        {
            let mut poorest = pa.get_poorest_peers();
            assert_eq!(&ip(6002), poorest.next().unwrap());
            assert_eq!(&ip(6001), poorest.next().unwrap());
            assert_eq!(&ip(6000), poorest.next().unwrap());
            assert_eq!(&ip(6003), poorest.next().unwrap());
            assert!(poorest.next().is_none());
        }

        pa.add_single_record(&ip(6002), 1);
        {
            let mut rarest = pa.missing_pieces_rarest_first();
            assert_eq!(3, rarest.next().unwrap());
            assert_eq!(2, rarest.next().unwrap());
            assert_eq!(HashSet::from([0, 1]), rarest.collect());
        }
        {
            let mut richest = pa.get_poorest_peers().collect::<Vec<_>>().into_iter().rev();
            assert_eq!(&ip(6003), richest.next().unwrap());
            assert_eq!(&ip(6000), richest.next().unwrap());
            assert_eq!(HashSet::from([&ip(6001), &ip(6002)]), richest.collect());
        }
    }

    #[test]
    fn test_add_records_and_forget_piece() {
        let mut pa = PieceTracker::new(4);
        pa.add_bitfield_record(&ip(6000), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 1, 1]));
        pa.add_bitfield_record(&ip(6001), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 1, 0]));
        pa.add_bitfield_record(&ip(6002), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 0, 0]));
        pa.add_single_record(&ip(6003), 0);

        pa.forget_piece(0);
        assert!(pa.get_piece_owners(0).next().is_none());
        assert_eq!(HashSet::new(), pa.get_peer_pieces(&ip(6003)).collect());
        assert_eq!(HashSet::from([1]), pa.get_peer_pieces(&ip(6002)).collect());
        assert_eq!(HashSet::from([1, 2]), pa.get_peer_pieces(&ip(6001)).collect());
        assert_eq!(HashSet::from([1, 2, 3]), pa.get_peer_pieces(&ip(6000)).collect());
        {
            let mut rarest = pa.missing_pieces_rarest_first();
            assert_eq!(3, rarest.next().unwrap());
            assert_eq!(2, rarest.next().unwrap());
            assert_eq!(1, rarest.next().unwrap());
            assert!(rarest.next().is_none());

            let mut poorest = pa.get_poorest_peers();
            assert_eq!(&ip(6002), poorest.next().unwrap());
            assert_eq!(&ip(6001), poorest.next().unwrap());
            assert_eq!(&ip(6000), poorest.next().unwrap());
            assert!(poorest.next().is_none());
        }

        pa.forget_piece(3);
        assert!(pa.get_piece_owners(3).next().is_none());
        assert_eq!(HashSet::new(), pa.get_peer_pieces(&ip(6003)).collect());
        assert_eq!(HashSet::from([1]), pa.get_peer_pieces(&ip(6002)).collect());
        assert_eq!(HashSet::from([1, 2]), pa.get_peer_pieces(&ip(6001)).collect());
        assert_eq!(HashSet::from([1, 2]), pa.get_peer_pieces(&ip(6000)).collect());
        {
            let mut rarest = pa.missing_pieces_rarest_first();
            assert_eq!(2, rarest.next().unwrap());
            assert_eq!(1, rarest.next().unwrap());
            assert!(rarest.next().is_none());

            let mut poorest = pa.get_poorest_peers();
            assert_eq!(&ip(6002), poorest.next().unwrap());
            assert_eq!(HashSet::from([&ip(6001), &ip(6000)]), poorest.collect());
        }

        pa.forget_piece(1);
        pa.forget_piece(2);
        assert_eq!(HashSet::new(), pa.get_peer_pieces(&ip(6003)).collect());
        assert_eq!(HashSet::new(), pa.get_peer_pieces(&ip(6002)).collect());
        assert_eq!(HashSet::new(), pa.get_peer_pieces(&ip(6001)).collect());
        assert_eq!(HashSet::new(), pa.get_peer_pieces(&ip(6000)).collect());
        assert!(pa.missing_pieces_rarest_first().next().is_none());
    }

    #[test]
    fn test_add_records_and_forget_peer() {
        let mut pa = PieceTracker::new(4);
        pa.add_bitfield_record(&ip(6000), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 1, 1]));
        pa.add_bitfield_record(&ip(6001), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 1, 0]));
        pa.add_bitfield_record(&ip(6002), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 0, 0]));
        pa.add_single_record(&ip(6003), 0);

        pa.forget_peer(&ip(6000));
        assert!(pa.get_peer_pieces(&ip(6000)).next().is_none());
        assert_eq!(HashSet::new(), pa.get_piece_owners(3).collect());
        assert_eq!(HashSet::from([&ip(6001)]), pa.get_piece_owners(2).collect());
        assert_eq!(HashSet::from([&ip(6001), &ip(6002)]), pa.get_piece_owners(1).collect());
        assert_eq!(
            HashSet::from([&ip(6001), &ip(6002), &ip(6003)]),
            pa.get_piece_owners(0).collect()
        );
        {
            let mut rarest = pa.missing_pieces_rarest_first();
            assert_eq!(2, rarest.next().unwrap());
            assert_eq!(1, rarest.next().unwrap());
            assert_eq!(0, rarest.next().unwrap());
            assert!(rarest.next().is_none());

            let mut poorest = pa.get_poorest_peers();
            assert_eq!(&ip(6003), poorest.next().unwrap());
            assert_eq!(&ip(6002), poorest.next().unwrap());
            assert_eq!(&ip(6001), poorest.next().unwrap());
            assert!(poorest.next().is_none());
        }

        pa.forget_peer(&ip(6003));
        assert!(pa.get_peer_pieces(&ip(6003)).next().is_none());
        assert_eq!(HashSet::new(), pa.get_piece_owners(3).collect());
        assert_eq!(HashSet::from([&ip(6001)]), pa.get_piece_owners(2).collect());
        assert_eq!(HashSet::from([&ip(6001), &ip(6002)]), pa.get_piece_owners(1).collect());
        assert_eq!(HashSet::from([&ip(6001), &ip(6002)]), pa.get_piece_owners(0).collect());
        {
            let mut rarest = pa.missing_pieces_rarest_first();
            assert_eq!(2, rarest.next().unwrap());
            assert_eq!(HashSet::from([1, 0]), rarest.collect());

            let mut poorest = pa.get_poorest_peers();
            assert_eq!(&ip(6002), poorest.next().unwrap());
            assert_eq!(&ip(6001), poorest.next().unwrap());
            assert!(poorest.next().is_none());
        }
    }

    #[test]
    fn test_dont_leak_empty_owner_count_entries() {
        let mut pa = PieceTracker::new(4);
        assert_eq!(1, pa.owner_count_to_piece_indices.len());

        pa.add_single_record(&ip(6000), 0);
        let mut keys = pa.owner_count_to_piece_indices.keys().cloned();
        assert_eq!(0, keys.next().unwrap());
        assert_eq!(1, keys.next().unwrap());
        assert!(keys.next().is_none());

        pa.add_single_record(&ip(6001), 0);
        let mut keys = pa.owner_count_to_piece_indices.keys().cloned();
        assert_eq!(0, keys.next().unwrap());
        assert_eq!(2, keys.next().unwrap());
        assert!(keys.next().is_none());

        pa.forget_piece(0);
        let mut keys = pa.owner_count_to_piece_indices.keys().cloned();
        assert_eq!(0, keys.next().unwrap());
        assert!(keys.next().is_none());
    }

    #[test]
    fn test_dont_leak_empty_piece_count_entries() {
        let mut pa = PieceTracker::new(4);
        assert_eq!(0, pa.piece_count_to_owners.len());

        pa.add_single_record(&ip(6000), 0);
        let mut keys = pa.piece_count_to_owners.keys().cloned();
        assert_eq!(1, keys.next().unwrap());
        assert!(keys.next().is_none());

        pa.add_single_record(&ip(6000), 1);
        let mut keys = pa.piece_count_to_owners.keys().cloned();
        assert_eq!(2, keys.next().unwrap());
        assert!(keys.next().is_none());

        pa.forget_peer(&ip(6000));
        assert!(pa.piece_count_to_owners.is_empty());
    }

    #[test]
    fn test_process_entire_bitfield_ignoring_forgotten_pieces() {
        let mut pa = PieceTracker::new(4);
        pa.forget_piece(0);

        pa.add_bitfield_record(&ip(6000), &BitVec::from_bitslice(bits![u8, Msb0; 1, 1, 1, 1]));
        assert!(pa.get_piece_owners(0).next().is_none());
        assert_eq!(HashSet::from([&ip(6000)]), pa.get_piece_owners(1).collect());
        assert_eq!(HashSet::from([&ip(6000)]), pa.get_piece_owners(2).collect());
        assert_eq!(HashSet::from([&ip(6000)]), pa.get_piece_owners(3).collect());
        assert_eq!(HashSet::from([1, 2, 3]), pa.missing_pieces_rarest_first().collect());
        assert_eq!(HashSet::from([1, 2, 3]), pa.get_peer_pieces(&ip(6000)).collect());
    }
}
