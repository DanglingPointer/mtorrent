use super::U160;
use bitvec::mem::bits_of;
use std::array;
use std::collections::BTreeMap;
use std::iter;
use std::net::SocketAddr;

pub struct Node {
    pub id: U160,
    pub addr: SocketAddr,
}

struct Bucket<const SIZE: usize> {
    nodes: [Option<Node>; SIZE],
}

impl<const SIZE: usize> Bucket<SIZE> {
    fn new() -> Self {
        Self {
            nodes: array::from_fn(|_| None),
        }
    }

    fn iter(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter().filter_map(Option::as_ref)
    }

    fn insert(&mut self, new_node: Node) -> bool {
        if self
            .nodes
            .iter()
            .any(|slot| matches!(slot, Some(node) if node.id == new_node.id))
        {
            false
        } else if let Some(slot) = self.nodes.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(new_node);
            true
        } else {
            false
        }
    }

    fn remove(&mut self, id: &U160) -> Option<Node> {
        self.nodes
            .iter_mut()
            .find(|slot| matches!(slot, Some(node) if &node.id == id))
            .and_then(|node| node.take())
    }
}

/// A fully pre-allocated Kademlia routing table as defined in https://www.scs.stanford.edu/~dm/home/papers/kpos.pdf.
/// Shouldn't be put on stack due to its size. The table enforces uniqueness of node IDs, but not of their addresses.
///
/// A bucket at index `i` contains nodes whose distance to the `local_id` is `2^(159-i) <= d < 2^(160-i)`.
/// E.g. the bucket at index `0` contains nodes furthest away from `local_id`.
/// Note that buckets closest to `local_id` still allocate `BUCKET_SIZE` slots even though they can never become full.
pub struct RoutingTable<const BUCKET_SIZE: usize> {
    buckets: [Bucket<BUCKET_SIZE>; bits_of::<U160>()],
    local_id: U160,
}

impl<const BUCKET_SIZE: usize> RoutingTable<BUCKET_SIZE> {
    pub const BUCKET_SIZE: usize = BUCKET_SIZE;

    pub fn new(local_id: U160) -> Self {
        Self {
            buckets: array::from_fn(|_| Bucket::new()),
            local_id,
        }
    }

    pub fn local_id(&self) -> &U160 {
        &self.local_id
    }

    /// Insert a new node if it's ID doesn't collide with any existing node,
    /// and the corresponding bucket is not full.
    pub fn insert_node(&mut self, id: &U160, addr: &SocketAddr) -> bool {
        let U160(distance) = *id ^ self.local_id;
        if let Some(bucket_index) = distance.first_one() {
            self.buckets[bucket_index].insert(Node {
                id: *id,
                addr: *addr,
            })
        } else {
            false
        }
    }

    pub fn remove_node(&mut self, id: &U160) -> Option<Node> {
        let U160(distance) = *id ^ self.local_id;
        distance
            .first_one()
            .and_then(|bucket_index| self.buckets[bucket_index].remove(id))
    }

    pub fn get_node(&self, id: &U160) -> Option<&Node> {
        let U160(distance) = *id ^ self.local_id;
        if let Some(bucket_index) = distance.first_one() {
            self.buckets[bucket_index].iter().find(|node| node.id == *id)
        } else {
            None
        }
    }

    /// Return all nodes stored in the bucket closest to `target`, i.e. up to `BUCKET_SIZE` nodes.
    pub fn closest_bucket(&self, target: &U160) -> Box<dyn Iterator<Item = &Node> + '_> {
        let U160(distance) = *target ^ self.local_id;
        if let Some(bucket_index) = distance.first_one() {
            Box::new(self.buckets[bucket_index].iter())
        } else {
            Box::new(iter::empty())
        }
    }

    /// Return all nodes currently in the table sorted from the lowest to the highest distance to `target`.
    pub fn all_closest_nodes(&self, target: &U160) -> impl Iterator<Item = SocketAddr> {
        let mut nodes_by_dist_asc = BTreeMap::<_, SocketAddr>::new();
        for node in self.buckets.iter().flat_map(Bucket::iter) {
            let U160(distance) = node.id ^ *target;
            nodes_by_dist_asc.insert(distance, node.addr);
        }
        nodes_by_dist_asc.into_values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    type RoutingTable = super::RoutingTable<8>;

    #[test]
    fn test_dont_insert_same_id_twice() {
        let mut rt = RoutingTable::new(U160::from([0; 20]));

        assert!(rt.insert_node(
            &U160::from([0xaf; 20]),
            &SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 6666)
        ));
        assert!(!rt.insert_node(
            &U160::from([0xaf; 20]),
            &SocketAddr::new(Ipv4Addr::BROADCAST.into(), 7777)
        ));
    }

    #[test]
    fn test_respect_bucket_size() {
        let mut rt = RoutingTable::new(U160::from([0; 20]));

        fn bucket_0_id(i: u8) -> U160 {
            let mut id = [0u8; 20];
            id[0] = 0b1000_0000u8;
            id[19] = i;
            id.into()
        }

        fn bucket_1_id(i: u8) -> U160 {
            let mut id = [0u8; 20];
            id[0] = 0b0100_0000u8;
            id[19] = i;
            id.into()
        }

        let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 6666);

        // fill the bucket furthest away
        for i in 0..RoutingTable::BUCKET_SIZE {
            assert!(rt.insert_node(&bucket_0_id(i as u8), &addr));
        }
        assert!(!rt.insert_node(&bucket_0_id(RoutingTable::BUCKET_SIZE as u8), &addr));

        // fill next furthest bucket
        for i in 0..RoutingTable::BUCKET_SIZE {
            assert!(rt.insert_node(&bucket_1_id(i as u8), &addr));
        }
        assert!(!rt.insert_node(&bucket_1_id(RoutingTable::BUCKET_SIZE as u8), &addr));

        // verify all nodes are still there
        for i in 0..RoutingTable::BUCKET_SIZE {
            assert!(rt.get_node(&bucket_0_id(i as u8)).is_some());
            assert!(rt.get_node(&bucket_1_id(i as u8)).is_some());
        }
        assert!(rt.get_node(&bucket_0_id(RoutingTable::BUCKET_SIZE as u8)).is_none());
        assert!(rt.get_node(&bucket_1_id(RoutingTable::BUCKET_SIZE as u8)).is_none());
    }
}
