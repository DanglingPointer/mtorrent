use super::U160;
use bitvec::mem::bits_of;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::{array, cmp};

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

    fn has_space(&self) -> bool {
        self.nodes.iter().any(Option::is_none)
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

    pub fn can_insert(&self, id: &U160) -> bool {
        let U160(distance) = *id ^ self.local_id;
        if let Some(bucket_index) = distance.first_one() {
            self.buckets[bucket_index].has_space()
        } else {
            false
        }
    }

    pub fn get_node(&self, id: &U160) -> Option<&Node> {
        let U160(distance) = *id ^ self.local_id;
        if let Some(bucket_index) = distance.first_one() {
            self.buckets[bucket_index].iter().find(|node| node.id == *id)
        } else {
            None
        }
    }

    pub fn node_count(&self) -> usize {
        self.buckets.iter().flat_map(Bucket::iter).count()
    }

    /// Return all nodes currently in the table sorted from the lowest to the highest distance to `target`.
    pub fn all_nodes_by_dist_asc(&self, target: &U160) -> impl Iterator<Item = &Node> {
        let mut nodes_by_dist_asc = BTreeMap::<_, &Node>::new();
        for node in self.buckets.iter().flat_map(Bucket::iter) {
            let U160(distance) = node.id ^ *target;
            nodes_by_dist_asc.insert(distance, node);
        }
        nodes_by_dist_asc.into_values()
    }

    /// Return N closest nodes to `target` where `0 <= N < max_count+2*BUCKET_SIZE`.
    ///
    /// Check `target` bucket first, then go through the adjacent buckets.
    /// This means the result is semi-sorted: closest buckets go first, but the nodes within each bucket are not sorted.
    pub fn get_closest_nodes(
        &self,
        target: &U160,
        max_count_hint: usize,
    ) -> impl Iterator<Item = &Node> {
        let mut closest_nodes = Vec::with_capacity(
            cmp::min(max_count_hint, self.buckets.len() / 2 * Self::BUCKET_SIZE)
                + 2 * Self::BUCKET_SIZE
                - 1,
        );

        let U160(distance) = *target ^ self.local_id;
        if let Some(initial_index) = distance.first_one() {
            assert!(initial_index < self.buckets.len());
            closest_nodes.extend(self.buckets[initial_index].iter());

            let mut delta = 1;
            let mut progress_made = true;
            while closest_nodes.len() < max_count_hint && progress_made {
                progress_made = false;

                if let Some(right_bucket) = self.buckets.get(initial_index + delta) {
                    progress_made = true;
                    closest_nodes.extend(right_bucket.iter());
                }

                if let Some(left_index) = initial_index.checked_sub(delta) {
                    progress_made = true;
                    closest_nodes.extend(self.buckets[left_index].iter());
                }

                delta += 1;
            }
        }

        closest_nodes.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashSet, net::Ipv4Addr};

    type RoutingTable = super::RoutingTable<16>;

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
        assert_eq!(rt.node_count(), RoutingTable::BUCKET_SIZE);

        // fill next furthest bucket
        for i in 0..RoutingTable::BUCKET_SIZE {
            assert!(rt.insert_node(&bucket_1_id(i as u8), &addr));
        }
        assert!(!rt.insert_node(&bucket_1_id(RoutingTable::BUCKET_SIZE as u8), &addr));
        assert_eq!(rt.node_count(), RoutingTable::BUCKET_SIZE * 2);

        // verify all nodes are still there
        for i in 0..RoutingTable::BUCKET_SIZE {
            assert!(rt.get_node(&bucket_0_id(i as u8)).is_some());
            assert!(rt.get_node(&bucket_1_id(i as u8)).is_some());
        }
        assert!(rt.get_node(&bucket_0_id(RoutingTable::BUCKET_SIZE as u8)).is_none());
        assert!(rt.get_node(&bucket_1_id(RoutingTable::BUCKET_SIZE as u8)).is_none());
    }

    #[test]
    fn test_get_closest_nodes_from_adjacent_buckets() {
        let mut rt = RoutingTable::new(U160::from([0; 20]));
        let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 6666);

        fn bucket_k_id(k: usize, i: u8) -> U160 {
            let mut id: U160 = [0u8; 20].into();
            id.0.get_mut(k).unwrap().set(true);
            id.0.data[19] = i;
            id
        }

        let target = bucket_k_id(100, 3);

        for k in 98..=102 {
            for i in 0..RoutingTable::BUCKET_SIZE {
                assert!(rt.insert_node(&bucket_k_id(k, i as u8), &addr));
            }
        }

        // from the same bucket
        let closest_nodes: HashSet<U160> = rt
            .get_closest_nodes(&target, RoutingTable::BUCKET_SIZE)
            .map(|node| node.id)
            .collect();
        assert_eq!(
            closest_nodes,
            (0..RoutingTable::BUCKET_SIZE).map(|i| bucket_k_id(100, i as u8)).collect()
        );
        let closest_nodes: HashSet<U160> =
            rt.get_closest_nodes(&target, 1).map(|node| node.id).collect();
        assert_eq!(
            closest_nodes,
            (0..RoutingTable::BUCKET_SIZE).map(|i| bucket_k_id(100, i as u8)).collect()
        );

        // from adjacent buckets on each side
        let closest_nodes: HashSet<U160> = rt
            .get_closest_nodes(&target, RoutingTable::BUCKET_SIZE * 3)
            .map(|node| node.id)
            .collect();
        assert_eq!(
            closest_nodes,
            (99..=101)
                .flat_map(|k| (0..RoutingTable::BUCKET_SIZE).map(move |i| bucket_k_id(k, i as u8)))
                .collect()
        );
        let closest_nodes: HashSet<U160> = rt
            .get_closest_nodes(&target, RoutingTable::BUCKET_SIZE + 1)
            .map(|node| node.id)
            .collect();
        assert_eq!(
            closest_nodes,
            (99..=101)
                .flat_map(|k| (0..RoutingTable::BUCKET_SIZE).map(move |i| bucket_k_id(k, i as u8)))
                .collect()
        );

        // from one adjacent bucket
        let target = bucket_k_id(98, 3);
        let closest_nodes: HashSet<U160> = rt
            .get_closest_nodes(&target, RoutingTable::BUCKET_SIZE + 1)
            .map(|node| node.id)
            .collect();
        assert_eq!(
            closest_nodes,
            (98..=99)
                .flat_map(|k| (0..RoutingTable::BUCKET_SIZE).map(move |i| bucket_k_id(k, i as u8)))
                .collect()
        );

        // from one non-full bucket
        let target = bucket_k_id(42, 5);
        assert!(rt.insert_node(&bucket_k_id(42, 3), &addr));
        assert!(rt.insert_node(&bucket_k_id(42, 1), &addr));
        let closest_nodes: Vec<U160> =
            rt.get_closest_nodes(&target, 1).map(|node| node.id).collect();
        assert_eq!(closest_nodes, Vec::from([bucket_k_id(42, 3), bucket_k_id(42, 1)]));
    }
}
