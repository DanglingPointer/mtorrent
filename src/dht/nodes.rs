use super::u160::U160;
use crate::min;
use std::cmp;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::ops::RangeInclusive;
use tokio::time::Instant;

pub struct Node {
    id: U160,
    addr: SocketAddr,
    last_received_query_time: Option<Instant>,
    last_received_response_time: Option<Instant>,
}

impl Node {
    pub fn id(&self) -> &U160 {
        &self.id
    }

    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    pub fn is_good(&self) -> bool {
        self.last_received_response_time.is_some_and(|time| time.elapsed() < min!(15))
            || (self.last_received_response_time.is_some()
                && self.last_received_query_time.is_some_and(|time| time.elapsed() < min!(15)))
    }

    pub fn last_active_time(&self) -> Option<Instant> {
        cmp::max(self.last_received_query_time, self.last_received_response_time)
    }
}

pub struct RoutingTable {
    nodes: HashMap<U160, Node>,
    buckets: BTreeMap<U160, Bucket>,
    local_id: U160,
}

struct Bucket {
    bounds: RangeInclusive<U160>,
    nodes: HashSet<U160>,
    splittable: bool,
}

impl RoutingTable {
    const MAX_NODES_PER_BUCKET: usize = 8;

    pub fn new(local_node_id: U160) -> Self {
        let initial_bucket = Bucket {
            bounds: U160::MIN..=U160::MAX,
            nodes: Default::default(),
            splittable: true,
        };
        Self {
            nodes: Default::default(),
            buckets: [(U160::MIN, initial_bucket)].into(),
            local_id: local_node_id,
        }
    }

    pub fn local_id(&self) -> &U160 {
        &self.local_id
    }

    pub fn get_node_by_id(&self, id: &U160) -> Option<&Node> {
        self.nodes.get(id)
    }

    pub fn get_node_by_ip(&self, addr: &SocketAddr) -> Option<&Node> {
        self.nodes.values().find(|node| node.addr() == addr)
    }

    pub fn closest_nodes(&self, target: &U160) -> impl Iterator<Item = &Node> {
        let mut distances_to_nodes: Vec<_> = self
            .nodes
            .iter()
            .map(|(id, node)| {
                let (min, max) = match id.cmp(target) {
                    cmp::Ordering::Less => (id, target),
                    cmp::Ordering::Equal => (id, target),
                    cmp::Ordering::Greater => (target, id),
                };
                (max - min, node)
            })
            .collect();
        distances_to_nodes.sort_by(|(lhs_dist, _), (rhs_dist, _)| lhs_dist.cmp(rhs_dist));
        distances_to_nodes.into_iter().map(|(_, node)| node)
    }

    pub fn submit_query_from_known_node(&mut self, node_id: &U160, node_addr: &SocketAddr) -> bool {
        self.update_node(node_id, node_addr, Some(Instant::now()), None)
    }

    pub fn submit_response_from_known_node(
        &mut self,
        node_id: &U160,
        node_addr: &SocketAddr,
    ) -> bool {
        self.update_node(node_id, node_addr, None, Some(Instant::now()))
    }

    pub fn add_responding_node(&mut self, node_id: &U160, node_addr: &SocketAddr) -> bool {
        if self.nodes.contains_key(node_id) {
            false
        } else {
            self.insert_node(Node {
                id: node_id.clone(),
                addr: *node_addr,
                last_received_query_time: None,
                last_received_response_time: Some(Instant::now()),
            })
        }
    }

    pub fn remove_node(&mut self, node_id: &U160) {
        if let Some(node) = self.nodes.remove(node_id) {
            let bucket = self
                .buckets
                .values_mut()
                .rev()
                .find(|bucket| bucket.bounds.start() < node_id)
                .expect("Hash table doesn't cover entire value space");
            bucket.nodes.remove(node_id);

            log::info!("Node removed: id={:?} addr={}", node.id(), node.addr());
        }
    }

    fn update_node(
        &mut self,
        node_id: &U160,
        node_addr: &SocketAddr,
        last_query_time: Option<Instant>,
        last_response_time: Option<Instant>,
    ) -> bool {
        match self.nodes.get_mut(node_id) {
            Some(node) => {
                if &node.addr != node_addr {
                    log::warn!(
                        "Address of node {:?} changed from {} to {}",
                        node_id,
                        node.addr,
                        node_addr
                    );
                    node.addr = *node_addr;
                }
                if last_query_time.is_some() {
                    node.last_received_query_time = last_query_time;
                } else {
                    node.last_received_response_time = last_response_time;
                }
                true
            }
            None => false,
        }
    }

    fn insert_node(&mut self, new_node: Node) -> bool {
        let (lower_bound, bucket) = self
            .buckets
            .iter_mut()
            .rev()
            .find(|&(lower_bound, _)| lower_bound < &new_node.id)
            .expect("Hash table doesn't cover entire value space");

        if bucket.nodes.len() < Self::MAX_NODES_PER_BUCKET {
            log::info!("New node inserted: id={:?} addr={}", new_node.id(), new_node.addr());
            bucket.nodes.insert(new_node.id.clone());
            self.nodes.insert(new_node.id.clone(), new_node);
            true
        } else if !bucket.splittable {
            log::info!("New node ignored: id={:?} addr={}", new_node.id(), new_node.addr());
            false
        } else {
            // split bucket and try again
            let key = lower_bound.clone();
            let bucket = self.buckets.remove(&key).unwrap();

            let (lhs, rhs) = bucket.split(&self.local_id);
            self.buckets.insert(lhs.bounds.start().clone(), lhs);
            self.buckets.insert(rhs.bounds.start().clone(), rhs);

            self.insert_node(new_node)
        }
    }
}

impl Bucket {
    fn split(self, local_id: &U160) -> (Self, Self) {
        let (lhs_start, rhs_end) = self.bounds.into_inner();
        let distance = &rhs_end - &lhs_start;
        let lhs_end = &lhs_start + &(distance >> 1);

        let mut rhs = Bucket {
            bounds: (&lhs_end + &U160::ONE)..=rhs_end,
            splittable: false,
            nodes: Default::default(),
        };
        rhs.splittable = rhs.bounds.contains(local_id);

        let mut lhs = Bucket {
            bounds: lhs_start..=lhs_end,
            splittable: !rhs.splittable,
            nodes: Default::default(),
        };

        debug_assert!(lhs.bounds.start() < lhs.bounds.end());
        debug_assert!(lhs.bounds.end() < rhs.bounds.start());
        debug_assert!(rhs.bounds.start() < rhs.bounds.end());
        debug_assert!(rhs.bounds.contains(local_id) != lhs.bounds.contains(local_id));

        for node_id in self.nodes {
            if lhs.bounds.contains(&node_id) {
                lhs.nodes.insert(node_id);
            } else if rhs.bounds.contains(&node_id) {
                rhs.nodes.insert(node_id);
            } else {
                unreachable!()
            }
        }

        (lhs, rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(port: u16) -> SocketAddr {
        SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), port)
    }

    fn id_from_byte(index: usize, val: u8) -> U160 {
        let mut bytes = [0; 20];
        bytes[index] = val;
        U160::from(bytes)
    }

    fn id_from_u128(val: u128) -> U160 {
        let mut bytes = [0; 20];
        bytes[4..20].copy_from_slice(&val.to_be_bytes());
        U160::from(bytes)
    }

    #[test]
    fn test_split_initial_bucket() {
        let local_id = id_from_byte(0, 0);
        let bucket = Bucket {
            bounds: U160::MIN..=U160::MAX,
            nodes: [
                id_from_byte(0, 0b1000_0000),
                id_from_byte(0, 0b1000_0000 - 1),
            ]
            .into(),
            splittable: true,
        };

        let (lhs, rhs) = bucket.split(&local_id);

        assert_eq!(lhs.bounds, U160::MIN..=(&id_from_byte(0, 0b1000_0000) - &U160::ONE));
        assert_eq!(rhs.bounds, id_from_byte(0, 0b1000_0000)..=U160::MAX);
        assert_eq!(lhs.nodes.len(), 1);
        assert_eq!(lhs.nodes.iter().next().unwrap(), &id_from_byte(0, 0b1000_0000 - 1));
        assert_eq!(rhs.nodes.len(), 1);
        assert_eq!(rhs.nodes.iter().next().unwrap(), &id_from_byte(0, 0b1000_0000));
        assert!(lhs.splittable);
        assert!(!rhs.splittable);
    }

    #[test]
    fn test_get_closest_nodes() {
        let mut rt = RoutingTable::new([0u8; 20].into());

        for i in 1..11 {
            assert!(rt.add_responding_node(&id_from_u128(i as u128), &ip(i as u16)));
        }

        assert_eq!(rt.nodes.len(), 10);
        assert_eq!(rt.buckets.len(), 158);

        assert_eq!(
            rt.closest_nodes(&id_from_u128(11))
                .map(|node| node.id().clone())
                .collect::<Vec<_>>(),
            (1..=10).rev().map(|i| id_from_u128(i as u128)).collect::<Vec<_>>()
        );

        assert_eq!(
            rt.closest_nodes(&id_from_u128(1))
                .map(|node| node.id().clone())
                .collect::<Vec<_>>(),
            (1..=10).map(|i| id_from_u128(i as u128)).collect::<Vec<_>>()
        );

        assert_eq!(
            rt.closest_nodes(&id_from_u128(5)).map(|node| node.id().clone()).next().unwrap(),
            id_from_u128(5)
        );

        assert_eq!(
            rt.closest_nodes(&id_from_u128(5))
                .map(|node| node.id().clone())
                .take(3)
                .collect::<HashSet<_>>(),
            [id_from_u128(4), id_from_u128(5), id_from_u128(6)].into()
        );

        assert_eq!(
            rt.closest_nodes(&id_from_u128(5))
                .map(|node| node.id().clone())
                .skip(3)
                .take(2)
                .collect::<HashSet<_>>(),
            [id_from_u128(3), id_from_u128(7)].into()
        );

        rt.remove_node(&id_from_u128(5));
        assert_eq!(rt.nodes.len(), 9);
        assert_eq!(rt.buckets.len(), 158);

        assert_eq!(
            rt.closest_nodes(&id_from_u128(5))
                .map(|node| node.id().clone())
                .take(2)
                .collect::<HashSet<_>>(),
            [id_from_u128(4), id_from_u128(6)].into()
        );
    }

    #[test]
    fn test_ignore_node_when_bucket_is_full() {
        let mut rt = RoutingTable::new([0u8; 20].into());

        for i in 1..16 {
            assert!(rt.add_responding_node(&id_from_u128(i as u128), &ip(i as u16)));
        }
        assert!(!rt.add_responding_node(&id_from_u128(16), &ip(16)));

        assert_eq!(rt.nodes.len(), 15);
        assert_eq!(rt.buckets.len(), 158);

        for i in 17..25 {
            assert!(rt.add_responding_node(&id_from_u128(i as u128), &ip(i as u16)));
        }
        assert!(!rt.add_responding_node(&id_from_u128(25), &ip(25)));

        assert_eq!(rt.nodes.len(), 23);
        assert_eq!(rt.buckets.len(), 158);

        rt.remove_node(&id_from_u128(24));
        assert_eq!(rt.nodes.len(), 22);

        assert!(rt.add_responding_node(&id_from_u128(25), &ip(25)));
        assert_eq!(rt.nodes.len(), 23);
    }
}
