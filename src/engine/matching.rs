use std::cell::Cell;

pub(super) trait Input {
    fn l_vertices_count(&self) -> u16;
    fn r_vertices_count(&self) -> u16;
    fn r_vertices_reachable_from(&self, l_vertex: u16) -> &Vec<u16>;
}

#[derive(Clone, Copy)]
enum RMatch {
    None,
    Some(graph::RVertexIndex),
}

#[derive(Clone, Copy)]
enum LMatch {
    None,
    Inspected(graph::LVertexIndex),
    Uninspected(graph::LVertexIndex),
}

mod graph {
    use super::*;

    #[derive(Clone, Copy)]
    pub(super) struct LVertexIndex(u16);

    #[derive(Clone, Copy)]
    pub(super) struct RVertexIndex(u16);

    pub(super) type RMatchSlot = Cell<RMatch>;
    pub(super) type LMatchSlot = Cell<LMatch>;

    pub(super) struct GraphHolder {
        l_to_r: Box<[RMatchSlot]>,
        r_to_l: Box<[LMatchSlot]>,
    }

    impl GraphHolder {
        pub(super) fn new(l_vertices_count: usize, r_vertices_count: usize) -> Self {
            let l_to_r = vec![RMatchSlot::new(RMatch::None); l_vertices_count].into_boxed_slice();
            let r_to_l = vec![LMatchSlot::new(LMatch::None); r_vertices_count].into_boxed_slice();
            Self { l_to_r, r_to_l }
        }

        pub(super) fn l_to_r(&self, l_vertex: LVertexIndex) -> &RMatchSlot {
            &self.l_to_r[l_vertex.0 as usize]
        }

        pub(super) fn r_to_l(&self, r_vertex: RVertexIndex) -> &LMatchSlot {
            &self.r_to_l[r_vertex.0 as usize]
        }

        pub(super) fn all_l_vertices(&self) -> impl Iterator<Item = LVertexIndex> {
            (0u16..self.l_to_r.len() as u16).map(LVertexIndex)
        }

        pub(super) fn mark_all_as_uninspected(&self) {
            for slot in &self.r_to_l[..] {
                if let LMatch::Inspected(v) = slot.get() {
                    slot.set(LMatch::Uninspected(v));
                }
            }
        }

        pub(super) fn connected_vertex_pairs(&self) -> impl Iterator<Item = (u16, u16)> + '_ {
            self.l_to_r
                .iter()
                .enumerate()
                .filter_map(|(l_index, r_match)| match r_match.get() {
                    RMatch::None => None,
                    RMatch::Some(r_vertex) => Some((l_index as u16, r_vertex.0)),
                })
        }
    }

    pub(super) fn reachable_r_vertices(
        input: &impl Input,
        l_vertex: LVertexIndex,
    ) -> impl Iterator<Item = RVertexIndex> + '_ {
        input.r_vertices_reachable_from(l_vertex.0).iter().map(|&index| {
            assert!(index < input.r_vertices_count());
            RVertexIndex(index)
        })
    }
}

use graph::*;

pub(super) struct MaxBipartiteMatching<I: Input> {
    input: I,
    state: GraphHolder,
}

#[allow(dead_code)]
impl<I: Input> MaxBipartiteMatching<I> {
    pub(super) fn new(input: I) -> Self {
        let state =
            GraphHolder::new(input.l_vertices_count() as usize, input.r_vertices_count() as usize);
        Self { input, state }
    }

    pub(super) fn calculate_max_matching(&self) {
        for l_vertex in self.state.all_l_vertices() {
            self.state.mark_all_as_uninspected();
            self.try_add_l_vertex(l_vertex);
        }
    }

    pub(super) fn output(self) -> Vec<(u16, u16)> {
        self.state.connected_vertex_pairs().collect()
    }

    fn try_add_l_vertex(&self, l_vertex: LVertexIndex) -> bool {
        let matching_r = self.state.l_to_r(l_vertex);

        if let RMatch::None = matching_r.get() {
            for r_vertex in reachable_r_vertices(&self.input, l_vertex) {
                let matching_l = self.state.r_to_l(r_vertex);

                if let LMatch::None = matching_l.get() {
                    matching_l.set(LMatch::Uninspected(l_vertex));
                    matching_r.set(RMatch::Some(r_vertex));
                    return true;
                }
            }

            for r_vertex in reachable_r_vertices(&self.input, l_vertex) {
                let matching_l = self.state.r_to_l(r_vertex);
                if let LMatch::Uninspected(matched_l_vertex) = matching_l.get() {
                    matching_r.set(RMatch::Some(r_vertex));
                    matching_l.set(LMatch::Inspected(l_vertex));

                    let matching_r = self.state.l_to_r(matched_l_vertex);
                    matching_r.set(RMatch::None);

                    if self.try_add_l_vertex(matched_l_vertex) {
                        return true;
                    }

                    matching_l.set(LMatch::Inspected(matched_l_vertex));
                    matching_r.set(RMatch::Some(r_vertex));
                }
            }
            matching_r.set(RMatch::None);
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::*;

    #[test]
    fn test_verify_slot_size() {
        assert_eq_size!(u32, RMatchSlot);
        assert_eq_size!(u32, LMatchSlot);
    }

    struct TestInput {
        l_to_r_edges: Vec<Vec<u16>>,
        r_vertices_count: usize,
    }

    impl Input for TestInput {
        fn l_vertices_count(&self) -> u16 {
            self.l_to_r_edges.len() as u16
        }

        fn r_vertices_count(&self) -> u16 {
            self.r_vertices_count as u16
        }

        fn r_vertices_reachable_from(&self, l_vertex: u16) -> &Vec<u16> {
            &self.l_to_r_edges[l_vertex as usize]
        }
    }

    /// ```
    ///   o--o
    ///   o--o
    /// ```
    #[test]
    fn test_simple_2l_2r_vertices() {
        let input = TestInput {
            l_to_r_edges: vec![vec![0u16], vec![1u16]],
            r_vertices_count: 2,
        };

        let m = MaxBipartiteMatching::new(input);
        m.calculate_max_matching();
        assert_eq!(vec![(0u16, 0u16), (1u16, 1u16)], m.output());
    }

    /// ```
    ///   o--o
    ///    \/
    ///    /\
    ///   o  o
    /// ```
    #[test]
    fn test_2l_2r_vertices() {
        let input = TestInput {
            l_to_r_edges: vec![vec![0u16, 1u16], vec![0u16]],
            r_vertices_count: 2,
        };

        let m = MaxBipartiteMatching::new(input);
        m.calculate_max_matching();
        assert_eq!(vec![(0u16, 1u16), (1u16, 0u16)], m.output());
    }

    /// Too complex to draw...
    #[test]
    fn test_3l_3r_vertices() {
        let input = TestInput {
            l_to_r_edges: vec![vec![0u16, 1u16, 2u16], vec![0u16, 1u16], vec![0u16]],
            r_vertices_count: 3,
        };

        let m = MaxBipartiteMatching::new(input);
        m.calculate_max_matching();
        assert_eq!(vec![(0u16, 2u16), (1u16, 1u16), (2u16, 0u16)], m.output());
    }

    /// "Introduction to Algorithms" by Thomas H. Corman, Figure 26.8
    #[test]
    fn test_5l_4r_vertices() {
        let input = TestInput {
            l_to_r_edges: vec![
                vec![0u16],
                vec![0u16, 2u16],
                vec![1u16, 2u16, 3u16],
                vec![2u16],
                vec![2u16],
            ],
            r_vertices_count: 4,
        };

        let m = MaxBipartiteMatching::new(input);
        m.calculate_max_matching();
        assert_eq!(vec![(0u16, 0u16), (1u16, 2u16), (2u16, 1u16)], m.output());
    }

    /// "Introduction to Algorithms" by Thomas H. Corman, Figure 26.8 MIRRORED
    #[test]
    fn test_4l_5r_vertices() {
        let input = TestInput {
            l_to_r_edges: vec![
                vec![0u16, 1u16],
                vec![2u16],
                vec![1u16, 2u16, 3u16, 4u16],
                vec![2u16],
            ],
            r_vertices_count: 5,
        };

        let m = MaxBipartiteMatching::new(input);
        m.calculate_max_matching();
        assert_eq!(vec![(0u16, 0u16), (1u16, 2u16), (2u16, 1u16)], m.output());
    }
}
