use super::{matcher, Context, MonitorOwner};
use crate::{data, pwp};
use std::cmp;
use std::collections::HashSet;
use std::net::SocketAddr;

pub(super) fn update_requests(ctx: &mut Context) {
    cancel_unneeded_requests(ctx);
    request_pieces_from_seeders(ctx);
    remove_unneeded_interest(ctx);
}

fn cancel_unneeded_requests(ctx: &mut Context) {
    for (_ip, dm) in ctx.monitor_owner.all_download_monitors() {
        for requested_block in dm.requested_blocks() {
            let block_offset = ctx
                .piece_info
                .global_offset(
                    requested_block.piece_index,
                    requested_block.in_piece_offset,
                    requested_block.block_length,
                )
                .unwrap();
            if ctx
                .local_availability
                .has_exact_block_at(block_offset, requested_block.block_length)
            {
                dm.submit_outbound(pwp::DownloaderMessage::Cancel(requested_block.clone()));
            }
        }
    }
}

fn request_pieces_from_seeders(ctx: &mut Context) {
    let is_piece_requested = |piece: usize| utils::is_piece_requested(ctx, piece);

    let input = mapper::MapSeedersToPieces::seeders_to_n_rarest_pieces(
        20,
        ctx.piece_tracker,
        ctx.local_availability,
        ctx.monitor_owner,
        is_piece_requested,
    );
    let matcher = matcher::MaxBipartiteMatcher::new(input);
    matcher.calculate_max_matching();
    let seeder_piece_pairs = matcher.output();
    for (ip, piece) in &seeder_piece_pairs {
        let dm = ctx.monitor_owner.download_monitor(ip).unwrap();
        let piece_len = ctx.piece_info.piece_len(*piece);
        for block in utils::divide_piece_into_blocks(*piece, piece_len) {
            dm.submit_outbound(pwp::DownloaderMessage::Request(block));
        }
    }

    let (matched_peers, requested_pieces): (HashSet<SocketAddr>, HashSet<usize>) =
        seeder_piece_pairs.into_iter().unzip();

    let input = mapper::MapSeedersToPieces::seeders_to_pieces_except(
        &matched_peers,
        &requested_pieces,
        ctx.piece_tracker,
        ctx.local_availability,
        ctx.monitor_owner,
        is_piece_requested,
    );
    let matcher = matcher::MaxBipartiteMatcher::new(input);
    matcher.calculate_max_matching();
    let seeder_piece_pairs = matcher.output();
    for (ip, piece) in &seeder_piece_pairs {
        let piece_len = ctx.piece_info.piece_len(*piece);
        let dm = ctx.monitor_owner.download_monitor(ip).unwrap();
        for block in utils::divide_piece_into_blocks(*piece, piece_len) {
            dm.submit_outbound(pwp::DownloaderMessage::Request(block));
        }
    }
}

fn remove_unneeded_interest(ctx: &mut Context) {
    for (_, dm) in ctx
        .monitor_owner
        .all_download_monitors()
        .filter(|(_, dm)| dm.am_interested() && !dm.peer_choking())
    {
        if dm.requested_blocks().count() == 0 && dm.pending_outbound_count() == 0 {
            dm.submit_outbound(pwp::DownloaderMessage::NotInterested);
        }
    }
}

mod mapper {
    use super::*;

    pub(super) struct MapSeedersToPieces {
        seeders: Vec<SocketAddr>,
        pieces: Vec<usize>,
        piece_indices_owned_by_seeder: Vec<Vec<u16>>,
    }

    impl MapSeedersToPieces {
        pub(super) fn seeders_to_pieces_except(
            irrelevant_peers: &HashSet<SocketAddr>,
            irrelevant_pieces: &HashSet<usize>,
            piece_tracker: &data::PieceTracker,
            local_availability: &data::BlockAccountant,
            monitors: &dyn MonitorOwner,
            is_piece_requested: impl Fn(usize) -> bool,
        ) -> Self {
            let relevant_seeders = {
                let mut relevant_peers = monitors
                    .all_download_monitors()
                    .filter(|(ip, dm)| {
                        dm.am_interested()
                            && !dm.peer_choking()
                            && dm.requested_blocks().count() < 2
                            && !irrelevant_peers.contains(ip)
                    })
                    .collect::<Vec<_>>();
                relevant_peers
                    .sort_by(|(_, dm1), (_, dm2)| dm2.bytes_received().cmp(&dm1.bytes_received()));
                relevant_peers.into_iter().map(|(ip, _dm)| *ip).collect::<Vec<_>>()
            };

            let relevant_pieces = {
                let mut pieces = relevant_seeders
                    .iter()
                    .flat_map(|ip| piece_tracker.get_peer_pieces(ip).unwrap())
                    .filter(|piece| {
                        !local_availability.has_piece(*piece)
                            && !is_piece_requested(*piece)
                            && !irrelevant_pieces.contains(piece)
                    })
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                if !pieces.is_empty() {
                    let mut i = 0usize;
                    while pieces.len() < relevant_seeders.len() {
                        pieces.push(pieces[i % pieces.len()]);
                        i += 1;
                    }
                }
                pieces
            };

            let piece_indices_owned_by_seeder =
                Self::peers_to_piece_indices(&relevant_seeders, &relevant_pieces, piece_tracker);

            Self {
                seeders: relevant_seeders,
                pieces: relevant_pieces,
                piece_indices_owned_by_seeder,
            }
        }

        pub(super) fn seeders_to_n_rarest_pieces(
            max_piece_count: usize,
            piece_tracker: &data::PieceTracker,
            local_availability: &data::BlockAccountant,
            monitors: &dyn MonitorOwner,
            is_piece_requested: impl Fn(usize) -> bool,
        ) -> Self {
            let relevant_seeders = {
                let mut relevant_peers = monitors
                    .all_download_monitors()
                    .filter(|(_ip, dm)| {
                        dm.am_interested()
                            && !dm.peer_choking()
                            && dm.requested_blocks().count() < 2
                    })
                    .collect::<Vec<_>>();
                relevant_peers
                    .sort_by(|(_, dm1), (_, dm2)| dm2.bytes_received().cmp(&dm1.bytes_received()));
                relevant_peers.into_iter().map(|(ip, _dm)| *ip).collect::<Vec<_>>()
            };

            let relevant_pieces = {
                let mut pieces = piece_tracker
                    .get_rarest_pieces()
                    .filter(|&piece| {
                        !local_availability.has_piece(piece)
                            && !is_piece_requested(piece)
                            && piece_tracker
                                .get_piece_owners(piece)
                                .unwrap()
                                .any(|ip| relevant_seeders.contains(ip))
                    })
                    .take(max_piece_count)
                    .collect::<Vec<_>>();
                if !pieces.is_empty() {
                    let mut i = 0usize;
                    while pieces.len() < relevant_seeders.len() {
                        pieces.push(pieces[i % pieces.len()]);
                        i += 1;
                    }
                }
                pieces
            };

            let piece_indices_owned_by_seeder =
                Self::peers_to_piece_indices(&relevant_seeders, &relevant_pieces, piece_tracker);

            Self {
                seeders: relevant_seeders,
                pieces: relevant_pieces,
                piece_indices_owned_by_seeder,
            }
        }

        fn peers_to_piece_indices(
            peers: &[SocketAddr],
            pieces: &[usize],
            piece_tracker: &data::PieceTracker,
        ) -> Vec<Vec<u16>> {
            let mut piece_indices_owned_by_peer = Vec::with_capacity(peers.len());
            for peer_ip in peers {
                let mut owned_pieces_indices = Vec::new();
                for owned_piece in piece_tracker.get_peer_pieces(peer_ip).unwrap() {
                    for (i, &piece) in pieces.iter().enumerate() {
                        if piece == owned_piece {
                            owned_pieces_indices.push(i as u16);
                        }
                    }
                }
                piece_indices_owned_by_peer.push(owned_pieces_indices);
            }
            piece_indices_owned_by_peer
        }
    }

    impl matcher::Input for MapSeedersToPieces {
        type Output = Vec<(SocketAddr, usize)>;

        fn l_vertices_count(&self) -> u16 {
            self.seeders.len() as u16
        }

        fn r_vertices_count(&self) -> u16 {
            self.pieces.len() as u16
        }

        fn r_vertices_reachable_from(&self, seeder_index: u16) -> &Vec<u16> {
            &self.piece_indices_owned_by_seeder[seeder_index as usize]
        }

        fn process_output(self, result: impl Iterator<Item = (u16, u16)>) -> Self::Output {
            result
                .map(|(seeder_index, piece_index)| {
                    (self.seeders[seeder_index as usize], self.pieces[piece_index as usize])
                })
                .collect()
        }
    }
}

mod utils {
    use super::*;

    pub(super) fn is_piece_requested(ctx: &Context, piece: usize) -> bool {
        let seeders_count = ctx
            .monitor_owner
            .all_download_monitors()
            .filter(|(_, dm)| dm.am_interested() && !dm.peer_choking())
            .count();
        let piece_count = ctx.piece_tracker.get_rarest_pieces().count();

        if piece_count < seeders_count {
            false
        } else {
            let piece_len = ctx.piece_info.piece_len(piece);
            let piece_blocks = divide_piece_into_blocks(piece, piece_len).collect::<HashSet<_>>();
            ctx.monitor_owner
                .all_download_monitors()
                .flat_map(|(_, dm)| dm.requested_blocks())
                .any(|block| piece_blocks.contains(block))
        }
    }

    pub(super) const MAX_BLOCK_SIZE: usize = 16384;

    pub(super) fn divide_piece_into_blocks(
        piece_index: usize,
        piece_len: usize,
    ) -> impl Iterator<Item = pwp::BlockInfo> {
        (0..piece_len)
            .step_by(MAX_BLOCK_SIZE)
            .map(move |in_piece_offset| pwp::BlockInfo {
                piece_index,
                in_piece_offset,
                block_length: cmp::min(MAX_BLOCK_SIZE, piece_len - in_piece_offset),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::utils::*;
    use super::*;

    #[test]
    fn test_divide_piece_into_multiple_blocks() {
        let mut blocks_it = divide_piece_into_blocks(42, 2 * MAX_BLOCK_SIZE + 100);
        assert_eq!(
            pwp::BlockInfo {
                piece_index: 42,
                in_piece_offset: 0,
                block_length: MAX_BLOCK_SIZE,
            },
            blocks_it.next().unwrap()
        );
        assert_eq!(
            pwp::BlockInfo {
                piece_index: 42,
                in_piece_offset: MAX_BLOCK_SIZE,
                block_length: MAX_BLOCK_SIZE,
            },
            blocks_it.next().unwrap()
        );
        assert_eq!(
            pwp::BlockInfo {
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
            pwp::BlockInfo {
                piece_index: 42,
                in_piece_offset: 0,
                block_length: MAX_BLOCK_SIZE - 100,
            },
            blocks_it.next().unwrap()
        );
        assert!(blocks_it.next().is_none());
    }
}
