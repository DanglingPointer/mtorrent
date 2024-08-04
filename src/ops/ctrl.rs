use super::ctx;
use crate::utils::metainfo;
use crate::{min, pwp, sec};
use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::time::Duration;
use std::{cmp, io};

pub fn get_peer_reqq(peer_ip: &SocketAddr, ctx: &ctx::MainCtx) -> usize {
    const MIN_PENDING_REQUESTS: usize = 250;
    cmp::max(
        1,
        ctx.peer_states
            .get(peer_ip)
            .and_then(|state| state.extensions.as_deref())
            .and_then(|hs| hs.request_limit)
            .unwrap_or(MIN_PENDING_REQUESTS),
    )
}

const MAX_SEEDER_COUNT: usize = 50;

fn is_peer_interesting(peer_ip: &SocketAddr, ctx: &ctx::MainCtx) -> bool {
    let has_missing_pieces = || {
        ctx.piece_tracker
            .get_peer_pieces(peer_ip)
            .is_some_and(|mut it| it.next().is_some())
    };
    let has_unique_pieces = || {
        ctx.piece_tracker.get_peer_pieces(peer_ip).is_some_and(|mut piece_it| {
            piece_it.any(|piece| {
                ctx.piece_tracker
                    .get_piece_owners(piece)
                    .is_some_and(|owners_it| owners_it.count() == 1)
            })
        })
    };
    let has_recently_uploaded_data = || {
        ctx.peer_states.get(peer_ip).is_some_and(|state| {
            // need to check both because last_download_time is set upon creation
            state.download.bytes_received > 0 && state.last_download_time.elapsed() < sec!(30)
        })
    };

    if ctx.accountant.missing_bytes() == 0 {
        false
    } else if ctx.peer_states.seeders_count() <= MAX_SEEDER_COUNT {
        has_missing_pieces() || has_recently_uploaded_data()
    } else {
        has_unique_pieces()
    }
}

fn pieces_to_request(peer_ip: &SocketAddr, ctx: &ctx::MainCtx) -> Vec<usize> {
    // Calculate the number of pieces to request based on the following:
    // peer_reqq * 16kB == piece_len * piece_count
    // Note that piece_len/MAX_BLOCK_SIZE might still exceed reqq. This is being dealt with in download::get_pieces()
    let max_request_count =
        (pwp::MAX_BLOCK_SIZE * get_peer_reqq(peer_ip, ctx) / ctx.pieces.piece_len(0)).clamp(1, 50);
    let available_pieces: HashSet<usize> =
        if let Some(it) = ctx.piece_tracker.get_peer_pieces(peer_ip) {
            it.collect()
        } else {
            Default::default()
        };
    let mut ret = Vec::new();
    if !available_pieces.is_empty() {
        ret.reserve(max_request_count);
        for piece in ctx
            .piece_tracker
            .get_rarest_pieces()
            .filter(|piece| {
                available_pieces.contains(piece) && !ctx.pending_requests.is_piece_requested(*piece)
            })
            .take(max_request_count)
        {
            ret.push(piece);
        }
        if ret.len() < max_request_count {
            for piece in ctx
                .piece_tracker
                .get_rarest_pieces()
                .filter(|piece| available_pieces.contains(piece))
            {
                if !ret.contains(&piece) {
                    ret.push(piece);
                }
                if ret.len() == max_request_count {
                    break;
                }
            }
        }
    }
    ret
}

pub enum IdleDownloadAction {
    ActivateDownload,
    WaitForUpdates(Duration),
}

pub fn idle_download_next_action(peer_addr: &SocketAddr, ctx: &ctx::MainCtx) -> IdleDownloadAction {
    if is_peer_interesting(peer_addr, ctx) {
        IdleDownloadAction::ActivateDownload
    } else {
        IdleDownloadAction::WaitForUpdates(sec!(30))
    }
}

pub enum SeederDownloadAction {
    RequestPieces(Vec<usize>),
    WaitForUpdates(Duration),
    DeactivateDownload,
}

pub fn active_download_next_action(
    peer_addr: &SocketAddr,
    ctx: &ctx::MainCtx,
) -> SeederDownloadAction {
    let pieces = pieces_to_request(peer_addr, ctx);
    if !pieces.is_empty() {
        SeederDownloadAction::RequestPieces(pieces)
    } else if !is_peer_interesting(peer_addr, ctx) {
        SeederDownloadAction::DeactivateDownload
    } else {
        SeederDownloadAction::WaitForUpdates(sec!(5))
    }
}

// ------------------------------------------------------------------------------------------------

const MAX_LEECH_COUNT: usize = 4;

fn is_active_seeder(state: &pwp::PeerState) -> bool {
    state.download.am_interested
        && !state.download.peer_choking
        && state.download.bytes_received > 0 // need to check this because last_download_time is set upon creation
        && state.last_download_time.elapsed() < sec!(11)
}

fn is_active_leech(state: &pwp::PeerState) -> bool {
    state.upload.peer_interested
        && !state.upload.am_choking
        && state.upload.bytes_sent > 0
        && state.last_upload_time.elapsed() < sec!(11)
}

/// MAX_LEEDH_COUNT - 1 seeders with highest download rates, that are also interested
fn get_interested_top_seeders(peer_states: &pwp::PeerStates) -> Vec<&SocketAddr> {
    let interested_seeders: BTreeMap<usize, &SocketAddr> = peer_states
        .iter()
        .filter_map(|(addr, state)| {
            (is_active_seeder(state) && state.upload.peer_interested)
                .then_some((state.download.last_bitrate_bps, addr))
        })
        .collect();
    interested_seeders.values().rev().take(MAX_LEECH_COUNT - 1).cloned().collect()
}

fn count_interested_peers(peer_states: &pwp::PeerStates) -> usize {
    peer_states
        .iter()
        .fold(0, |acc, (_, state)| acc + if state.upload.peer_interested { 1 } else { 0 })
}

/// Checks existence of interested peers to whom we haven't sent any data yet
fn has_new_interested_peers(peer_states: &pwp::PeerStates) -> bool {
    peer_states.iter().any(|(_, state)| {
        state.upload.peer_interested && state.upload.am_choking && state.upload.bytes_sent == 0
    })
}

const MAX_JITTER: Duration = sec!(5);

#[cfg(not(test))]
fn with_jitter(duration: Duration) -> Duration {
    use rand::Rng;
    let min = duration - MAX_JITTER;
    let max = duration + MAX_JITTER;
    rand::thread_rng().gen_range(min..=max)
}

#[cfg(test)]
fn with_jitter(duration: Duration) -> Duration {
    assert!(duration >= MAX_JITTER);
    duration
}

#[cfg_attr(test, derive(PartialEq, Eq, Debug))]
pub enum IdleUploadAction {
    ActivateUploadAndServe(Duration),
    Linger(Duration),
}

pub fn idle_upload_next_action(
    peer_addr: &SocketAddr,
    peer_states: &pwp::PeerStates,
) -> IdleUploadAction {
    let state = peer_states
        .get(peer_addr)
        .unwrap_or_else(|| panic!("Unknown peer: {}", peer_addr));
    debug_assert!(state.upload.am_choking || !state.upload.peer_interested);

    if !state.upload.peer_interested {
        return IdleUploadAction::Linger(Duration::MAX);
    }

    let interested_top_seeders = get_interested_top_seeders(peer_states);
    let count_unhandled_interested_top_seeders = || {
        interested_top_seeders
            .iter()
            .filter(|addr| !peer_states.leeches().contains(addr))
            .count()
    };

    if interested_top_seeders.iter().any(|&addr| addr == peer_addr) {
        if peer_states.leeches_count() < MAX_LEECH_COUNT {
            // serve to this top seeder
            IdleUploadAction::ActivateUploadAndServe(sec!(15))
        } else {
            // wait for a slot
            IdleUploadAction::Linger(sec!(2))
        }
    } else if peer_states.leeches_count() + count_unhandled_interested_top_seeders()
        < MAX_LEECH_COUNT
        && (state.upload.bytes_sent == 0 /* is new */ || !has_new_interested_peers(peer_states))
    {
        // there are enough slots for at least one non-'top seeder'
        IdleUploadAction::ActivateUploadAndServe(with_jitter(if is_active_seeder(state) {
            sec!(30)
        } else {
            sec!(10)
        }))
    } else if is_active_seeder(state) {
        // this peer can potentially become a top seeder
        IdleUploadAction::Linger(sec!(5))
    } else {
        IdleUploadAction::Linger(sec!(30))
    }
}

#[cfg_attr(test, derive(PartialEq, Eq, Debug))]
pub enum LeechUploadAction {
    DeactivateUploadAndLinger(Duration),
    Serve(Duration),
}

pub fn active_upload_next_action(
    peer_addr: &SocketAddr,
    peer_states: &pwp::PeerStates,
) -> LeechUploadAction {
    let state = peer_states
        .get(peer_addr)
        .unwrap_or_else(|| panic!("Unknown peer: {}", peer_addr));
    debug_assert!(!state.upload.am_choking && state.upload.peer_interested);

    let interested_top_seeders = get_interested_top_seeders(peer_states);

    if interested_top_seeders.iter().any(|&addr| addr == peer_addr) {
        // is a top seeder
        LeechUploadAction::Serve(sec!(15))
    } else if count_interested_peers(peer_states) <= MAX_LEECH_COUNT {
        // no one else is interested in the slot
        LeechUploadAction::Serve(with_jitter(sec!(10)))
    } else if is_active_seeder(state) {
        // this peer can potentially become a top seeder
        LeechUploadAction::DeactivateUploadAndLinger(sec!(5))
    } else {
        LeechUploadAction::DeactivateUploadAndLinger(with_jitter(sec!(30)))
    }
}

// ------------------------------------------------------------------------------------------------

pub fn is_finished(ctx: &ctx::MainCtx) -> bool {
    #[cfg(debug_assertions)]
    if ctx.peer_states.iter().next().is_some() {
        // needed for integration tests to make sure we report all downloaded pieces before exiting
        return false;
    }
    // finish if we have downloaded everything, and all active leeches (if any) have received at least 50 blocks
    ctx.accountant.missing_bytes() == 0
        && !ctx.peer_states.iter().any(|(_, state)| {
            is_active_leech(state) && state.upload.bytes_sent < pwp::MAX_BLOCK_SIZE * 50
        })
}

pub fn verify_metadata(ctx: &mut ctx::PreliminaryCtx) -> bool {
    if ctx.metainfo_pieces.is_empty() || !ctx.metainfo_pieces.all() {
        false
    } else {
        match metainfo::Metainfo::new(&ctx.metainfo) {
            Some(metainfo) if metainfo.info_hash() == ctx.magnet.info_hash() => true,
            _ => {
                log::error!("Discarding corrupt metainfo");
                ctx.metainfo_pieces.fill(false);
                false
            }
        }
    }
}

pub fn can_serve_metadata(_peer_addr: &SocketAddr, _ctx: &ctx::MainCtx) -> bool {
    true
}

pub fn validate_peer_utility(peer_addr: &SocketAddr, ctx: &ctx::MainCtx) -> io::Result<()> {
    let state = ctx
        .peer_states
        .get(peer_addr)
        .unwrap_or_else(|| panic!("Unknown peer: {}", peer_addr));

    const TIMEOUT: Duration = min!(5);

    let owns_missing_piece = || {
        ctx.piece_tracker
            .get_peer_pieces(peer_addr)
            .is_some_and(|mut it| it.next().is_some())
    };
    let supports_pex = || {
        state
            .extensions
            .as_ref()
            .is_some_and(|hs| hs.extensions.contains_key(&pwp::Extension::PeerExchange))
    };
    if state.last_upload_time.elapsed() >= TIMEOUT
        && state.last_download_time.elapsed() >= TIMEOUT
        && !owns_missing_piece()
        && (!supports_pex() || ctx.peer_states.seeders_count() > 2)
    {
        Err(io::Error::new(io::ErrorKind::Other, "peer is useless"))
    } else {
        Ok(())
    }
}

// ------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    fn ip(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    #[test]
    fn test_reciprocate_to_top_seeders_with_highest_bitrates() {
        let mut peer_states = pwp::PeerStates::default();
        let ul_state = pwp::UploadState {
            am_choking: true,
            peer_interested: true,
            bytes_sent: 0,
            last_bitrate_bps: 0,
        };

        let ips = (6666u16..6676u16).map(ip).collect::<Vec<_>>();

        let mut bitrate = 1_000_000;
        for ip in &ips {
            peer_states.update_upload(ip, &ul_state);
            peer_states.update_download(
                ip,
                &pwp::DownloadState {
                    last_bitrate_bps: bitrate,
                    am_interested: true,
                    peer_choking: false,
                    bytes_received: 1,
                },
            );
            bitrate += 100_000;
        }

        let mut it = ips.iter();
        {
            // 1 optimistic unchoke
            let ip = it.next().unwrap();
            let action = idle_upload_next_action(ip, &peer_states);
            assert_eq!(action, IdleUploadAction::ActivateUploadAndServe(sec!(30)));
            peer_states.update_upload(
                ip,
                &pwp::UploadState {
                    am_choking: false,
                    ..ul_state
                },
            );
        }
        for _ in 0..ips.len() - MAX_LEECH_COUNT {
            // non top seeders
            let ip = it.next().unwrap();
            let action = idle_upload_next_action(ip, &peer_states);
            assert_eq!(action, IdleUploadAction::Linger(sec!(5)), "{:?}", ip);
        }
        for ip in it {
            // top seeders
            let action = idle_upload_next_action(ip, &peer_states);
            assert_eq!(action, IdleUploadAction::ActivateUploadAndServe(sec!(15)), "{:?}", ip);
            peer_states.update_upload(
                ip,
                &pwp::UploadState {
                    am_choking: false,
                    ..ul_state
                },
            );
        }

        // swap the optimistic unchoke
        let ip = &ips[0];
        let action = active_upload_next_action(ip, &peer_states);
        assert_eq!(action, LeechUploadAction::DeactivateUploadAndLinger(sec!(5)), "{:?}", ip);
        peer_states.update_upload(ip, &ul_state);

        let ip = &ips[1];
        let action = idle_upload_next_action(ip, &peer_states);
        assert_eq!(action, IdleUploadAction::ActivateUploadAndServe(sec!(30)));
        peer_states.update_upload(
            ip,
            &pwp::UploadState {
                am_choking: false,
                ..ul_state
            },
        );
    }

    #[test]
    fn test_upload_to_non_seeders_but_reserve_slots_for_seeders() {
        let mut peer_states = pwp::PeerStates::default();
        let ul_state = pwp::UploadState {
            am_choking: true,
            peer_interested: true,
            bytes_sent: 0,
            last_bitrate_bps: 0,
        };

        let ips = (6666u16..6676u16).map(ip).collect::<Vec<_>>();

        // 9 leeches and 1 seeder, all interested
        for ip in &ips {
            peer_states.update_upload(ip, &ul_state);
        }
        peer_states.update_download(
            ips.last().unwrap(),
            &pwp::DownloadState {
                last_bitrate_bps: 100_000,
                am_interested: true,
                peer_choking: false,
                bytes_received: 1,
            },
        );

        let mut it = ips.iter();

        for _ in 0..MAX_LEECH_COUNT - 1 {
            let ip = it.next().unwrap();
            let action = idle_upload_next_action(ip, &peer_states);
            assert_eq!(action, IdleUploadAction::ActivateUploadAndServe(sec!(10)), "{:?}", ip);
            peer_states.update_upload(
                ip,
                &pwp::UploadState {
                    am_choking: false,
                    ..ul_state
                },
            );
        }

        for _ in 0..ips.len() - MAX_LEECH_COUNT {
            let ip = it.next().unwrap();
            let action = idle_upload_next_action(ip, &peer_states);
            assert_eq!(action, IdleUploadAction::Linger(sec!(30)), "{:?}", ip);
        }

        // reciprocate to the one seeder
        let last_ip = it.next().unwrap();
        let action = idle_upload_next_action(last_ip, &peer_states);
        assert_eq!(action, IdleUploadAction::ActivateUploadAndServe(sec!(15)), "{:?}", last_ip);

        assert!(it.next().is_none());
    }

    #[test]
    fn test_switch_upload_between_non_seeding_peers() {
        let mut peer_states = pwp::PeerStates::default();
        let ul_state = pwp::UploadState {
            am_choking: true,
            peer_interested: true,
            bytes_sent: 0,
            last_bitrate_bps: 0,
        };

        let ips = (6666u16..6676u16).map(ip).collect::<Vec<_>>();

        peer_states.update_download(
            &ips[0],
            &pwp::DownloadState {
                am_interested: true,
                peer_choking: false,
                bytes_received: 1,
                last_bitrate_bps: 100_000,
            },
        );
        for ip in ips.iter().take(MAX_LEECH_COUNT) {
            peer_states.update_upload(
                ip,
                &pwp::UploadState {
                    am_choking: false,
                    bytes_sent: 1,
                    last_bitrate_bps: 1,
                    ..ul_state
                },
            );
        }
        for ip in ips.iter().skip(MAX_LEECH_COUNT) {
            peer_states.update_upload(ip, &ul_state);
        }

        // continue uploading to the one (top) seeder
        let action = active_upload_next_action(&ips[0], &peer_states);
        assert_eq!(action, LeechUploadAction::Serve(sec!(15)));

        // stop uploading to non-seeding peers
        for ip in ips.iter().skip(1).take(MAX_LEECH_COUNT - 1) {
            let action = active_upload_next_action(ip, &peer_states);
            assert_eq!(action, LeechUploadAction::DeactivateUploadAndLinger(sec!(30)), "{:?}", ip);
            peer_states.update_upload(
                ip,
                &pwp::UploadState {
                    am_choking: true,
                    ..peer_states.get(ip).unwrap().upload
                },
            );
        }

        // start uploading to other non-seeding peers
        for ip in ips.iter().skip(MAX_LEECH_COUNT).take(MAX_LEECH_COUNT - 1) {
            let action = idle_upload_next_action(ip, &peer_states);
            assert_eq!(action, IdleUploadAction::ActivateUploadAndServe(sec!(10)), "{:?}", ip);
            peer_states.update_upload(
                ip,
                &pwp::UploadState {
                    am_choking: false,
                    ..peer_states.get(ip).unwrap().upload
                },
            );
        }

        for ip in ips.iter().skip(1).take(MAX_LEECH_COUNT - 1) {
            let action = idle_upload_next_action(ip, &peer_states);
            assert_eq!(action, IdleUploadAction::Linger(sec!(30)), "{:?}", ip);
        }
    }

    #[test]
    fn test_replace_non_seeding_leech_with_a_seeder() {
        let mut peer_states = pwp::PeerStates::default();
        let ul_state = pwp::UploadState {
            am_choking: true,
            peer_interested: true,
            bytes_sent: 0,
            last_bitrate_bps: 0,
        };

        let ips = (6666u16..6676u16).map(ip).collect::<Vec<_>>();

        peer_states.update_download(
            &ips[0],
            &pwp::DownloadState {
                am_interested: true,
                peer_choking: false,
                bytes_received: 1,
                last_bitrate_bps: 100_000,
            },
        );
        for ip in ips.iter().take(MAX_LEECH_COUNT) {
            peer_states.update_upload(
                ip,
                &pwp::UploadState {
                    am_choking: false,
                    bytes_sent: 1,
                    last_bitrate_bps: 1,
                    ..ul_state
                },
            );
        }
        for ip in ips.iter().skip(MAX_LEECH_COUNT) {
            peer_states.update_upload(ip, &Default::default());
        }

        // continue uploading to non-seeding peers, as no one else is interested
        for ip in ips.iter().skip(1).take(MAX_LEECH_COUNT - 1) {
            let action = active_upload_next_action(ip, &peer_states);
            assert_eq!(action, LeechUploadAction::Serve(sec!(10)), "{:?}", ip);
        }

        // last peer becomes an interested seeder, but there is no slot
        let new_seeder_ip = ips.last().unwrap();
        peer_states.update_upload(new_seeder_ip, &ul_state);
        peer_states.update_download(
            new_seeder_ip,
            &pwp::DownloadState {
                am_interested: true,
                peer_choking: false,
                bytes_received: 1,
                last_bitrate_bps: 200_000,
            },
        );
        let action = idle_upload_next_action(new_seeder_ip, &peer_states);
        assert_eq!(action, IdleUploadAction::Linger(sec!(2)), "{:?}", new_seeder_ip);

        // continue uploading to the first seeder
        let action = active_upload_next_action(&ips[0], &peer_states);
        assert_eq!(action, LeechUploadAction::Serve(sec!(15)), "{:?}", new_seeder_ip);

        // stop uploading to one of the non-seeding peers
        let ip = &ips[2];
        let action = active_upload_next_action(ip, &peer_states);
        assert_eq!(action, LeechUploadAction::DeactivateUploadAndLinger(sec!(30)), "{:?}", ip);
        peer_states.update_upload(
            ip,
            &pwp::UploadState {
                am_choking: true,
                ..peer_states.get(ip).unwrap().upload
            },
        );

        // start uploading to the new seeder
        let action = idle_upload_next_action(new_seeder_ip, &peer_states);
        assert_eq!(
            action,
            IdleUploadAction::ActivateUploadAndServe(sec!(15)),
            "{:?}",
            new_seeder_ip
        );
        peer_states.update_upload(
            new_seeder_ip,
            &pwp::UploadState {
                am_choking: false,
                ..peer_states.get(new_seeder_ip).unwrap().upload
            },
        );

        // swap one non-seeding peer
        let ip = &ips[1];
        let action = active_upload_next_action(ip, &peer_states);
        assert_eq!(action, LeechUploadAction::DeactivateUploadAndLinger(sec!(30)), "{:?}", ip);
        peer_states.update_upload(
            ip,
            &pwp::UploadState {
                am_choking: true,
                ..peer_states.get(ip).unwrap().upload
            },
        );
        let ip = &ips[2];
        let action = idle_upload_next_action(ip, &peer_states);
        assert_eq!(action, IdleUploadAction::ActivateUploadAndServe(sec!(10)), "{:?}", ip);
        peer_states.update_upload(
            ip,
            &pwp::UploadState {
                am_choking: false,
                ..peer_states.get(ip).unwrap().upload
            },
        );
        let ip = &ips[1];
        let action = idle_upload_next_action(ip, &peer_states);
        assert_eq!(action, IdleUploadAction::Linger(sec!(30)), "{:?}", ip);
    }

    #[test]
    fn test_deactivate_excessive_leeches() {
        let mut peer_states = pwp::PeerStates::default();
        let ul_state = pwp::UploadState {
            am_choking: false,
            peer_interested: true,
            bytes_sent: 1,
            last_bitrate_bps: 1000,
        };

        let ips = (6666u16..6676u16).map(ip).collect::<Vec<_>>();

        // uploading to 10 peers, first one is a seeder
        for ip in &ips {
            peer_states.update_upload(ip, &ul_state);
        }
        let seeder_ip = &ips[0];
        peer_states.update_download(
            seeder_ip,
            &pwp::DownloadState {
                am_interested: true,
                peer_choking: false,
                bytes_received: 1,
                last_bitrate_bps: 100_000,
            },
        );

        // don't deactivate upload to the seeder
        let action = active_upload_next_action(seeder_ip, &peer_states);
        assert_eq!(action, LeechUploadAction::Serve(sec!(15)));

        // deactivate excessive upload (in reality all upload to non-seeders)
        for ip in ips.iter().skip(1) {
            let action = active_upload_next_action(ip, &peer_states);
            assert_eq!(action, LeechUploadAction::DeactivateUploadAndLinger(sec!(30)), "{:?}", ip);
            peer_states.update_upload(
                ip,
                &pwp::UploadState {
                    am_choking: true,
                    ..peer_states.get(ip).unwrap().upload
                },
            );
        }

        // let mut it = ips.iter().skip(1);
        // for _ in 0..ips.len() - MAX_LEECH_COUNT {
        //     let ip = it.next().unwrap();
        //     let action = active_upload_next_action(ip, &peer_states);
        //     assert_eq!(action, LeechUploadAction::DeactivateUploadAndLinger(sec!(30)), "{:?}", ip);
        //     peer_states.update_upload(
        //         ip,
        //         &pwp::UploadState {
        //             am_choking: true,
        //             ..peer_states.get(ip).unwrap().upload
        //         },
        //     );
        // }
    }
}
