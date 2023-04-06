use super::Context;
use crate::pwp;

#[derive(Default)]
pub struct State {
    leechers: usize,
    seeders: usize,
}

const MAX_SEEDERS_COUNT: usize = 30;

pub fn update_interest(ctx: &mut Context) {
    let start = std::time::Instant::now();
    update_state(ctx);
    remove_unneeded_interest(ctx);
    show_interest_to_nonchoking_single_owners(ctx);
    show_interest_to_only_nonchoking_owners(ctx);
    show_interest_to_any_nonchoking_owners(ctx);
    show_desperate_interest(ctx);
    let end = std::time::Instant::now();
    let running_time_ms = (end - start).as_millis();
    if running_time_ms > 0 {
        log::debug!("Engine spent {}ms running", running_time_ms);
    }
}

fn update_state(ctx: &mut Context) {
    ctx.state.interest.seeders = ctx
        .monitor_owner
        .all_download_monitors()
        .filter(|(_addr, mon)| mon.am_interested() && !mon.peer_choking())
        .count();
    ctx.state.interest.leechers = ctx
        .monitor_owner
        .all_upload_monitors()
        .filter(|(_addr, mon)| !mon.am_choking() && mon.peer_interested())
        .count();
}

fn remove_unneeded_interest(ctx: &mut Context) {
    let piece_tracker = ctx.piece_tracker;

    for (addr, dm) in ctx
        .monitor_owner
        .all_download_monitors()
        .filter(|(_addr, mon)| mon.am_interested())
    {
        let owns_missing_piece =
            piece_tracker.get_peer_pieces(addr).map(|mut it| it.next()).is_some();

        if !owns_missing_piece {
            dm.submit_outbound(pwp::DownloaderMessage::NotInterested);
            if !dm.peer_choking() {
                ctx.state.interest.seeders -= 1;
            }
        }
    }

    // TODO: remove interest if no pending requests
}

fn show_interest_to_nonchoking_single_owners(ctx: &mut Context) {
    let piece_tracker = ctx.piece_tracker;
    let monitors = &mut ctx.monitor_owner;

    for (addr, dm) in monitors
        .all_download_monitors()
        .filter(|(_addr, mon)| !mon.peer_choking() && !mon.am_interested())
    {
        let single_owner = piece_tracker
            .get_rarest_pieces()
            .find_map(|piece_index| {
                if let Some(mut owners_it) = piece_tracker.get_piece_owners(piece_index) {
                    let first_owner = owners_it.next();
                    let second_owner = owners_it.next();
                    match (first_owner, second_owner) {
                        (Some(ip), None) if ip == addr => Some(true),
                        (Some(_), None) => None,
                        _ => Some(false),
                    }
                } else {
                    debug_assert!(false);
                    None
                }
            })
            .unwrap_or(false);
        if single_owner {
            dm.submit_outbound(pwp::DownloaderMessage::Interested);
            ctx.state.interest.seeders += 1;
        }
    }
}

fn show_interest_to_only_nonchoking_owners(ctx: &mut Context) {
    let piece_tracker = ctx.piece_tracker;
    let monitors = &mut ctx.monitor_owner;

    for (addr, dm) in monitors
        .all_download_monitors()
        .filter(|(_addr, mon)| !mon.peer_choking() && !mon.am_interested())
    {
        let only_nonchoking_owner = match piece_tracker.get_peer_pieces(addr) {
            Some(mut it) => it.any(|piece_index| {
                if let Some(owners_it) = piece_tracker.get_piece_owners(piece_index) {
                    let nonchoking_owners = owners_it
                        .filter(|ip| match monitors.download_monitor(ip) {
                            Some(mon) => !mon.peer_choking(),
                            _ => false,
                        })
                        .count();
                    nonchoking_owners == 1
                } else {
                    false
                }
            }),
            None => false,
        };
        if only_nonchoking_owner {
            dm.submit_outbound(pwp::DownloaderMessage::Interested);
            ctx.state.interest.seeders += 1;
        }
    }
}

fn show_interest_to_any_nonchoking_owners(ctx: &mut Context) {
    let piece_tracker = ctx.piece_tracker;
    let monitors = &mut ctx.monitor_owner;

    for (addr, dm) in monitors
        .all_download_monitors()
        .filter(|(_addr, mon)| !mon.peer_choking() && !mon.am_interested())
    {
        if ctx.state.interest.seeders >= MAX_SEEDERS_COUNT {
            break;
        }
        let owns_missing_piece =
            piece_tracker.get_peer_pieces(addr).map(|mut it| it.next()).is_some();

        if owns_missing_piece {
            dm.submit_outbound(pwp::DownloaderMessage::Interested);
            ctx.state.interest.seeders += 1;
        }
    }
}

fn show_desperate_interest(ctx: &mut Context) {
    let non_choking_peer_that_owns = |piece_index: usize| {
        let owners_ips = ctx.piece_tracker.get_piece_owners(piece_index)?;
        owners_ips
            .filter_map(|ip| ctx.monitor_owner.download_monitor(ip))
            .find(|dm| !dm.peer_choking())
    };
    let has_non_choking_relevant_peers = ctx
        .piece_tracker
        .get_rarest_pieces()
        .any(|piece_index| non_choking_peer_that_owns(piece_index).is_some());
    if has_non_choking_relevant_peers {
        return;
    }
    for missing_piece in ctx.piece_tracker.get_rarest_pieces() {
        if let Some(piece_owners_it) = ctx.piece_tracker.get_piece_owners(missing_piece) {
            for dm in piece_owners_it.filter_map(|ip| ctx.monitor_owner.download_monitor(ip)) {
                debug_assert!(dm.peer_choking());
                if !dm.am_interested() {
                    dm.submit_outbound(pwp::DownloaderMessage::Interested);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::testutils::*;
    use std::collections::HashSet;
    use std::iter;

    #[test]
    fn test_update_state_seeders_count() {
        let mut f = Fixture::new(10, 1024);

        // given
        let am_interested_vals = [false, true, false, true];
        let peer_choking_vals = [false, false, true, true];

        for (am_interested, peer_choking) in iter::zip(am_interested_vals, peer_choking_vals) {
            let (_ip, dm, _um) = f.monitor_owner.add_peer();
            dm.am_interested.set(am_interested);
            dm.peer_choking.set(peer_choking);
        }

        // when
        update_state(&mut f.ctx());

        // then
        assert_eq!(1, f.state.interest.seeders);
        assert_eq!(0, f.state.interest.leechers)
    }

    #[test]
    fn test_update_state_leechers_count() {
        let mut f = Fixture::new(10, 1024);

        // given
        let peer_interested_vals = [false, true, false, true];
        let am_choking_vals = [false, false, true, true];

        for (peer_interested, am_choking) in iter::zip(peer_interested_vals, am_choking_vals) {
            let (_ip, _dm, um) = f.monitor_owner.add_peer();
            um.peer_interested.set(peer_interested);
            um.am_choking.set(am_choking);
        }

        // when
        update_state(&mut f.ctx());

        // then
        assert_eq!(1, f.state.interest.leechers);
        assert_eq!(0, f.state.interest.seeders);
    }

    #[test]
    fn test_remove_unneeded_interest() {
        let mut f = Fixture::new(10, 1024);

        // given
        let (_ip, dm, _um) = f.monitor_owner.add_peer();
        dm.am_interested.set(true);
        dm.peer_choking.set(false);
        f.state.interest.seeders += 1;

        let (_ip, dm, _um) = f.monitor_owner.add_peer();
        dm.am_interested.set(true);
        dm.peer_choking.set(true);

        let (ip, dm, _um) = f.monitor_owner.add_peer();
        dm.am_interested.set(true);
        dm.peer_choking.set(false);
        f.state.interest.seeders += 1;
        f.piece_tracker.add_single_record(ip, 0);

        let (ip, dm, _um) = f.monitor_owner.add_peer();
        dm.am_interested.set(true);
        dm.peer_choking.set(true);
        f.piece_tracker.add_single_record(ip, 0);

        // when
        remove_unneeded_interest(&mut f.ctx());

        // then
        for (dm, _um) in f.monitor_owner.monitors.values().take(2) {
            assert_eq!(1, dm.submitted_msgs.borrow().len());
            assert!(matches!(
                dm.submitted_msgs.borrow().back().unwrap(),
                pwp::DownloaderMessage::NotInterested
            ));
        }
        for (dm, _um) in f.monitor_owner.monitors.values().skip(2) {
            assert!(dm.submitted_msgs.borrow().is_empty());
        }
        assert_eq!(1, f.state.interest.seeders);
    }

    #[test]
    fn test_show_interest_to_nonchoking_single_owners() {
        let mut f = Fixture::new(10, 1024);

        // given
        let (choking_single_owner_ip, _, _) = f.monitor_owner.add_peer();
        f.piece_tracker.add_single_record(choking_single_owner_ip, 0);

        let (nonchoking_single_owner_ip, dm, _) = f.monitor_owner.add_peer();
        dm.peer_choking.set(false);
        f.piece_tracker.add_single_record(nonchoking_single_owner_ip, 1);

        let (choking_nonsignle_owner_ip, _, _) = f.monitor_owner.add_peer();
        f.piece_tracker.add_single_record(choking_nonsignle_owner_ip, 2);

        let (only_nonchoking_owner_ip, dm, _) = f.monitor_owner.add_peer();
        dm.peer_choking.set(false);
        f.piece_tracker.add_single_record(only_nonchoking_owner_ip, 2);

        let (single_owner_seeder_ip, dm, _) = f.monitor_owner.add_peer();
        dm.peer_choking.set(false);
        dm.am_interested.set(true);
        f.piece_tracker.add_single_record(single_owner_seeder_ip, 3);

        f.state.interest.seeders = 1;

        // when
        show_interest_to_nonchoking_single_owners(&mut f.ctx());

        // then
        let (dm, _) = &f.monitor_owner.monitors.get(&nonchoking_single_owner_ip).unwrap();
        assert_eq!(1, dm.submitted_msgs.borrow().len());
        assert!(matches!(
            dm.submitted_msgs.borrow().back().unwrap(),
            pwp::DownloaderMessage::Interested
        ));

        for (ip, (dm, _um)) in &f.monitor_owner.monitors {
            if *ip != nonchoking_single_owner_ip {
                assert!(dm.submitted_msgs.borrow().is_empty(), "{ip}");
            }
        }

        assert_eq!(2, f.state.interest.seeders);
    }

    #[test]
    fn test_show_interest_to_the_only_nonchoking_owners() {
        let mut f = Fixture::new(10, 1024);

        // given
        let piece_index = 0;

        let (choking_nonsignle_owner_ip, _, _) = f.monitor_owner.add_peer();
        f.piece_tracker.add_single_record(choking_nonsignle_owner_ip, piece_index);

        let (nonchoking_nonsignle_owner_ip, dm, _) = f.monitor_owner.add_peer();
        dm.peer_choking.set(false);
        f.piece_tracker.add_single_record(nonchoking_nonsignle_owner_ip, piece_index);

        let piece_index = 1;

        let (choking_nonsignle_owner_ip_2, _, _) = f.monitor_owner.add_peer();
        f.piece_tracker.add_single_record(choking_nonsignle_owner_ip_2, piece_index);

        let (seeder_ip, dm, _) = f.monitor_owner.add_peer();
        dm.peer_choking.set(false);
        dm.am_interested.set(true);
        f.piece_tracker.add_single_record(seeder_ip, piece_index);

        let (nonchoking_nonsignle_owner_ip_2, dm, _) = f.monitor_owner.add_peer();
        dm.peer_choking.set(false);
        f.piece_tracker.add_single_record(nonchoking_nonsignle_owner_ip_2, piece_index);

        f.state.interest.seeders = 1;

        // when
        show_interest_to_only_nonchoking_owners(&mut f.ctx());

        // then
        let (dm, _) = &f.monitor_owner.monitors.get(&nonchoking_nonsignle_owner_ip).unwrap();
        assert_eq!(1, dm.submitted_msgs.borrow().len());
        assert!(matches!(
            dm.submitted_msgs.borrow().back().unwrap(),
            pwp::DownloaderMessage::Interested
        ));

        for (ip, (dm, _um)) in &f.monitor_owner.monitors {
            if *ip != nonchoking_nonsignle_owner_ip {
                assert!(dm.submitted_msgs.borrow().is_empty(), "{ip}");
            }
        }

        assert_eq!(2, f.state.interest.seeders);
    }

    #[test]
    fn test_show_interest_to_any_nonchoking_owners() {
        let mut f = Fixture::new(10, 1024);

        // given
        f.state.interest.seeders = MAX_SEEDERS_COUNT - 3;

        let piece_index = 0;

        let (nonchoking_nonsignle_owner_ip_1, dm, _) = f.monitor_owner.add_peer();
        dm.peer_choking.set(false);
        f.piece_tracker.add_single_record(nonchoking_nonsignle_owner_ip_1, piece_index);

        let (choking_nonsignle_owner_ip, _, _) = f.monitor_owner.add_peer();
        f.piece_tracker.add_single_record(choking_nonsignle_owner_ip, piece_index);

        let (nonchoking_nonsignle_owner_ip_2, dm, _) = f.monitor_owner.add_peer();
        dm.peer_choking.set(false);
        f.piece_tracker.add_single_record(nonchoking_nonsignle_owner_ip_2, piece_index);

        let piece_index = 1;

        let (nonchoking_nonsignle_owner_ip_3, dm, _) = f.monitor_owner.add_peer();
        dm.peer_choking.set(false);
        f.piece_tracker.add_single_record(nonchoking_nonsignle_owner_ip_3, piece_index);

        let (nonchoking_nonsignle_owner_ip_4, dm, _) = f.monitor_owner.add_peer();
        dm.peer_choking.set(false);
        f.piece_tracker.add_single_record(nonchoking_nonsignle_owner_ip_4, piece_index);

        let (seeder_ip, dm, _) = f.monitor_owner.add_peer();
        dm.peer_choking.set(false);
        dm.am_interested.set(true);
        f.piece_tracker.add_single_record(seeder_ip, piece_index);

        // when
        show_interest_to_any_nonchoking_owners(&mut f.ctx());

        // then
        let interesting_peers = HashSet::from([
            nonchoking_nonsignle_owner_ip_1,
            nonchoking_nonsignle_owner_ip_2,
            nonchoking_nonsignle_owner_ip_3,
        ]);
        for ip in &interesting_peers {
            let (dm, _) = &f.monitor_owner.monitors.get(ip).unwrap();
            assert_eq!(1, dm.submitted_msgs.borrow().len());
            assert!(
                matches!(
                    dm.submitted_msgs.borrow().back().unwrap(),
                    pwp::DownloaderMessage::Interested
                ),
                "{ip}"
            );
        }
        for (ip, (dm, _)) in f
            .monitor_owner
            .monitors
            .iter()
            .filter(|(ip, _)| !interesting_peers.contains(ip))
        {
            assert!(dm.submitted_msgs.borrow().is_empty(), "{ip}");
        }
    }

    #[test]
    fn test_show_desperate_interest() {
        let mut f = Fixture::new(10, 1024);

        // given
        let (choking_nonsignle_owner_ip, _, _) = f.monitor_owner.add_peer();
        f.piece_tracker.add_single_record(choking_nonsignle_owner_ip, 0);

        let (choking_nonsignle_owner_ip, _, _) = f.monitor_owner.add_peer();
        f.piece_tracker.add_single_record(choking_nonsignle_owner_ip, 0);

        let (nonchoking_owner_ip, dm, _) = f.monitor_owner.add_peer();
        dm.peer_choking.set(false);
        f.piece_tracker.add_single_record(nonchoking_owner_ip, 1);

        // when
        show_desperate_interest(&mut f.ctx());

        // then
        for (ip, (dm, _um)) in &f.monitor_owner.monitors {
            assert!(dm.submitted_msgs.borrow().is_empty(), "{ip}");
        }

        // when
        f.piece_tracker.forget_peer(nonchoking_owner_ip);
        f.monitor_owner.monitors.remove(&nonchoking_owner_ip);
        show_desperate_interest(&mut f.ctx());

        // then
        for (ip, (dm, _um)) in &f.monitor_owner.monitors {
            assert_eq!(1, dm.submitted_msgs.borrow().len());
            assert!(
                matches!(
                    dm.submitted_msgs.borrow().back().unwrap(),
                    pwp::DownloaderMessage::Interested
                ),
                "{ip}"
            );
        }

        // when
        for (dm, _) in f.monitor_owner.monitors.values() {
            dm.am_interested.set(true);
            dm.submitted_msgs.borrow_mut().clear();
        }
        show_desperate_interest(&mut f.ctx());

        // then
        for (ip, (dm, _um)) in &f.monitor_owner.monitors {
            assert!(dm.submitted_msgs.borrow().is_empty(), "{ip}");
        }
    }
}
