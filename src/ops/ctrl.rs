use super::ctx;
use std::collections::HashSet;
use std::net::SocketAddr;

pub fn should_activate_download(peer_ip: &SocketAddr, ctx: &ctx::Ctx) -> bool {
    ctx.piece_tracker
        .get_peer_pieces(peer_ip)
        .is_some_and(|mut it| it.next().is_some())
}

pub fn pieces_to_request(peer_ip: &SocketAddr, ctx: &ctx::Ctx) -> Vec<usize> {
    const MAX_REQUEST_COUNT: usize = 10;
    let available_pieces: HashSet<usize> =
        if let Some(it) = ctx.piece_tracker.get_peer_pieces(peer_ip) {
            it.collect()
        } else {
            Default::default()
        };
    let mut ret = Vec::new();
    if !available_pieces.is_empty() {
        ret.reserve(MAX_REQUEST_COUNT);
        for piece in ctx
            .piece_tracker
            .get_rarest_pieces()
            .filter(|piece| {
                available_pieces.contains(piece) && !ctx.pending_requests.is_piece_requested(*piece)
            })
            .take(MAX_REQUEST_COUNT)
        {
            ret.push(piece);
        }
        if ret.len() < MAX_REQUEST_COUNT {
            for piece in ctx
                .piece_tracker
                .get_rarest_pieces()
                .filter(|piece| available_pieces.contains(piece))
            {
                if !ret.contains(&piece) {
                    ret.push(piece);
                }
                if ret.len() == MAX_REQUEST_COUNT {
                    break;
                }
            }
        }
    }
    ret
}

const MAX_LEECH_COUNT: usize = 30;

pub fn should_activate_upload(peer_ip: &SocketAddr, ctx: &ctx::Ctx) -> bool {
    // am_choking == true
    ctx.peer_states.get(peer_ip).is_some_and(|(download_state, upload_state)| {
        (download_state.am_interested && download_state.peer_choking)
            || (upload_state.peer_interested && ctx.peer_states.leeches_count() < MAX_LEECH_COUNT)
    })
}

pub fn should_stop_upload(peer_ip: &SocketAddr, ctx: &ctx::Ctx) -> bool {
    // peer_interested == true
    if let Some((download_state, _upload_state)) = ctx.peer_states.get(peer_ip) {
        let active_seeder = download_state.am_interested
            && !download_state.peer_choking
            && download_state.bytes_received > 0;
        !active_seeder && ctx.peer_states.leeches_count() >= MAX_LEECH_COUNT
    } else {
        true
    }
}
