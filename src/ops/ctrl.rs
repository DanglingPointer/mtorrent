use super::ctx;
use crate::utils::meta;
use crate::{pwp, sec};
use core::fmt;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;
use std::{cmp, error, io};

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
    // libtorrent supports max 250 queued requests, hence:
    // 250 * 16kB == piece_len * piece_count
    let max_request_count =
        cmp::min(50, cmp::max(1, pwp::MAX_BLOCK_SIZE * 250 / ctx.pieces.piece_len(0)));
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

const MAX_LEECH_COUNT: usize = 20;

fn should_seed_to_peer(state: &pwp::PeerState, leech_count: usize) -> bool {
    let is_active_seeder = || {
        state.download.am_interested
            && !state.download.peer_choking
            && state.download.bytes_received > 0 // need to check this because last_download_time is set upon creation
            && state.last_download_time.elapsed() < sec!(11)
    };
    if !state.upload.peer_interested {
        false
    } else if state.download.am_interested && state.download.peer_choking {
        true
    } else {
        leech_count < MAX_LEECH_COUNT
            && (is_active_seeder()
                || state.last_upload_time.elapsed() > sec!(30)
                || state.upload.bytes_sent == 0)
    }
}

pub enum IdleUploadAction {
    ActivateUpload,
    Linger(Duration),
}

pub fn idle_upload_next_action(peer_addr: &SocketAddr, ctx: &ctx::MainCtx) -> IdleUploadAction {
    if let Some(state) = ctx.peer_states.get(peer_addr) {
        if should_seed_to_peer(state, ctx.peer_states.leeches_count()) {
            IdleUploadAction::ActivateUpload
        } else {
            IdleUploadAction::Linger(if state.download.am_interested {
                sec!(1)
            } else {
                sec!(10)
            })
        }
    } else {
        debug_assert!(false, "This should never happen");
        IdleUploadAction::Linger(sec!(30))
    }
}

pub enum LeechUploadAction {
    DeactivateUpload,
    Serve(Duration),
}

pub fn active_upload_next_action(peer_addr: &SocketAddr, ctx: &ctx::MainCtx) -> LeechUploadAction {
    if let Some(state) = ctx.peer_states.get(peer_addr) {
        if !should_seed_to_peer(state, ctx.peer_states.leeches_count()) {
            LeechUploadAction::DeactivateUpload
        } else {
            LeechUploadAction::Serve(if state.upload.bytes_sent == 0 {
                sec!(30)
            } else {
                sec!(10)
            })
        }
    } else {
        debug_assert!(false, "This should never happen");
        LeechUploadAction::DeactivateUpload
    }
}

pub fn is_finished(ctx: &ctx::MainCtx) -> bool {
    ctx.accountant.missing_bytes() == 0 && ctx.peer_states.leeches_count() == 0
}

pub fn verify_metadata(ctx: &mut ctx::PreliminaryCtx) -> bool {
    if ctx.metainfo_pieces.is_empty() || !ctx.metainfo_pieces.all() {
        false
    } else {
        match meta::Metainfo::new(&ctx.metainfo) {
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

const MAX_CONNECTED_PEERS_COUNT: usize = 50;

#[derive(Debug)]
pub enum GrantError {
    ConnectionsLimitReached,
    AlreadyConnected(SocketAddr),
}

impl fmt::Display for GrantError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GrantError::ConnectionsLimitReached => write!(f, "connections limit reached"),
            GrantError::AlreadyConnected(addr) => write!(f, "already connected to {addr}"),
        }
    }
}

impl error::Error for GrantError {}

impl From<GrantError> for io::Error {
    fn from(value: GrantError) -> Self {
        io::Error::new(io::ErrorKind::PermissionDenied, Box::new(value))
    }
}

pub fn grant_preliminary_connection_permission(
    ctx: &ctx::PreliminaryCtx,
    candidate: &SocketAddr,
) -> Result<(), GrantError> {
    if ctx.connected_peers.len() >= MAX_CONNECTED_PEERS_COUNT {
        Err(GrantError::ConnectionsLimitReached)
    } else {
        match ctx.connected_peers.get(candidate) {
            Some(_) => Err(GrantError::AlreadyConnected(*candidate)),
            None => Ok(()),
        }
    }
}

pub fn grant_main_connection_permission(
    ctx: &ctx::MainCtx,
    candidate: &SocketAddr,
) -> Result<(), GrantError> {
    if ctx.peer_states.iter().count() >= MAX_CONNECTED_PEERS_COUNT {
        Err(GrantError::ConnectionsLimitReached)
    } else {
        match ctx.peer_states.get(candidate) {
            Some(_) => Err(GrantError::AlreadyConnected(*candidate)),
            None => Ok(()),
        }
    }
}
