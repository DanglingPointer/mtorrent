use super::peers::PeerManager;
use crate::data;

pub struct Context {
    pub local_availability: data::BlockAccountant,
    pub piece_tracker: data::PieceTracker,
    pub peermgr: PeerManager,
}
