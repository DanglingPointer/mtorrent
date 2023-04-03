mod interest;
mod listeners;
#[cfg(test)]
mod testutils;

pub use interest::update_interest;
pub use listeners::{DownloadChannelMonitor, MonitorOwner, Timer, UploadChannelMonitor};

#[derive(Default)]
pub struct State {
    interest: interest::State,
}

pub struct Context<'a> {
    pub local_availability: &'a crate::data::BlockAccountant,
    pub piece_tracker: &'a crate::data::PieceTracker,
    pub monitor_owner: &'a dyn listeners::MonitorOwner,
    pub state: &'a mut State,
    pub timer: &'a mut dyn listeners::Timer,
}
