mod interest;
mod listeners;
mod matcher;
mod requests;
#[cfg(test)]
mod testutils;

pub use listeners::{DownloadChannelMonitor, MonitorOwner, Timer, UploadChannelMonitor};

#[derive(Default)]
pub struct State {
    interest: interest::State,
}

pub struct Context<'a> {
    pub piece_info: &'a crate::data::PieceInfo,
    pub local_availability: &'a crate::data::BlockAccountant,
    pub piece_tracker: &'a crate::data::PieceTracker,
    pub monitor_owner: &'a dyn listeners::MonitorOwner,
    pub state: &'a mut State,
    pub timer: &'a mut dyn listeners::Timer,
}

pub fn run(ctx: &mut Context) {
    interest::update_interest(ctx);
    requests::update_requests(ctx);
}
