#[macro_use]
mod ctx;
mod ctrl;
mod download;
mod listener;
mod peer;
mod tracker;
mod upload;

#[cfg(test)]
mod tests;

pub use ctx::new_ctx;
pub use ctx::periodic_state_dump;
pub use ctx::Handle as CtxHandle;
pub use listener::run_pwp_listener;
pub use peer::{incoming_pwp_connection, outgoing_pwp_connection};
pub use tracker::run_periodic_announces;
pub use tracker::ResponseData as TrackerResponse;

const MAX_BLOCK_SIZE: usize = 16384;
