mod ctrl;
mod ctx;
mod download;
mod peer;
mod tracker;
mod upload;

#[cfg(test)]
mod tests;

pub use ctx::Handle as CtxHandle;
pub use ctx::Owner as CtxOwner;
pub use peer::{incoming_pwp_connection, outgoing_pwp_connection};
pub use tracker::run_periodic_announces;

const MAX_BLOCK_SIZE: usize = 16384;
