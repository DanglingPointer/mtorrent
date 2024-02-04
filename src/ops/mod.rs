mod ctrl;
mod ctx;
mod download;
pub mod peer;
mod upload;

#[cfg(test)]
mod tests;

pub use ctx::Ctx;
pub use ctx::Handle as CtxHandle;
pub use ctx::Owner as CtxOwner;

const MAX_BLOCK_SIZE: usize = 16384;
