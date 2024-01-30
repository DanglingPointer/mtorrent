#![allow(dead_code)]
mod ctx;
mod download;
mod peer;
mod upload;

#[cfg(test)]
mod tests;

pub use ctx::Ctx;

const MAX_BLOCK_SIZE: usize = 16384;
