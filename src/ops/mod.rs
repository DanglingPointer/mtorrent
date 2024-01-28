#![allow(dead_code)]
mod ctx;
mod download;
mod upload;

pub use ctx::Ctx;

const MAX_BLOCK_SIZE: usize = 16384;
