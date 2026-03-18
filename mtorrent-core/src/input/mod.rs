mod magnet;
mod metainfo;

pub use magnet::MagnetLink;
pub use metainfo::Metainfo;

/// Bencode parser.
pub use mtorrent_utils::benc; // re-export
