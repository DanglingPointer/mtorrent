//! File format (bencoded dictionary):
//! ```text
//! {
//!     <info hash 1>: <bitfield>,
//!     <info hash 2>: <bitfield>,
//!     ...
//!     "trackers": [ <tracker1>, <tracker2>, ... ]
//! }
//! ```
//! No fields are mandatory, i.e. a config can contain only trackers or only bitfields, or both.
use crate::pwp::Bitfield;
use crate::utils::benc;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::{fs, io};

const FILENAME: &str = ".mtorrent";
const TRACKERS_KEY: &str = "trackers";

#[derive(Default)]
struct Content {
    bitfields: HashMap<[u8; 20], Bitfield>,
    trackers: BTreeSet<String>,
}

fn decode_content(content: benc::Element) -> Option<Content> {
    use benc::Element;
    if let Element::Dictionary(root) = content {
        let mut bitfields = HashMap::new();
        let mut trackers = BTreeSet::new();
        for (key, value) in root {
            match (key, value) {
                (Element::ByteString(key), Element::ByteString(value)) if key.len() == 20 => {
                    let mut info_hash = [0u8; 20];
                    info_hash.copy_from_slice(&key);
                    let bitfield = Bitfield::from_vec(value);
                    bitfields.insert(info_hash, bitfield);
                }
                (Element::ByteString(key), Element::List(value))
                    if key == TRACKERS_KEY.as_bytes() =>
                {
                    trackers.extend(value.into_iter().filter_map(|tracker| match tracker {
                        Element::ByteString(tracker) => String::from_utf8(tracker).ok(),
                        _ => None,
                    }));
                }
                _ => (),
            }
        }
        Some(Content {
            bitfields,
            trackers,
        })
    } else {
        None
    }
}

fn encode_content(content: Content) -> benc::Element {
    use benc::Element;
    let mut root = BTreeMap::new();
    for (info_hash, bitfield) in content.bitfields {
        let key = Element::ByteString(Vec::from(info_hash));
        let value = Element::ByteString(bitfield.into_vec());
        root.insert(key, value);
    }
    if !content.trackers.is_empty() {
        root.insert(
            Element::from(TRACKERS_KEY),
            Element::List(
                content
                    .trackers
                    .into_iter()
                    .map(|tracker| Element::ByteString(tracker.into_bytes()))
                    .collect(),
            ),
        );
    }
    Element::Dictionary(root)
}

fn read_config_file(filepath: impl AsRef<Path>) -> io::Result<Content> {
    let buf = fs::read(filepath)?;
    if let Ok(bencoded) = benc::Element::from_bytes(&buf) {
        decode_content(bencoded).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "unexpected bencoded structure")
        })
    } else {
        Err(io::Error::new(io::ErrorKind::InvalidInput, "not bencoded"))
    }
}

pub fn load_state(config_dir: impl AsRef<Path>, info_hash: &[u8; 20]) -> io::Result<Bitfield> {
    let mut content = read_config_file(config_dir.as_ref().join(FILENAME))?;
    content
        .bitfields
        .remove(info_hash)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "info hash not found"))
}

pub fn load_trackers(
    config_dir: impl AsRef<Path>,
) -> io::Result<impl ExactSizeIterator<Item = String>> {
    let content = read_config_file(config_dir.as_ref().join(FILENAME))?;
    Ok(content.trackers.into_iter())
}

pub fn save_state(
    config_dir: impl AsRef<Path>,
    info_hash: &[u8; 20],
    state: Bitfield,
) -> io::Result<()> {
    let config_path = config_dir.as_ref().join(FILENAME);
    let mut content = read_config_file(&config_path).unwrap_or_default();
    let current_state = content.bitfields.entry(*info_hash).or_default();
    *current_state = state;

    let bencoded_content = encode_content(content);
    fs::write(config_path, bencoded_content.to_bytes())
}

pub fn save_trackers<T: Into<String>>(
    config_dir: impl AsRef<Path>,
    trackers: impl Iterator<Item = T>,
) -> io::Result<()> {
    let config_path = config_dir.as_ref().join(FILENAME);
    let mut content = read_config_file(&config_path).unwrap_or_default();
    content.trackers.extend(trackers.map(Into::into));

    let bencoded_content = encode_content(content);
    fs::write(config_path, bencoded_content.to_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_write_and_read_config_for_single_torrent() {
        let dir = "test_write_and_read_config_for_single_torrent";
        fs::create_dir_all(dir).unwrap();

        let info_hash = [b'a'; 20];
        let piece_count = 668;
        let bitfield = Bitfield::repeat(true, piece_count);

        assert!(
            matches!(load_state(dir, &info_hash), Err(e) if e.kind() == io::ErrorKind::NotFound)
        );

        save_state(dir, &info_hash, bitfield.clone()).unwrap();
        assert!(Path::new(dir).join(FILENAME).is_file());

        let mut loaded_state = load_state(dir, &info_hash).unwrap();
        loaded_state.resize(bitfield.len(), false);
        assert_eq!(bitfield, loaded_state);

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_write_and_read_and_modify_trackers() {
        let dir = "test_write_and_read_and_modify_trackers";
        fs::create_dir_all(dir).unwrap();

        assert!(matches!(load_trackers(dir), Err(e) if e.kind() == io::ErrorKind::NotFound));

        let trackers = ["http://tracker1.com", "http://tracker2.com"];
        save_trackers(dir, trackers.into_iter()).unwrap();
        assert!(Path::new(dir).join(FILENAME).is_file());

        let loaded_trackers = load_trackers(dir).unwrap();
        assert_eq!(
            trackers.into_iter().map(ToString::to_string).collect::<BTreeSet<_>>(),
            loaded_trackers.collect::<BTreeSet<_>>(),
        );

        let new_trackers = ["http://tracker3.com", "http://tracker4.com"];
        save_trackers(dir, new_trackers.into_iter()).unwrap();

        let loaded_trackers = load_trackers(dir).unwrap();
        assert_eq!(
            [trackers, new_trackers]
                .into_iter()
                .flatten()
                .map(ToString::to_string)
                .collect::<BTreeSet<_>>(),
            loaded_trackers.collect::<BTreeSet<_>>(),
        );

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_modify_and_read_config_for_multiple_torrents() {
        let dir = "test_modify_and_read_config_for_multiple_torrents";
        fs::create_dir_all(dir).unwrap();

        let info_hash_1 = [b'1'; 20];
        let piece_count_1 = 668;
        let bitfield_1 = {
            let mut tmp = Bitfield::repeat(true, piece_count_1);
            for (index, mut is_piece_present) in tmp.iter_mut().enumerate() {
                if index % 2 == 0 {
                    is_piece_present.set(false);
                }
            }
            tmp
        };

        let info_hash_2 = [b'2'; 20];
        let piece_count_2 = 56;
        let bitfield_2 = {
            let mut tmp = Bitfield::repeat(true, piece_count_2);
            for (index, mut is_piece_present) in tmp.iter_mut().enumerate() {
                if index % 2 != 0 {
                    is_piece_present.set(false);
                }
            }
            tmp
        };

        save_state(dir, &info_hash_1, bitfield_1.clone()).unwrap();
        save_state(dir, &info_hash_2, bitfield_2.clone()).unwrap();
        assert!(Path::new(dir).join(FILENAME).is_file());

        let loaded_state_1 = load_state(dir, &info_hash_1).unwrap();
        assert_eq!(bitfield_1, loaded_state_1[0..bitfield_1.len()]);

        let loaded_state_2 = load_state(dir, &info_hash_2).unwrap();
        assert_eq!(bitfield_2, loaded_state_2[0..bitfield_2.len()]);

        let modified_bitfield_1 = {
            let mut tmp = bitfield_1.clone();
            for mut is_piece_present in &mut tmp {
                is_piece_present.set(true);
            }
            tmp
        };
        save_state(dir, &info_hash_1, modified_bitfield_1.clone()).unwrap();

        let loaded_state_1 = load_state(dir, &info_hash_1).unwrap();
        assert_eq!(modified_bitfield_1, loaded_state_1[0..modified_bitfield_1.len()]);

        let loaded_state_2 = load_state(dir, &info_hash_2).unwrap();
        assert_eq!(bitfield_2, loaded_state_2[0..bitfield_2.len()]);

        fs::remove_dir_all(dir).unwrap();
    }
}
