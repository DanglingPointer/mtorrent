//! Trackers file (bencoded dictionary or JSON):
//! ```text
//! {
//!     "trackers": [ <tracker1>, <tracker2>, ... ]
//! }
//! ```
//!
//! Progress file (bencoded dictionary):
//! ```text
//! {
//!     <info hash>: <bitfield>
//! }
//! ```
use crate::pwp::Bitfield;
use crate::utils::benc::Element;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::{fs, io};

const FILENAME: &str = ".mtorrent";
const TRACKERS_KEY: &str = "trackers";

#[derive(Default, Serialize, Deserialize)]
struct Trackers {
    trackers: BTreeSet<String>,
}

impl Trackers {
    fn from_file(filepath: impl AsRef<Path>) -> io::Result<Self> {
        let buf = fs::read(filepath)?;

        if let Ok(bencode) = Element::from_bytes(&buf) {
            if let Element::Dictionary(root) = bencode {
                let mut trackers = BTreeSet::new();
                for (key, value) in root {
                    match (key, value) {
                        (Element::ByteString(key), Element::List(value))
                            if key == TRACKERS_KEY.as_bytes() =>
                        {
                            trackers.extend(value.into_iter().filter_map(
                                |tracker| match tracker {
                                    Element::ByteString(tracker) => String::from_utf8(tracker).ok(),
                                    _ => None,
                                },
                            ));
                        }
                        _ => (),
                    }
                }
                Ok(Self { trackers })
            } else {
                Err(io::Error::new(io::ErrorKind::InvalidInput, "not bencoded"))
            }
        } else {
            Ok(serde_json::from_slice(&buf)?)
        }
    }
}

pub fn load_trackers(
    config_dir: impl AsRef<Path>,
) -> io::Result<impl ExactSizeIterator<Item = String>> {
    let Trackers { trackers } = Trackers::from_file(config_dir.as_ref().join(FILENAME))?;
    Ok(trackers.into_iter())
}

pub fn save_trackers<T: Into<String>>(
    config_dir: impl AsRef<Path>,
    trackers: impl IntoIterator<Item = T>,
) -> io::Result<()> {
    let config_path = config_dir.as_ref().join(FILENAME);
    let mut saved_trackers = Trackers::from_file(&config_path).unwrap_or_default();
    saved_trackers.trackers.extend(trackers.into_iter().map(Into::into));

    let json_bytes = serde_json::to_vec_pretty(&saved_trackers)?;
    fs::write(config_path, json_bytes)?;
    Ok(())
}

pub fn load_state(config_dir: impl AsRef<Path>, info_hash: &[u8; 20]) -> io::Result<Bitfield> {
    let buf = fs::read(config_dir.as_ref().join(FILENAME))?;
    let bencode = Element::from_bytes(&buf)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "not bencoded"))?;

    if let Element::Dictionary(mut root) = bencode {
        let key = Element::ByteString(info_hash.into());
        if let Some(Element::ByteString(bitfield)) = root.remove(&key) {
            Ok(Bitfield::from_vec(bitfield))
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "info hash not found"))
        }
    } else {
        Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected bencoded structure"))
    }
}

pub fn save_state(
    config_dir: impl AsRef<Path>,
    info_hash: &[u8; 20],
    state: Bitfield,
) -> io::Result<()> {
    let key = Element::ByteString(info_hash.into());
    let value = Element::ByteString(state.into_vec());
    let root: BTreeMap<Element, Element> = [(key, value)].into();
    let bencode = Element::Dictionary(root);

    let config_path = config_dir.as_ref().join(FILENAME);
    fs::write(config_path, bencode.to_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_read_bencoded_trackers() {
        let dir = "test_read_bencoded_trackers";
        fs::create_dir_all(dir).unwrap();
        fs::write(
            Path::new(dir).join(FILENAME),
            b"d8:trackersl19:http://tracker1.com19:http://tracker2.comee",
        )
        .unwrap();

        let loaded_trackers = load_trackers(dir).unwrap();
        assert_eq!(
            ["http://tracker1.com", "http://tracker2.com"]
                .into_iter()
                .map(ToString::to_string)
                .collect::<BTreeSet<_>>(),
            loaded_trackers.collect::<BTreeSet<_>>(),
        );

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_write_json_trackers() {
        let dir = "test_write_json_trackers";
        fs::create_dir_all(dir).unwrap();

        save_trackers(dir, ["http://tracker1.com", "http://tracker2.com"]).unwrap();

        let content = fs::read_to_string(Path::new(dir).join(FILENAME)).unwrap();
        assert_eq!(
            content,
            r#"{
  "trackers": [
    "http://tracker1.com",
    "http://tracker2.com"
  ]
}"#,
        );

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_write_and_read_and_modify_trackers() {
        let dir = "test_write_and_read_and_modify_trackers";
        fs::create_dir_all(dir).unwrap();

        assert!(matches!(load_trackers(dir), Err(e) if e.kind() == io::ErrorKind::NotFound));

        let trackers = ["http://tracker1.com", "http://tracker2.com"];
        save_trackers(dir, trackers).unwrap();
        assert!(Path::new(dir).join(FILENAME).is_file());

        let loaded_trackers = load_trackers(dir).unwrap();
        assert_eq!(
            trackers.into_iter().map(ToString::to_string).collect::<BTreeSet<_>>(),
            loaded_trackers.collect::<BTreeSet<_>>(),
        );

        let new_trackers = ["http://tracker3.com", "http://tracker4.com"];
        save_trackers(dir, new_trackers).unwrap();

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
}
