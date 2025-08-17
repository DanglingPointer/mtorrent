//! Trackers file (JSON):
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
use mtorrent_core::pwp::Bitfield;
use mtorrent_utils::benc::Element;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Seek;
use std::path::Path;
use std::{fs, io};

const FILENAME: &str = ".mtorrent";

#[derive(Default, Serialize, Deserialize)]
struct Trackers {
    trackers: BTreeSet<String>,
}

/// Read tracker addresses from the trackers file (JSON).
pub fn load_trackers(
    config_dir: impl AsRef<Path>,
) -> io::Result<impl ExactSizeIterator<Item = String>> {
    let file = fs::File::open(config_dir.as_ref().join(FILENAME))?;
    let Trackers { trackers } = serde_json::from_reader(io::BufReader::new(file))?;
    Ok(trackers.into_iter())
}

/// Write tracker addresses to the trackers file (JSON).
/// If the file already exists, the new trackers will be appended.
pub fn save_trackers<T: Into<String>>(
    config_dir: impl AsRef<Path>,
    trackers: impl IntoIterator<Item = T>,
) -> io::Result<()> {
    // r+w mode, open existing or create new
    let mut file = fs::File::options()
        .write(true)
        .read(true)
        .create(true)
        .truncate(false)
        .append(false)
        .open(config_dir.as_ref().join(FILENAME))?;

    // parse existing content
    let mut saved_trackers: Trackers =
        serde_json::from_reader(io::BufReader::new(&file)).unwrap_or_default();

    // append new trackers
    saved_trackers.trackers.extend(trackers.into_iter().map(Into::into));

    // overwrite the file
    file.seek(io::SeekFrom::Start(0))?;
    serde_json::to_writer_pretty(io::BufWriter::new(&file), &saved_trackers)?;
    Ok(())
}

/// Read the bencoded progress file, and get the state of `info_hash`.
pub fn load_state(config_dir: impl AsRef<Path>, info_hash: &[u8; 20]) -> io::Result<Bitfield> {
    let buf = fs::read(config_dir.as_ref().join(FILENAME))?;
    let bencode = Element::from_bytes(&buf)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "not bencoded"))?;

    let Element::Dictionary(mut root) = bencode else {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected bencoded structure"));
    };
    let key = Element::ByteString(info_hash.into());
    if let Some(Element::ByteString(bitfield)) = root.remove(&key) {
        Ok(Bitfield::from_vec(bitfield))
    } else {
        Err(io::Error::new(io::ErrorKind::NotFound, "info hash not found"))
    }
}

/// Write the state of `info_hash` to the bencoded progress file.
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
    fn test_write_and_read_state_for_single_torrent() {
        let dir = "test_write_and_read_state_for_single_torrent";
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
    fn test_read_trackers() {
        let dir = "test_read_trackers";
        fs::create_dir_all(dir).unwrap();
        fs::write(
            Path::new(dir).join(FILENAME),
            r#"{
                "trackers": [
                    "http://tracker1.com",
                    "http://tracker2.com"
                ]
            }"#,
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
    fn test_write_trackers() {
        let dir = "test_write_trackers";
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
