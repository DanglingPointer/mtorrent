use crate::utils::benc;
use sha1_smol::Sha1;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::str;

pub struct Metainfo {
    root: BTreeMap<String, benc::Element>,
    info: BTreeMap<String, benc::Element>,
    info_hash: [u8; 20],
    size: usize,
}

impl Hash for Metainfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&self.info_hash)
    }
}

impl Metainfo {
    pub fn new(file_content: &[u8]) -> Option<Self> {
        Self::from_full_metainfo(file_content)
            .or_else(|| Self::from_incomplete_metainfo(file_content))
    }

    fn from_full_metainfo(file_content: &[u8]) -> Option<Self> {
        let root_entity = benc::Element::from_bytes(file_content).ok()?;
        log::debug!("Metainfo file content:\n{root_entity}");
        let (root_dictionary, info_element) = match root_entity {
            benc::Element::Dictionary(mut root) => {
                let info_key: benc::Element = benc::Element::from("info");
                let info = root.remove(&info_key);
                (Some(root), info)
            }
            _ => (None, None),
        };
        let info_element = info_element?;
        let info_hash = Sha1::from(info_element.to_bytes()).digest().bytes();
        match (root_dictionary, info_element) {
            (Some(root), benc::Element::Dictionary(info)) => Some(Metainfo {
                root: benc::convert_dictionary(root),
                info: benc::convert_dictionary(info),
                info_hash,
                size: file_content.len(),
            }),
            _ => None,
        }
    }

    fn from_incomplete_metainfo(file_content: &[u8]) -> Option<Self> {
        let info_element = benc::Element::from_bytes(file_content).ok()?;
        let info_hash = Sha1::from(info_element.to_bytes()).digest().bytes();
        match info_element {
            benc::Element::Dictionary(info) => Some(Metainfo {
                root: Default::default(),
                info: benc::convert_dictionary(info),
                info_hash,
                size: file_content.len(),
            }),
            _ => None,
        }
    }

    pub fn announce(&self) -> Option<&str> {
        if let Some(benc::Element::ByteString(data)) = self.root.get("announce") {
            str::from_utf8(data).ok()
        } else {
            None
        }
    }

    pub fn announce_list(&self) -> Option<impl Iterator<Item = impl Iterator<Item = &str>>> {
        if let Some(benc::Element::List(list)) = self.root.get("announce-list") {
            Some(list.iter().filter_map(try_get_string_iter))
        } else {
            None
        }
    }

    pub fn name(&self) -> Option<&str> {
        if let Some(benc::Element::ByteString(data)) = self.info.get("name") {
            str::from_utf8(data).ok()
        } else {
            None
        }
    }

    pub fn info_hash(&self) -> &[u8; 20] {
        &self.info_hash
    }

    pub fn piece_length(&self) -> Option<usize> {
        if let Some(benc::Element::Integer(data)) = self.info.get("piece length") {
            usize::try_from(*data).ok()
        } else {
            None
        }
    }

    pub fn pieces(&self) -> Option<impl Iterator<Item = &[u8]>> {
        if let Some(benc::Element::ByteString(data)) = self.info.get("pieces") {
            Some(data.chunks_exact(20))
        } else {
            None
        }
    }

    pub fn length(&self) -> Option<usize> {
        if let Some(benc::Element::Integer(data)) = self.info.get("length") {
            usize::try_from(*data).ok()
        } else {
            None
        }
    }

    pub fn files(&self) -> Option<impl Iterator<Item = (usize, PathBuf)> + '_> {
        if let Some(benc::Element::List(data)) = self.info.get("files") {
            Some(data.iter().filter_map(try_get_length_path_pair))
        } else {
            None
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

fn try_get_length_path_pair(e: &benc::Element) -> Option<(usize, PathBuf)> {
    fn path_from_list(list: &Vec<benc::Element>) -> PathBuf {
        let mut ret = PathBuf::new();
        for e in list {
            if let benc::Element::ByteString(data) = e {
                if let Ok(text) = str::from_utf8(data) {
                    ret.push(text);
                }
            }
        }
        ret
    }

    if let benc::Element::Dictionary(dict) = e {
        let length_key = benc::Element::from("length");
        let path_key = benc::Element::from("path");

        match (dict.get(&length_key), dict.get(&path_key)) {
            (Some(benc::Element::Integer(length)), Some(benc::Element::List(list))) => {
                Some((*length as usize, path_from_list(list)))
            }
            _ => None,
        }
    } else {
        None
    }
}

fn try_get_string_iter(e: &benc::Element) -> Option<impl Iterator<Item = &str>> {
    fn str_from_element(e: &benc::Element) -> Option<&str> {
        if let benc::Element::ByteString(data) = e {
            str::from_utf8(data).ok()
        } else {
            None
        }
    }

    if let benc::Element::List(list) = e {
        Some(list.iter().filter_map(str_from_element))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::Path};

    #[test]
    fn test_read_example_torrent_file() {
        let data = fs::read("tests/assets/example.torrent").unwrap();
        let info = Metainfo::new(&data).unwrap();

        let announce = info.announce().unwrap();
        assert_eq!("http://tracker.trackerfix.com:80/announce", announce, "announce: {announce}");

        {
            let mut iter = info.announce_list().unwrap();

            let tier: Vec<&str> = iter.next().unwrap().collect();
            assert_eq!(vec!["http://tracker.trackerfix.com:80/announce"], tier);

            let tier: Vec<&str> = iter.next().unwrap().collect();
            assert_eq!(vec!["udp://9.rarbg.me:2720/announce"], tier);

            let tier: Vec<&str> = iter.next().unwrap().collect();
            assert_eq!(vec!["udp://9.rarbg.to:2740/announce"], tier);

            let tier: Vec<&str> = iter.next().unwrap().collect();
            assert_eq!(vec!["udp://tracker.fatkhoala.org:13780/announce"], tier);

            let tier: Vec<&str> = iter.next().unwrap().collect();
            assert_eq!(vec!["udp://tracker.tallpenguin.org:15760/announce"], tier);

            assert!(iter.next().is_none());
        }

        let name = info.name().unwrap();
        assert_eq!(
            "The.Witcher.Nightmare.of.the.Wolf.2021.1080p.WEBRip.x265-RARBG", name,
            "name: {name}"
        );

        let piece_length = info.piece_length().unwrap();
        assert_eq!(2_097_152, piece_length, "piece length: {piece_length}");

        let piece_count = info.pieces().unwrap().count();
        assert_eq!(/* 13360 / 20 */ 668, piece_count);

        let length = info.length();
        assert_eq!(None, length, "length: {length:?}");

        let total_length: usize = info.files().unwrap().map(|(len, _path)| len).sum();
        assert!(piece_length * piece_count > total_length);
        assert_eq!(1_160_807, total_length % piece_length);

        {
            let mut iter = info.files().unwrap();

            let (length, path) = iter.next().unwrap();
            assert_eq!(30, length);
            assert_eq!("RARBG.txt", path.to_string_lossy());

            let (length, path) = iter.next().unwrap();
            assert_eq!(99, length);
            assert_eq!("RARBG_DO_NOT_MIRROR.exe", path.to_string_lossy());

            let (length, path) = iter.next().unwrap();
            assert_eq!(66667, length);
            assert_eq!(Path::new("Subs/10_French.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(67729, length);
            assert_eq!(Path::new("Subs/11_German.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(98430, length);
            assert_eq!(Path::new("Subs/12_Greek.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(89001, length);
            assert_eq!(Path::new("Subs/13_Hebrew.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(66729, length);
            assert_eq!(Path::new("Subs/14_hrv.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(69251, length);
            assert_eq!(Path::new("Subs/15_Hungarian.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(67897, length);
            assert_eq!(Path::new("Subs/16_Indonesian.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(67035, length);
            assert_eq!(Path::new("Subs/17_Italian.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(68310, length);
            assert_eq!(Path::new("Subs/18_Japanese.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(79479, length);
            assert_eq!(Path::new("Subs/19_Korean.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(67367, length);
            assert_eq!(Path::new("Subs/20_may.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(63337, length);
            assert_eq!(Path::new("Subs/21_Bokmal.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(68715, length);
            assert_eq!(Path::new("Subs/22_Polish.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(67838, length);
            assert_eq!(Path::new("Subs/23_Portuguese.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(69077, length);
            assert_eq!(Path::new("Subs/24_Portuguese.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(70967, length);
            assert_eq!(Path::new("Subs/25_Romanian.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(90311, length);
            assert_eq!(Path::new("Subs/26_Russian.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(67143, length);
            assert_eq!(Path::new("Subs/27_Spanish.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(67068, length);
            assert_eq!(Path::new("Subs/28_Spanish.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(63229, length);
            assert_eq!(Path::new("Subs/29_Swedish.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(97509, length);
            assert_eq!(Path::new("Subs/2_Arabic.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(126859, length);
            assert_eq!(Path::new("Subs/30_Thai.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(69519, length);
            assert_eq!(Path::new("Subs/31_Turkish.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(87216, length);
            assert_eq!(Path::new("Subs/32_ukr.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(86745, length);
            assert_eq!(Path::new("Subs/33_Vietnamese.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(71908, length);
            assert_eq!(Path::new("Subs/3_Chinese.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(71949, length);
            assert_eq!(Path::new("Subs/4_Chinese.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(69054, length);
            assert_eq!(Path::new("Subs/5_Czech.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(64987, length);
            assert_eq!(Path::new("Subs/6_Danish.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(60512, length);
            assert_eq!(Path::new("Subs/7_Dutch.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(81102, length);
            assert_eq!(Path::new("Subs/8_English.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(62658, length);
            assert_eq!(Path::new("Subs/9_Finnish.srt"), path);

            let (length, path) = iter.next().unwrap();
            assert_eq!(1397575464, length);
            assert_eq!(
                "The.Witcher.Nightmare.of.the.Wolf.2021.1080p.WEBRip.x265-RARBG.mp4",
                path.to_string_lossy()
            );

            assert!(iter.next().is_none());
        }
    }

    #[test]
    fn test_read_torrent_file_without_announce_list() {
        let data = fs::read("tests/assets/pcap.torrent").unwrap();
        let info = Metainfo::new(&data).unwrap();

        let announce = info.announce().unwrap();
        assert_eq!("http://localhost:8000/announce", announce, "announce: {announce}");

        assert!(info.announce_list().is_none());
    }

    #[test]
    fn test_read_incomplete_metainfo_file() {
        let data = fs::read("tests/assets/incomplete.torrent").unwrap();
        let info = Metainfo::new(&data).unwrap();

        let expected_info_hash: &[u8; 20] = &[
            209, 68, 239, 216, 66, 44, 231, 247, 155, 34, 252, 154, 11, 67, 23, 64, 149, 2, 72, 89,
        ];
        assert_eq!(expected_info_hash, info.info_hash());
        assert_eq!(1470069860, info.length().unwrap());
        assert_eq!(
            "[ Torrent911.com ] Spider-Man.No.Way.Home.2021.FRENCH.BDRip.XviD-EXTREME.avi",
            info.name().unwrap()
        );
        assert_eq!(1048576, info.piece_length().unwrap());
        assert_eq!(28040 / 20, info.pieces().unwrap().count());
        assert_eq!(data.len(), info.size());

        assert!(info.files().is_none());
        assert!(info.announce().is_none());
        assert!(info.announce_list().is_none());
    }
}
