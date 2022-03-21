use crate::benc;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str;

pub struct MetaInfo {
    root: BTreeMap<String, benc::Element>,
    info: BTreeMap<String, benc::Element>,
}

impl TryFrom<benc::Element> for MetaInfo {
    type Error = ();

    fn try_from(e: benc::Element) -> Result<Self, Self::Error> {
        let (root_dictionary, info_element) = match e {
            benc::Element::Dictionary(mut root) => {
                let info_key: benc::Element = benc::Element::from("info");
                let info = root.remove(&info_key);
                (Some(root), info)
            }
            _ => (None, None),
        };

        match (root_dictionary, info_element) {
            (Some(root), Some(benc::Element::Dictionary(info))) => Ok(MetaInfo {
                root: benc::convert_dictionary(root),
                info: benc::convert_dictionary(info),
            }),
            _ => Err(()),
        }
    }
}

impl MetaInfo {
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
        if let Some(benc::Element::List(ref data)) = self.info.get("files") {
            Some(data.iter().filter_map(try_get_length_path_pair))
        } else {
            None
        }
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
