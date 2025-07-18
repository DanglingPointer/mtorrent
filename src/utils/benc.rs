use derive_more::Display;
use std::collections::BTreeMap;
use std::error::Error;
use std::io::Write;
use std::num::ParseIntError;
use std::{fmt, io, str};
use thiserror::Error;

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
pub enum Element {
    Integer(i64),
    ByteString(Vec<u8>),
    List(Vec<Element>),
    Dictionary(BTreeMap<Element, Element>),
}

impl Element {
    pub fn from_bytes(src: &[u8]) -> Result<Element, ParseError> {
        match read_element(src) {
            Ok((entity, _)) => Ok(entity),
            Err(e) => Err(e),
        }
    }

    pub fn from_bytes_with_len(src: &[u8]) -> Result<(Element, usize), ParseError> {
        let (entity, rest) = read_element(src)?;
        Ok((entity, src.len() - rest.len()))
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut dest = Vec::<u8>::new();
        write_element(self, &mut dest);
        dest
    }
}

impl From<&str> for Element {
    fn from(text: &str) -> Self {
        Element::ByteString(Vec::<u8>::from(text))
    }
}

impl From<String> for Element {
    fn from(value: String) -> Self {
        Element::ByteString(value.into_bytes())
    }
}

impl From<i64> for Element {
    fn from(number: i64) -> Self {
        Element::Integer(number)
    }
}

impl fmt::Display for Element {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const INDENT_INCREMENT: usize = 3;
        fn write_entity(
            entity: &Element,
            f: &mut fmt::Formatter<'_>,
            last_indent: usize,
            ignore_first_indent: bool,
        ) -> fmt::Result {
            let first_indent = if ignore_first_indent { 0 } else { last_indent };
            match entity {
                Element::ByteString(data) => match std::str::from_utf8(data) {
                    Ok(text) => write!(f, "{:first_indent$}\"{}\"", ' ', text)?,
                    Err(_) => write!(f, "{:first_indent$}<{} raw bytes>", ' ', data.len())?,
                },
                Element::Integer(number) => write!(f, "{:first_indent$}{}", ' ', number)?,
                Element::List(elements) => {
                    writeln!(f, "{:first_indent$}[", ' ')?;
                    for e in elements {
                        write_entity(e, f, last_indent + INDENT_INCREMENT, false)?;
                        writeln!(f, ",")?;
                    }
                    write!(f, "{:last_indent$}]", ' ')?;
                }
                Element::Dictionary(map) => {
                    writeln!(f, "{:first_indent$}{{", ' ')?;
                    for (key, val) in map {
                        write_entity(key, f, last_indent + INDENT_INCREMENT, false)?;
                        write!(f, ":")?;
                        write_entity(val, f, last_indent + INDENT_INCREMENT, true)?;
                        writeln!(f, ",")?;
                    }
                    write!(f, "{:last_indent$}}}", ' ')?;
                }
            }
            Ok(())
        }
        write_entity(self, f, INDENT_INCREMENT, false)
    }
}

#[derive(Debug, Error, Display)]
pub enum ParseError {
    EmptySource,
    InvalidPrefix,
    NoIntegerPrefix,
    NoIntegerEnd,
    NoStringDelimeter,
    InvalidStringLength,
    NoListPrefix,
    NoDictionaryPrefix,
    #[display("{_0}")]
    ExternalError(#[source] Box<dyn Error + Send + Sync>),
}

impl From<ParseIntError> for ParseError {
    fn from(e: ParseIntError) -> Self {
        ParseError::ExternalError(Box::new(e))
    }
}

impl From<str::Utf8Error> for ParseError {
    fn from(e: str::Utf8Error) -> Self {
        ParseError::ExternalError(Box::new(e))
    }
}

impl From<ParseError> for io::Error {
    fn from(e: ParseError) -> Self {
        io::Error::other(e)
    }
}

pub fn convert_dictionary(src: BTreeMap<Element, Element>) -> BTreeMap<String, Element> {
    fn to_string_key(pair: (Element, Element)) -> Option<(String, Element)> {
        let (key, value) = pair;
        match key {
            Element::ByteString(data) => {
                if let Ok(text) = String::from_utf8(data) {
                    Some((text, value))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    src.into_iter().filter_map(to_string_key).collect()
}

const DELIMITER_STRING: u8 = b':';
const PREFIX_INTEGER: u8 = b'i';
const PREFIX_LIST: u8 = b'l';
const PREFIX_DICTIONARY: u8 = b'd';
const SUFFIX_COMMON: u8 = b'e';

fn write_element(e: &Element, dest: &mut Vec<u8>) {
    match e {
        Element::Integer(number) => {
            let _ = write!(dest, "{}{}{}", PREFIX_INTEGER as char, number, SUFFIX_COMMON as char);
        }
        Element::ByteString(data) => {
            let _ = write!(dest, "{}{}", data.len(), DELIMITER_STRING as char);
            dest.extend_from_slice(data);
        }
        Element::List(list) => {
            dest.push(PREFIX_LIST);
            for e in list {
                write_element(e, dest);
            }
            dest.push(SUFFIX_COMMON);
        }
        Element::Dictionary(map) => {
            dest.push(PREFIX_DICTIONARY);
            for (key, value) in map {
                write_element(key, dest);
                write_element(value, dest);
            }
            dest.push(SUFFIX_COMMON);
        }
    };
}

fn read_element(src: &[u8]) -> Result<(Element, &[u8]), ParseError> {
    let first_byte = src.first().ok_or(ParseError::EmptySource)?;
    match *first_byte {
        b'0'..=b'9' => read_string(src),
        PREFIX_INTEGER => read_integer(src),
        PREFIX_LIST => read_list(src),
        PREFIX_DICTIONARY => read_dictionary(src),
        _ => Err(ParseError::InvalidPrefix),
    }
}

fn split_once(src: &[u8], delimeter: u8) -> Option<(&[u8], &[u8])> {
    let opt_index = src.iter().position(|b| *b == delimeter);
    match opt_index {
        Some(index) => {
            let lhs = unsafe { src.get_unchecked(..index) };
            let rhs = unsafe { src.get_unchecked(index + 1..) };
            Some((lhs, rhs))
        }
        None => None,
    }
}

fn read_integer(src: &[u8]) -> Result<(Element, &[u8]), ParseError> {
    let rest = src.strip_prefix(&[PREFIX_INTEGER]).ok_or(ParseError::NoIntegerPrefix)?;

    let (number_data, rest) = split_once(rest, SUFFIX_COMMON).ok_or(ParseError::NoIntegerEnd)?;
    let number_text = str::from_utf8(number_data)?;
    let number = number_text.parse::<i64>()?;

    Ok((Element::Integer(number), rest))
}

fn read_string(src: &[u8]) -> Result<(Element, &[u8]), ParseError> {
    let (size_data, rest) =
        split_once(src, DELIMITER_STRING).ok_or(ParseError::NoStringDelimeter)?;
    let size_text = str::from_utf8(size_data)?;
    let size = size_text.parse::<usize>()?;

    let data = rest.get(..size).ok_or(ParseError::InvalidStringLength)?;
    let rest = unsafe { rest.get_unchecked(size..) };

    Ok((Element::ByteString(Vec::from(data)), rest))
}

fn read_list(src: &[u8]) -> Result<(Element, &[u8]), ParseError> {
    let mut rest = src.strip_prefix(&[PREFIX_LIST]).ok_or(ParseError::NoListPrefix)?;

    let mut list = Vec::new();
    while !rest.starts_with(&[SUFFIX_COMMON]) {
        let (entity, new_rest) = read_element(rest)?;
        list.push(entity);
        rest = new_rest;
    }
    Ok((Element::List(list), unsafe { rest.get_unchecked(1..) }))
}

fn read_dictionary(src: &[u8]) -> Result<(Element, &[u8]), ParseError> {
    let mut rest = src.strip_prefix(&[PREFIX_DICTIONARY]).ok_or(ParseError::NoDictionaryPrefix)?;

    let mut map = BTreeMap::new();
    while !rest.starts_with(&[SUFFIX_COMMON]) {
        let (key, new_rest) = read_string(rest)?;
        rest = new_rest;

        let (value, new_rest) = read_element(rest)?;
        rest = new_rest;

        map.insert(key, value);
    }
    Ok((Element::Dictionary(map), unsafe { rest.get_unchecked(1..) }))
}

#[allow(clippy::get_first)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_and_encode_negative_integer() {
        let input = b"i-42e";

        let parsed = read_integer(input);
        assert!(parsed.is_ok(), "Error: {:?}", parsed.err());

        let (entity, rest) = parsed.unwrap();
        assert_eq!(Element::Integer(-42), entity);
        assert!(rest.is_empty());

        assert_eq!(input, entity.to_bytes().as_slice());
    }

    #[test]
    fn test_decode_invalid_integer() {
        let input = b"i0_0e";

        let parsed = read_integer(input);
        assert!(parsed.is_err());

        if let ParseError::ExternalError(msg) = parsed.err().unwrap() {
            assert_eq!("invalid digit found in string", format!("{msg}"));
        } else {
            panic!("Wrong error type");
        }
    }

    #[test]
    fn test_decode_non_utf8_integer() {
        let input = [PREFIX_INTEGER, 200, 201, 202, SUFFIX_COMMON];

        let parsed = read_integer(&input);
        assert!(parsed.is_err());

        if let ParseError::ExternalError(msg) = parsed.err().unwrap() {
            assert_eq!("invalid utf-8 sequence of 1 bytes from index 0", format!("{msg}"));
        } else {
            panic!("Wrong error type");
        }
    }

    #[test]
    fn test_decode_and_encode_simple_string() {
        let input = b"15:A simple string";

        let parsed = read_string(input);
        assert!(parsed.is_ok(), "Error: {:?}", parsed.err());

        let (entity, rest) = parsed.unwrap();
        assert_eq!(Element::ByteString(Vec::from(b"A simple string".as_slice())), entity);
        assert!(rest.is_empty());

        assert_eq!(input, entity.to_bytes().as_slice());
    }

    #[test]
    fn test_decode_and_encode_non_ascii_string() {
        let input = "22:Добрый день!";

        let parsed = read_string(input.as_bytes());
        assert!(parsed.is_ok(), "Error: {:?}", parsed.err());

        let (entity, rest) = parsed.unwrap();
        assert_eq!(Element::ByteString(Vec::from("Добрый день!".as_bytes())), entity);
        assert!(rest.is_empty());

        assert_eq!(input.as_bytes(), entity.to_bytes().as_slice());
    }

    #[test]
    fn test_decode_and_encode_binary_string() {
        let input = [b'4', b':', 0xf1, 0xf2, 0xf3, 0xf4];

        let parsed = read_string(&input);
        assert!(parsed.is_ok(), "Error: {:?}", parsed.err());

        let (entity, rest) = parsed.unwrap();
        assert_eq!(Element::ByteString(Vec::from([0xf1, 0xf2, 0xf3, 0xf4].as_slice())), entity);
        assert!(rest.is_empty());

        assert_eq!(&input, entity.to_bytes().as_slice());
    }

    #[test]
    fn test_decode_string_with_invalid_length() {
        let input = [b'9', b'2', b':', 0xf1, 0xf2, 0xf3, 0xf4];

        let parsed = read_string(&input);
        assert!(parsed.is_err());
        assert!(matches!(parsed.err().unwrap(), ParseError::InvalidStringLength));
    }

    #[test]
    fn test_decode_and_encode_simple_list() {
        let input = "li-42ei42e15:A simple stringe";

        let parsed = read_list(input.as_bytes());
        assert!(parsed.is_ok(), "Error: {:?}", parsed.err());

        let (entity, rest) = parsed.unwrap();
        assert!(rest.is_empty());

        if let Element::List(ref list) = entity {
            assert_eq!(3, list.len());
            assert_eq!(Element::Integer(-42), *list.get(0).unwrap());
            assert_eq!(Element::Integer(42), *list.get(1).unwrap());
            assert_eq!(
                Element::ByteString(Vec::from("A simple string".as_bytes())),
                *list.get(2).unwrap()
            );
        } else {
            panic!("Not a list");
        }

        assert_eq!(input.as_bytes(), entity.to_bytes().as_slice());
    }

    #[test]
    fn test_decode_list_without_end_postfix() {
        let input = "li-42ei42e15:A simple string";

        let parsed = read_list(input.as_bytes());
        assert!(parsed.is_err());
        assert!(matches!(parsed.err().unwrap(), ParseError::EmptySource));
    }

    #[test]
    fn test_decode_and_encode_list_with_binary_string() {
        let input = {
            let start = b"li42e";
            let end = [b'4', b':', 0xf1, 0xf2, 0xf3, 0xf4, b'e'];
            let mut buf = Vec::<u8>::new();
            buf.write_all(start).unwrap();
            buf.write_all(&end).unwrap();
            buf
        };

        let parsed = read_list(&input);
        assert!(parsed.is_ok(), "Error: {:?}", parsed.err());

        let (entity, rest) = parsed.unwrap();
        assert!(rest.is_empty());

        if let Element::List(ref list) = entity {
            assert_eq!(2, list.len());
            assert_eq!(Element::Integer(42), *list.get(0).unwrap());
            assert_eq!(
                Element::ByteString(Vec::from([0xf1, 0xf2, 0xf3, 0xf4].as_slice())),
                *list.get(1).unwrap()
            );
        } else {
            panic!("Not a list");
        }

        assert_eq!(&input, entity.to_bytes().as_slice());
    }

    #[test]
    fn test_decode_and_encode_simple_dictionary() {
        let input = "d8:announce41:http://tracker.trackerfix.com:80/announcee";

        let parsed = read_dictionary(input.as_bytes());
        assert!(parsed.is_ok(), "Error: {:?}", parsed.err());

        let (entity, rest) = parsed.unwrap();
        assert!(rest.is_empty());
        if let Element::Dictionary(ref map) = entity {
            assert_eq!(1, map.len());
            let (key, value) = map.iter().next().unwrap();
            assert_eq!(Element::ByteString(Vec::from("announce".as_bytes())), *key);
            assert_eq!(
                Element::ByteString(Vec::from(
                    "http://tracker.trackerfix.com:80/announce".as_bytes()
                )),
                *value
            );
        } else {
            panic!("Not a dictionary");
        }

        assert_eq!(input.as_bytes(), entity.to_bytes().as_slice());
    }

    #[test]
    fn test_decode_dictionary_with_appended_data() {
        let input = "d8:msg_typei1e5:piecei0e10:total_sizei34256eexxxxxxxx";

        let parsed = Element::from_bytes(input.as_bytes()).unwrap();

        if let Element::Dictionary(map) = parsed {
            let mut it = map.into_iter();

            let (key, value) = it.next().unwrap();
            assert_eq!(Element::from("msg_type"), key);
            assert_eq!(Element::Integer(1), value);

            let (key, value) = it.next().unwrap();
            assert_eq!(Element::from("piece"), key);
            assert_eq!(Element::Integer(0), value);

            let (key, value) = it.next().unwrap();
            assert_eq!(Element::from("total_size"), key);
            assert_eq!(Element::Integer(34256), value);

            assert!(it.next().is_none());
        } else {
            panic!("Not a dictionary");
        }
    }

    #[test]
    fn test_decode_and_encode_real_dictionary() {
        let input = "d8:announce41:http://tracker.trackerfix.com:80/announce13:announce-listll41:http://tracker.trackerfix.com:80/announceel30:udp://9.rarbg.me:2720/announceel30:udp://9.rarbg.to:2740/announceel42:udp://tracker.fatkhoala.org:13780/announceel44:udp://tracker.tallpenguin.org:15760/announceee7:comment40:Torrent downloaded from https://rarbg.to10:created by5:RARBG13:creation datei1629718368e4:infod5:filesld6:lengthi30e4:pathl9:RARBG.txteed6:lengthi99e4:pathl23:RARBG_DO_NOT_MIRROR.exeeed6:lengthi66667e4:pathl4:Subs13:10_French.srteed6:lengthi67729e4:pathl4:Subs13:11_German.srteed6:lengthi98430e4:pathl4:Subs12:12_Greek.srteed6:lengthi89001e4:pathl4:Subs13:13_Hebrew.srteed6:lengthi66729e4:pathl4:Subs10:14_hrv.srteed6:lengthi69251e4:pathl4:Subs16:15_Hungarian.srteed6:lengthi67897e4:pathl4:Subs17:16_Indonesian.srteed6:lengthi67035e4:pathl4:Subs14:17_Italian.srteed6:lengthi68310e4:pathl4:Subs15:18_Japanese.srteed6:lengthi79479e4:pathl4:Subs13:19_Korean.srteed6:lengthi67367e4:pathl4:Subs10:20_may.srteed6:lengthi63337e4:pathl4:Subs13:21_Bokmal.srteed6:lengthi68715e4:pathl4:Subs13:22_Polish.srteed6:lengthi67838e4:pathl4:Subs17:23_Portuguese.srteed6:lengthi69077e4:pathl4:Subs17:24_Portuguese.srteed6:lengthi70967e4:pathl4:Subs15:25_Romanian.srteed6:lengthi90311e4:pathl4:Subs14:26_Russian.srteed6:lengthi67143e4:pathl4:Subs14:27_Spanish.srteed6:lengthi67068e4:pathl4:Subs14:28_Spanish.srteed6:lengthi63229e4:pathl4:Subs14:29_Swedish.srteed6:lengthi97509e4:pathl4:Subs12:2_Arabic.srteed6:lengthi126859e4:pathl4:Subs11:30_Thai.srteed6:lengthi69519e4:pathl4:Subs14:31_Turkish.srteed6:lengthi87216e4:pathl4:Subs10:32_ukr.srteed6:lengthi86745e4:pathl4:Subs17:33_Vietnamese.srteed6:lengthi71908e4:pathl4:Subs13:3_Chinese.srteed6:lengthi71949e4:pathl4:Subs13:4_Chinese.srteed6:lengthi69054e4:pathl4:Subs11:5_Czech.srteed6:lengthi64987e4:pathl4:Subs12:6_Danish.srteed6:lengthi60512e4:pathl4:Subs11:7_Dutch.srteed6:lengthi81102e4:pathl4:Subs13:8_English.srteed6:lengthi62658e4:pathl4:Subs13:9_Finnish.srteed6:lengthi1397575464e4:pathl66:The.Witcher.Nightmare.of.the.Wolf.2021.1080p.WEBRip.x265-RARBG.mp4eee4:name62:The.Witcher.Nightmare.of.the.Wolf.2021.1080p.WEBRip.x265-RARBG12:piece lengthi2097152eee";

        let parsed = read_dictionary(input.as_bytes());
        assert!(parsed.is_ok(), "Error: {:?}", parsed.err());

        let entity = parsed.unwrap();
        match entity.0 {
            Element::Dictionary(_) => (),
            _ => panic!(),
        };

        assert_eq!(input.as_bytes(), entity.0.to_bytes().as_slice());
    }

    #[test]
    fn test_format_bencode() {
        #[rustfmt::skip]
        let expected = 
r#"   {
      "announce": "http://tracker.trackerfix.com:80/announce",
      "announce-list": [
         [
            "http://tracker.trackerfix.com:80/announce",
         ],
         [
            "udp://9.rarbg.me:2720/announce",
         ],
         [
            "udp://9.rarbg.to:2740/announce",
         ],
         [
            "udp://tracker.fatkhoala.org:13780/announce",
         ],
         [
            "udp://tracker.tallpenguin.org:15760/announce",
         ],
      ],
      "comment": "Torrent downloaded from https://rarbg.to",
      "created by": "RARBG",
      "creation date": 1629718368,
      "info": {
         "files": [
            {
               "length": 30,
               "path": [
                  "RARBG.txt",
               ],
            },
            {
               "length": 99,
               "path": [
                  "RARBG_DO_NOT_MIRROR.exe",
               ],
            },
            {
               "length": 66667,
               "path": [
                  "Subs",
                  "10_French.srt",
               ],
            },
            {
               "length": 67729,
               "path": [
                  "Subs",
                  "11_German.srt",
               ],
            },
            {
               "length": 98430,
               "path": [
                  "Subs",
                  "12_Greek.srt",
               ],
            },
            {
               "length": 89001,
               "path": [
                  "Subs",
                  "13_Hebrew.srt",
               ],
            },
            {
               "length": 66729,
               "path": [
                  "Subs",
                  "14_hrv.srt",
               ],
            },
            {
               "length": 69251,
               "path": [
                  "Subs",
                  "15_Hungarian.srt",
               ],
            },
            {
               "length": 67897,
               "path": [
                  "Subs",
                  "16_Indonesian.srt",
               ],
            },
            {
               "length": 67035,
               "path": [
                  "Subs",
                  "17_Italian.srt",
               ],
            },
            {
               "length": 68310,
               "path": [
                  "Subs",
                  "18_Japanese.srt",
               ],
            },
            {
               "length": 79479,
               "path": [
                  "Subs",
                  "19_Korean.srt",
               ],
            },
            {
               "length": 67367,
               "path": [
                  "Subs",
                  "20_may.srt",
               ],
            },
            {
               "length": 63337,
               "path": [
                  "Subs",
                  "21_Bokmal.srt",
               ],
            },
            {
               "length": 68715,
               "path": [
                  "Subs",
                  "22_Polish.srt",
               ],
            },
            {
               "length": 67838,
               "path": [
                  "Subs",
                  "23_Portuguese.srt",
               ],
            },
            {
               "length": 69077,
               "path": [
                  "Subs",
                  "24_Portuguese.srt",
               ],
            },
            {
               "length": 70967,
               "path": [
                  "Subs",
                  "25_Romanian.srt",
               ],
            },
            {
               "length": 90311,
               "path": [
                  "Subs",
                  "26_Russian.srt",
               ],
            },
            {
               "length": 67143,
               "path": [
                  "Subs",
                  "27_Spanish.srt",
               ],
            },
            {
               "length": 67068,
               "path": [
                  "Subs",
                  "28_Spanish.srt",
               ],
            },
            {
               "length": 63229,
               "path": [
                  "Subs",
                  "29_Swedish.srt",
               ],
            },
            {
               "length": 97509,
               "path": [
                  "Subs",
                  "2_Arabic.srt",
               ],
            },
            {
               "length": 126859,
               "path": [
                  "Subs",
                  "30_Thai.srt",
               ],
            },
            {
               "length": 69519,
               "path": [
                  "Subs",
                  "31_Turkish.srt",
               ],
            },
            {
               "length": 87216,
               "path": [
                  "Subs",
                  "32_ukr.srt",
               ],
            },
            {
               "length": 86745,
               "path": [
                  "Subs",
                  "33_Vietnamese.srt",
               ],
            },
            {
               "length": 71908,
               "path": [
                  "Subs",
                  "3_Chinese.srt",
               ],
            },
            {
               "length": 71949,
               "path": [
                  "Subs",
                  "4_Chinese.srt",
               ],
            },
            {
               "length": 69054,
               "path": [
                  "Subs",
                  "5_Czech.srt",
               ],
            },
            {
               "length": 64987,
               "path": [
                  "Subs",
                  "6_Danish.srt",
               ],
            },
            {
               "length": 60512,
               "path": [
                  "Subs",
                  "7_Dutch.srt",
               ],
            },
            {
               "length": 81102,
               "path": [
                  "Subs",
                  "8_English.srt",
               ],
            },
            {
               "length": 62658,
               "path": [
                  "Subs",
                  "9_Finnish.srt",
               ],
            },
            {
               "length": 1397575464,
               "path": [
                  "The.Witcher.Nightmare.of.the.Wolf.2021.1080p.WEBRip.x265-RARBG.mp4",
               ],
            },
         ],
         "name": "The.Witcher.Nightmare.of.the.Wolf.2021.1080p.WEBRip.x265-RARBG",
         "piece length": 2097152,
      },
   }"#;

        let input = "d8:announce41:http://tracker.trackerfix.com:80/announce13:announce-listll41:http://tracker.trackerfix.com:80/announceel30:udp://9.rarbg.me:2720/announceel30:udp://9.rarbg.to:2740/announceel42:udp://tracker.fatkhoala.org:13780/announceel44:udp://tracker.tallpenguin.org:15760/announceee7:comment40:Torrent downloaded from https://rarbg.to10:created by5:RARBG13:creation datei1629718368e4:infod5:filesld6:lengthi30e4:pathl9:RARBG.txteed6:lengthi99e4:pathl23:RARBG_DO_NOT_MIRROR.exeeed6:lengthi66667e4:pathl4:Subs13:10_French.srteed6:lengthi67729e4:pathl4:Subs13:11_German.srteed6:lengthi98430e4:pathl4:Subs12:12_Greek.srteed6:lengthi89001e4:pathl4:Subs13:13_Hebrew.srteed6:lengthi66729e4:pathl4:Subs10:14_hrv.srteed6:lengthi69251e4:pathl4:Subs16:15_Hungarian.srteed6:lengthi67897e4:pathl4:Subs17:16_Indonesian.srteed6:lengthi67035e4:pathl4:Subs14:17_Italian.srteed6:lengthi68310e4:pathl4:Subs15:18_Japanese.srteed6:lengthi79479e4:pathl4:Subs13:19_Korean.srteed6:lengthi67367e4:pathl4:Subs10:20_may.srteed6:lengthi63337e4:pathl4:Subs13:21_Bokmal.srteed6:lengthi68715e4:pathl4:Subs13:22_Polish.srteed6:lengthi67838e4:pathl4:Subs17:23_Portuguese.srteed6:lengthi69077e4:pathl4:Subs17:24_Portuguese.srteed6:lengthi70967e4:pathl4:Subs15:25_Romanian.srteed6:lengthi90311e4:pathl4:Subs14:26_Russian.srteed6:lengthi67143e4:pathl4:Subs14:27_Spanish.srteed6:lengthi67068e4:pathl4:Subs14:28_Spanish.srteed6:lengthi63229e4:pathl4:Subs14:29_Swedish.srteed6:lengthi97509e4:pathl4:Subs12:2_Arabic.srteed6:lengthi126859e4:pathl4:Subs11:30_Thai.srteed6:lengthi69519e4:pathl4:Subs14:31_Turkish.srteed6:lengthi87216e4:pathl4:Subs10:32_ukr.srteed6:lengthi86745e4:pathl4:Subs17:33_Vietnamese.srteed6:lengthi71908e4:pathl4:Subs13:3_Chinese.srteed6:lengthi71949e4:pathl4:Subs13:4_Chinese.srteed6:lengthi69054e4:pathl4:Subs11:5_Czech.srteed6:lengthi64987e4:pathl4:Subs12:6_Danish.srteed6:lengthi60512e4:pathl4:Subs11:7_Dutch.srteed6:lengthi81102e4:pathl4:Subs13:8_English.srteed6:lengthi62658e4:pathl4:Subs13:9_Finnish.srteed6:lengthi1397575464e4:pathl66:The.Witcher.Nightmare.of.the.Wolf.2021.1080p.WEBRip.x265-RARBG.mp4eee4:name62:The.Witcher.Nightmare.of.the.Wolf.2021.1080p.WEBRip.x265-RARBG12:piece lengthi2097152eee";

        let parsed = read_dictionary(input.as_bytes());
        assert!(parsed.is_ok(), "Error: {:?}", parsed.err());

        let entity = parsed.unwrap().0;
        assert_eq!(expected, format!("{entity}"), "\n{entity}\n");
    }
}
