use std::collections::BTreeMap;
use std::io::Write;
use std::num::ParseIntError;
use std::{io, str};

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

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut dest = Vec::<u8>::new();
        write_element(self, &mut dest).unwrap();
        dest
    }
}

impl From<&str> for Element {
    fn from(text: &str) -> Self {
        Element::ByteString(Vec::<u8>::from(text))
    }
}

impl From<i64> for Element {
    fn from(number: i64) -> Self {
        Element::Integer(number)
    }
}

#[derive(Debug, PartialEq)]
pub enum ParseError {
    EmptySource,
    InvalidPrefix,
    NoIntegerPrefix,
    NoIntegerEnd,
    NoStringDelimeter,
    InvalidStringLength,
    NoListPrefix,
    NoDictionaryPrefix,
    ExternalError(String),
}

impl From<ParseIntError> for ParseError {
    fn from(e: ParseIntError) -> Self {
        ParseError::ExternalError(format!("{}", e))
    }
}

impl From<str::Utf8Error> for ParseError {
    fn from(e: str::Utf8Error) -> Self {
        ParseError::ExternalError(format!("{}", e))
    }
}

impl From<ParseError> for io::Error {
    fn from(e: ParseError) -> Self {
        io::Error::new(io::ErrorKind::Other, format!("{:?}", e))
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

fn write_element(e: &Element, dest: &mut Vec<u8>) -> std::io::Result<()> {
    match e {
        Element::Integer(number) => {
            write!(dest, "{}{}{}", PREFIX_INTEGER as char, number, SUFFIX_COMMON as char)?;
        }
        Element::ByteString(data) => {
            write!(dest, "{}{}", data.len(), DELIMITER_STRING as char)?;
            dest.write_all(data)?;
        }
        Element::List(list) => {
            dest.push(PREFIX_LIST);
            for e in list {
                write_element(e, dest)?;
            }
            dest.push(SUFFIX_COMMON);
        }
        Element::Dictionary(map) => {
            dest.push(PREFIX_DICTIONARY);
            for (key, value) in map {
                write_element(key, dest)?;
                write_element(value, dest)?;
            }
            dest.push(SUFFIX_COMMON);
        }
    };
    Ok(())
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
            assert_eq!("invalid digit found in string", msg);
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
            assert_eq!("invalid utf-8 sequence of 1 bytes from index 0", msg);
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
        assert_eq!(ParseError::InvalidStringLength, parsed.err().unwrap());
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
        assert_eq!(ParseError::EmptySource, parsed.err().unwrap());
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
}
