use std::fs;
use mtorrent::benc;

#[test]
fn test_read_torrent_file_into_dictionary() {
    let data = fs::read("tests/example.torrent").unwrap();
    let entity = benc::Element::from_bytes(&data).unwrap();
    if let benc::Element::Dictionary(dict) = entity {
        assert!(!dict.is_empty());
    } else {
        panic!("Not a dictionary");
    }
}