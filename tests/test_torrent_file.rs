use mtorrent::storage;
use mtorrent::{benc, meta};
use std::fs;
use std::path::Path;

#[test]
fn test_read_example_torrent_file() {
    let data = fs::read("tests/example.torrent").unwrap();
    let entity = benc::Element::from_bytes(&data).unwrap();
    if let benc::Element::Dictionary(ref dict) = entity {
        assert!(!dict.is_empty());
    } else {
        panic!("Not a dictionary");
    }

    let info = meta::MetaInfo::try_from(entity).unwrap();

    let announce = info.announce().unwrap();
    assert_eq!(
        "http://tracker.trackerfix.com:80/announce", announce,
        "announce: {}",
        announce
    );

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
        "name: {}",
        name
    );

    let piece_length = info.piece_length().unwrap();
    assert_eq!(2097152, piece_length, "piece length: {}", piece_length);

    let pieces = info.pieces().unwrap();
    assert_eq!(13360 / 20, pieces.count());

    let length = info.length();
    assert_eq!(None, length, "length: {:?}", length);

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
        assert_eq!("Subs/10_French.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(67729, length);
        assert_eq!("Subs/11_German.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(98430, length);
        assert_eq!("Subs/12_Greek.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(89001, length);
        assert_eq!("Subs/13_Hebrew.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(66729, length);
        assert_eq!("Subs/14_hrv.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(69251, length);
        assert_eq!("Subs/15_Hungarian.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(67897, length);
        assert_eq!("Subs/16_Indonesian.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(67035, length);
        assert_eq!("Subs/17_Italian.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(68310, length);
        assert_eq!("Subs/18_Japanese.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(79479, length);
        assert_eq!("Subs/19_Korean.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(67367, length);
        assert_eq!("Subs/20_may.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(63337, length);
        assert_eq!("Subs/21_Bokmal.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(68715, length);
        assert_eq!("Subs/22_Polish.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(67838, length);
        assert_eq!("Subs/23_Portuguese.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(69077, length);
        assert_eq!("Subs/24_Portuguese.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(70967, length);
        assert_eq!("Subs/25_Romanian.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(90311, length);
        assert_eq!("Subs/26_Russian.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(67143, length);
        assert_eq!("Subs/27_Spanish.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(67068, length);
        assert_eq!("Subs/28_Spanish.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(63229, length);
        assert_eq!("Subs/29_Swedish.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(97509, length);
        assert_eq!("Subs/2_Arabic.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(126859, length);
        assert_eq!("Subs/30_Thai.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(69519, length);
        assert_eq!("Subs/31_Turkish.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(87216, length);
        assert_eq!("Subs/32_ukr.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(86745, length);
        assert_eq!("Subs/33_Vietnamese.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(71908, length);
        assert_eq!("Subs/3_Chinese.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(71949, length);
        assert_eq!("Subs/4_Chinese.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(69054, length);
        assert_eq!("Subs/5_Czech.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(64987, length);
        assert_eq!("Subs/6_Danish.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(60512, length);
        assert_eq!("Subs/7_Dutch.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(81102, length);
        assert_eq!("Subs/8_English.srt", path.to_string_lossy());

        let (length, path) = iter.next().unwrap();
        assert_eq!(62658, length);
        assert_eq!("Subs/9_Finnish.srt", path.to_string_lossy());

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
fn test_read_metainfo_and_spawn_files() {
    let data = fs::read("tests/example.torrent").unwrap();
    let entity = benc::Element::from_bytes(&data).unwrap();
    if let benc::Element::Dictionary(ref dict) = entity {
        assert!(!dict.is_empty());
    } else {
        panic!("Not a dictionary");
    }
    let info = meta::MetaInfo::try_from(entity).unwrap();

    let parent_dir = "test_output";
    let filekeeper = storage::FileKeeper::new(parent_dir, info.files().unwrap());

    for (length, path) in info.files().unwrap() {
        let path = Path::new(parent_dir).join(path);
        let file = fs::File::open(&path)
            .expect(format!("{} does not exist", path.to_string_lossy()).as_str());
        assert_eq!(length as u64, file.metadata().unwrap().len());
    }
    fs::remove_dir_all(parent_dir).unwrap();
    drop(filekeeper);
}
