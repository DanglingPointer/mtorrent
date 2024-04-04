use mtorrent::tracker::utils;
use mtorrent::utils::metainfo;
use mtorrent::utils::startup;
use std::collections::HashSet;
use std::path::Path;
use std::{fs, io};

fn get_udp_trackers<T: AsRef<str>>(trackers: impl IntoIterator<Item = T>) -> HashSet<String> {
    trackers
        .into_iter()
        .filter_map(|tracker| utils::get_udp_tracker_addr(&tracker).map(ToString::to_string))
        .collect()
}

fn get_http_trackers<T: AsRef<str>>(trackers: impl IntoIterator<Item = T>) -> HashSet<String> {
    trackers
        .into_iter()
        .filter_map(|tracker| utils::get_http_tracker_addr(&tracker).map(ToString::to_string))
        .collect()
}

#[test]
fn test_read_example_torrent_file() {
    let data = fs::read("tests/assets/example.torrent").unwrap();
    let info = metainfo::Metainfo::new(&data).unwrap();

    let announce = info.announce().unwrap();
    assert_eq!("http://tracker.trackerfix.com:80/announce", announce, "announce: {}", announce);

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
    {
        let mut http_iter = get_http_trackers(utils::trackers_from_metainfo(&info)).into_iter();
        assert_eq!("http://tracker.trackerfix.com:80/announce", http_iter.next().unwrap());
        assert!(http_iter.next().is_none());
    }
    {
        let udp_trackers = get_udp_trackers(utils::trackers_from_metainfo(&info));
        assert_eq!(4, udp_trackers.len());
        assert!(udp_trackers.contains("9.rarbg.me:2720"));
        assert!(udp_trackers.contains("9.rarbg.to:2740"));
        assert!(udp_trackers.contains("tracker.fatkhoala.org:13780"));
        assert!(udp_trackers.contains("tracker.tallpenguin.org:15760"));
    }

    let name = info.name().unwrap();
    assert_eq!(
        "The.Witcher.Nightmare.of.the.Wolf.2021.1080p.WEBRip.x265-RARBG", name,
        "name: {}",
        name
    );

    let piece_length = info.piece_length().unwrap();
    assert_eq!(2_097_152, piece_length, "piece length: {}", piece_length);

    let piece_count = info.pieces().unwrap().count();
    assert_eq!(/* 13360 / 20 */ 668, piece_count);

    let length = info.length();
    assert_eq!(None, length, "length: {:?}", length);

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
    let info = metainfo::Metainfo::new(&data).unwrap();

    let announce = info.announce().unwrap();
    assert_eq!("http://localhost:8000/announce", announce, "announce: {}", announce);

    assert!(info.announce_list().is_none());

    let mut http_iter = get_http_trackers(utils::trackers_from_metainfo(&info)).into_iter();
    assert_eq!("http://localhost:8000/announce", http_iter.next().unwrap());
    assert!(http_iter.next().is_none());
}

fn count_files(dir: impl AsRef<Path>) -> io::Result<usize> {
    let mut count = 0usize;
    if dir.as_ref().is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                count += count_files(&path)?;
            } else {
                count += 1;
            }
        }
    }
    Ok(count)
}

#[test]
fn test_read_metainfo_and_spawn_files() {
    let data = fs::read("tests/assets/example.torrent").unwrap();
    let info = metainfo::Metainfo::new(&data).unwrap();

    let parent_dir = "test_read_metainfo_and_spawn_files_output";
    let filedir = Path::new(parent_dir).join("files");
    let storage = startup::create_content_storage(&info, &filedir).unwrap();

    assert_eq!(info.files().unwrap().count(), count_files(parent_dir).unwrap());

    for (length, path) in info.files().unwrap() {
        let path = Path::new(&filedir).join(path);
        let file = fs::File::open(&path)
            .unwrap_or_else(|_| panic!("{} does not exist", path.to_string_lossy()));
        assert_eq!(length as u64, file.metadata().unwrap().len());
    }

    fs::remove_dir_all(&filedir).unwrap();
    drop(storage);

    assert_eq!(0, count_files(parent_dir).unwrap());

    fs::remove_dir_all(parent_dir).unwrap();
}

#[test]
fn test_read_metainfo_and_spawn_single_file() {
    let data = fs::read("tests/assets/pcap.torrent").unwrap();
    let info = metainfo::Metainfo::new(&data).unwrap();

    let parent_dir = "test_read_metainfo_and_spawn_single_file_output";
    let filedir = Path::new(parent_dir).join("files");
    let storage = startup::create_content_storage(&info, &filedir).unwrap();

    assert_eq!(1, count_files(parent_dir).unwrap());

    let path = Path::new(&filedir).join(info.name().unwrap());
    let file = fs::File::open(&path)
        .unwrap_or_else(|_| panic!("{} does not exist", path.to_string_lossy()));
    assert_eq!(info.length().unwrap() as u64, file.metadata().unwrap().len());

    fs::remove_dir_all(&filedir).unwrap();
    drop(storage);

    assert_eq!(0, count_files(parent_dir).unwrap());

    fs::remove_dir_all(parent_dir).unwrap();
}

#[test]
fn test_read_incomplete_metainfo_file() {
    let data = fs::read("tests/assets/incomplete.torrent").unwrap();
    let info = metainfo::Metainfo::new(&data).unwrap();

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
