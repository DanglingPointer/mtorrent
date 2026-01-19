use mtorrent_core::{data, input};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::{fs, io, iter};

pub(crate) fn read_metainfo<P: AsRef<Path>>(metainfo_filepath: P) -> io::Result<input::Metainfo> {
    log::info!("Input metainfo file: {}", metainfo_filepath.as_ref().to_string_lossy());
    let metainfo = input::Metainfo::from_file(metainfo_filepath)?;
    Ok(metainfo)
}

#[doc(hidden)]
pub fn create_content_storage(
    metainfo: &input::Metainfo,
    content_parent_dir: impl AsRef<Path>,
) -> io::Result<(data::StorageClient, data::StorageServer)> {
    let total_size = metainfo
        .length()
        .or_else(|| metainfo.files().map(|it| it.map(|(len, _path)| len).sum()))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no total length in metainfo"))?;

    if let Some(files) = metainfo.files() {
        Ok(data::new_async_storage(content_parent_dir, files)?)
    } else {
        let name = match metainfo.name() {
            Some(s) => s.to_string(),
            None => String::from_utf8_lossy(metainfo.info_hash()).to_string(),
        };
        Ok(data::new_async_storage(
            content_parent_dir,
            iter::once((total_size, PathBuf::from(name))),
        )?)
    }
}

#[doc(hidden)]
pub fn create_metainfo_storage(
    metainfo_path: impl AsRef<Path>,
) -> io::Result<(data::StorageClient, data::StorageServer)> {
    let metainfo_filelen = fs::metadata(&metainfo_path)?.len() as usize;
    let parent = metainfo_path.as_ref().parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::Unsupported, "metainfo file has no parent directory")
    })?;
    let filename = metainfo_path.as_ref().file_name().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "metainfo file has no filename")
    })?;
    Ok(data::new_async_storage(parent, iter::once((metainfo_filelen, PathBuf::from(filename))))?)
}

/// Name of the torrent being downloaded. If `uri` is a magnet link, the name field
/// in the magnet link is used. If `uri` is a path to a `.torrent` file, the filename
/// without extension is used. If no name can be determined, `None` is returned.
pub fn get_torrent_name(uri: impl AsRef<str>) -> Option<String> {
    if let Ok(magnet) = input::MagnetLink::from_str(uri.as_ref()) {
        magnet.name().map(ToOwned::to_owned)
    } else {
        Path::new(uri.as_ref()).file_stem().map(|s| s.display().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let info =
            input::Metainfo::from_file("../mtorrent-cli/tests/assets/example.torrent").unwrap();

        let parent_dir = "test_read_metainfo_and_spawn_files_output";
        let filedir = Path::new(parent_dir).join("files");
        let storage = create_content_storage(&info, &filedir).unwrap();

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
        let info = input::Metainfo::from_file(
            "../mtorrent-cli/tests/assets/torrents_with_tracker/pcap.torrent",
        )
        .unwrap();

        let parent_dir = "test_read_metainfo_and_spawn_single_file_output";
        let filedir = Path::new(parent_dir).join("files");
        let storage = create_content_storage(&info, &filedir).unwrap();

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
    fn test_get_torrent_name() {
        let uri = "magnet:?xt=urn:btih:1EBD3DBFBB25C1333F51C99C7EE670FC2A1727C9&dn=Dune.Part.Two.2024.1080p.HD-TS.X264-EMIN3M%5BTGx%5D&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.tiny-vps.com%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Fexplodie.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.cyberia.is%3A6969%2Fannounce&tr=udp%3A%2F%2Fipv4.tracker.harry.lu%3A80%2Fannounce&tr=udp%3A%2F%2Fp4p.arenabg.com%3A1337%2Fannounce&tr=udp%3A%2F%2Ftracker.birkenwald.de%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.moeking.me%3A6969%2Fannounce&tr=udp%3A%2F%2Fopentor.org%3A2710%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fuploads.gamecoast.net%3A6969%2Fannounce&tr=https%3A%2F%2Ftracker.foreverpirates.co%3A443%2Fannounce&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce&tr=http%3A%2F%2Ftracker.openbittorrent.com%3A80%2Fannounce&tr=udp%3A%2F%2Fopentracker.i2p.rocks%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.internetwarriors.net%3A1337%2Fannounce&tr=udp%3A%2F%2Ftracker.leechers-paradise.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fcoppersurfer.tk%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.zer0day.to%3A1337%2Fannounce";
        assert_eq!(
            get_torrent_name(uri),
            Some("Dune.Part.Two.2024.1080p.HD-TS.X264-EMIN3M[TGx]".to_owned())
        );

        let uri = "../mtorrent-cli/tests/assets/screenshots_bad_tracker.torrent";
        assert_eq!(get_torrent_name(uri), Some("screenshots_bad_tracker".to_owned()));
    }
}
