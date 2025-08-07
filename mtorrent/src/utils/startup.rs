use mtorrent_core::data;
use mtorrent_utils::metainfo;
use std::path::{Path, PathBuf};
use std::{fs, io, iter};

pub fn read_metainfo<P: AsRef<Path>>(metainfo_filepath: P) -> io::Result<metainfo::Metainfo> {
    log::info!("Input metainfo file: {}", metainfo_filepath.as_ref().to_string_lossy());
    let file_content = fs::read(metainfo_filepath)?;
    let metainfo = metainfo::Metainfo::new(&file_content)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Invalid metainfo file"))?;
    Ok(metainfo)
}

pub fn create_content_storage(
    metainfo: &metainfo::Metainfo,
    filedir: impl AsRef<Path>,
) -> io::Result<(data::StorageClient, data::StorageServer)> {
    let total_size = metainfo
        .length()
        .or_else(|| metainfo.files().map(|it| it.map(|(len, _path)| len).sum()))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no total length in metainfo"))?;

    if let Some(files) = metainfo.files() {
        Ok(data::new_async_storage(filedir, files)?)
    } else {
        let name = match metainfo.name() {
            Some(s) => s.to_string(),
            None => String::from_utf8_lossy(metainfo.info_hash()).to_string(),
        };
        Ok(data::new_async_storage(filedir, iter::once((total_size, PathBuf::from(name))))?)
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use mtorrent_utils::metainfo;

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
        let data = fs::read("tests/assets/pcap.torrent").unwrap();
        let info = metainfo::Metainfo::new(&data).unwrap();

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
}
