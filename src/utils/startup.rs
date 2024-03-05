use crate::data;
use crate::utils::{meta, worker};
use std::path::{Path, PathBuf};
use std::{fs, io, iter};

pub fn read_metainfo<P: AsRef<Path>>(metainfo_filepath: P) -> io::Result<meta::Metainfo> {
    log::info!("Input metainfo file: {}", metainfo_filepath.as_ref().to_string_lossy());
    let file_content = fs::read(metainfo_filepath)?;
    let metainfo = meta::Metainfo::new(&file_content)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Invalid metainfo file"))?;
    Ok(metainfo)
}

pub fn create_content_storage(
    metainfo: &meta::Metainfo,
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

pub fn start_pwp() -> worker::rt::Handle {
    worker::with_runtime(worker::rt::Config {
        io_enabled: true,
        time_enabled: true,
        name: "pwp".to_string(),
        ..Default::default()
    })
}
