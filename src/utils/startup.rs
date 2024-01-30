use crate::data;
use crate::utils::{benc, meta, worker};
use std::{env, fs, io};
use std::{path::Path, rc::Rc};

pub fn read_metainfo<P: AsRef<Path>>(metainfo_filepath: P) -> io::Result<Rc<meta::Metainfo>> {
    log::info!("Input metainfo file: {}", metainfo_filepath.as_ref().to_string_lossy());
    let file_content = fs::read(metainfo_filepath)?;
    let root_entity = benc::Element::from_bytes(&file_content)?;
    let metainfo = meta::Metainfo::try_from(root_entity)
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "Invalid metainfo file"))?;
    Ok(Rc::new(metainfo))
}

pub fn start_storage(storage: data::Storage) -> (data::StorageClient, worker::simple::Handle) {
    let (client, server) = data::async_storage(storage);

    let handle = worker::without_runtime(
        worker::simple::Config {
            name: "storage".to_string(),
            ..Default::default()
        },
        move || {
            server.run_blocking();
        },
    );

    (client, handle)
}

pub fn start_pwp() -> worker::rt::Handle {
    worker::with_runtime(worker::rt::Config {
        io_enabled: true,
        time_enabled: true,
        name: "pwp".to_string(),
        ..Default::default()
    })
}

pub fn generate_local_peer_id() -> [u8; 20] {
    let mut ret = [0u8; 20];
    let maj = str::parse::<u8>(env!("CARGO_PKG_VERSION_MAJOR")).unwrap_or(b'x');
    let min = str::parse::<u8>(env!("CARGO_PKG_VERSION_MINOR")).unwrap_or(b'x');
    let pat = str::parse::<u8>(env!("CARGO_PKG_VERSION_PATCH")).unwrap_or(b'x');

    let s = format!("-mt0{}{}{}-", maj, min, pat);
    ret[..8].copy_from_slice(s.as_bytes());

    for b in &mut ret[8..] {
        *b = rand::random::<u8>() % (127 - 32) + 32;
    }
    ret
}
