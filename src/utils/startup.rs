use crate::data;
use crate::utils::{benc, meta, worker};
use std::{fs, io};
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
