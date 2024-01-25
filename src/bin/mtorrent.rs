use log::{error, info, LevelFilter};
use mtorrent::ctrl::OperationHandler;
use mtorrent::data;
use mtorrent::tracker::utils;
use mtorrent::utils::dispatch::Dispatcher;
use mtorrent::utils::meta::Metainfo;
use mtorrent::utils::{benc, upnp, worker};
use std::net::SocketAddrV4;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::{env, fs, io, iter};
use tokio::runtime;

fn read_metainfo<P: AsRef<Path>>(metainfo_filepath: P) -> io::Result<Rc<Metainfo>> {
    let file_content = fs::read(metainfo_filepath)?;
    let root_entity = benc::Element::from_bytes(&file_content)?;
    let metainfo = Metainfo::try_from(root_entity)
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "Invalid metainfo file"))?;
    Ok(Rc::new(metainfo))
}

fn generate_local_peer_id() -> [u8; 20] {
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

fn start_storage(storage: data::Storage) -> (data::StorageClient, worker::simple::Handle) {
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

fn start_pwp() -> worker::rt::Handle {
    worker::with_runtime(worker::rt::Config {
        io_enabled: true,
        time_enabled: true,
        name: "pwp".to_string(),
        ..Default::default()
    })
}

fn main() -> io::Result<()> {
    simple_logger::SimpleLogger::new()
        .with_threads(true)
        .with_level(LevelFilter::Off)
        .with_module_level("mtorrent", LevelFilter::Debug)
        .init()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}", e)))?;

    let metainfo = read_metainfo(if let Some(arg) = env::args().nth(1) {
        arg
    } else {
        "tests/example.torrent".to_string()
    })?;
    info!("Successfully consumed metainfo file for '{}'", metainfo.name().unwrap_or("<unknown>"));

    let local_internal_ip = SocketAddrV4::new(utils::get_local_ip()?, 23015);
    info!("Local internal ip address: {}", local_internal_ip);

    let output_dir = if let Some(arg) = env::args().nth(2) {
        arg
    } else {
        "mtorrent_output".to_string()
    };
    let _ = fs::remove_dir_all(&output_dir);
    let filekeeper = if let Some(files) = metainfo.files() {
        data::Storage::new(output_dir, files)?
    } else {
        let name = match metainfo.name() {
            Some(s) => s.to_string(),
            None => String::from_utf8_lossy(metainfo.info_hash()).to_string(),
        };
        data::Storage::new(
            output_dir,
            iter::once((
                metainfo.length().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "No 'length' in metainfo file")
                })?,
                PathBuf::from(name),
            )),
        )?
    };

    let local_peer_id = generate_local_peer_id();
    info!("Local peer id: {}", String::from_utf8_lossy(&local_peer_id));

    let pwp_worker_handle = start_pwp();
    let (storage, _storage_handle) = start_storage(filekeeper);

    runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async move {
            let pwp_rt_handle = pwp_worker_handle.runtime_handle();

            let port_opener_result =
                upnp::PortOpener::new(local_internal_ip, igd::PortMappingProtocol::TCP).await;

            let local_external_ip = match port_opener_result {
                Ok(port_opener) => {
                    let public_ip = port_opener.external_ip();
                    info!("UPnP succeeded, public ip: {}", public_ip);
                    pwp_rt_handle.spawn(async move {
                        if let Err(e) = port_opener.do_continuous_renewal().await {
                            error!("UPnP port renewal failed: {e}");
                        }
                    });
                    public_ip
                }
                Err(e) => {
                    error!("UPnP failed: {}", e);
                    local_internal_ip
                }
            };

            let ctrl = OperationHandler::new(
                metainfo,
                storage,
                local_internal_ip,
                local_external_ip,
                local_peer_id,
                pwp_rt_handle,
            )
            .unwrap_or_else(|e| panic!("Startup failed: {e}"));

            let mut dispatcher = Dispatcher::new(ctrl);
            while dispatcher.dispatch_one().await {}
        });

    Ok(())
}
