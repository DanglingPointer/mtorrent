use log::{error, info, LevelFilter};
use mtorrent::ctrl::OperationHandler;
use mtorrent::data;
use mtorrent::utils::dispatch::Dispatcher;
use mtorrent::utils::ip;
use mtorrent::utils::peer_id::PeerId;
use mtorrent::utils::startup::*;
use mtorrent::utils::upnp;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::rc::Rc;
use std::{env, fs, io, iter};
use tokio::runtime;

fn main() -> io::Result<()> {
    simple_logger::SimpleLogger::new()
        .with_threads(true)
        .with_level(LevelFilter::Off)
        .with_module_level("mtorrent", LevelFilter::Info)
        .init()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}", e)))?;

    let metainfo = read_metainfo(if let Some(arg) = env::args().nth(1) {
        arg
    } else {
        "tests/example.torrent".to_string()
    })?;
    let name = metainfo.name().unwrap_or("<unknown>");
    log::info!("Successfully consumed metainfo file for '{}'", name);

    let local_internal_ip = SocketAddrV4::new(ip::get_local_addr()?, 23015);
    info!("Local internal ip address: {}", local_internal_ip);

    let output_dir = if let Some(arg) = env::args().nth(2) {
        arg
    } else {
        format!("mtorrent_output_{}", name)
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

    let local_peer_id = PeerId::generate_new();
    info!("Local peer id: {}", local_peer_id);

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
                Rc::new(metainfo),
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
