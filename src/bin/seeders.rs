use futures::future;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;
use std::{collections::HashSet, fs, io, net::SocketAddr, path::Path, rc::Rc};
use std::{env, iter};

use mtorrent::utils::{benc, meta::Metainfo, worker};
use mtorrent::{data, pwp};
use tokio::runtime;
use tokio::{net::TcpListener, sync::mpsc, task};

async fn run_one_seeder(
    local_addr: SocketAddr,
    storage: data::StorageClient,
    info: Rc<data::PieceInfo>,
) -> io::Result<()> {
    log::info!("Starting seeder on {local_addr}");
    let listener = TcpListener::bind(local_addr).await?;
    let (stream, remote_addr) = listener.accept().await?;
    log::info!("{} accepted connection from {}", local_addr, remote_addr);
    let (download_chans, upload_chans, runner) =
        pwp::channels_from_incoming(&[0u8; 20], None, stream).await?;
    task::spawn_local(async move {
        let _ = runner.run().await;
    });
    task::spawn_local(async move {
        let pwp::DownloadChannels(tx, mut rx) = download_chans;
        while let Ok(msg) = rx.receive_message().await {
            log::debug!("{local_addr} received {msg}");
        }
        drop(tx);
    });
    let bitfield = pwp::Bitfield::repeat(true, info.piece_count());
    let pwp::UploadChannels(mut tx, mut rx) = upload_chans;
    tx.send_message(pwp::UploaderMessage::Bitfield(bitfield)).await?;
    tx.send_message(pwp::UploaderMessage::Unchoke).await?;

    let (request_sender, mut request_receiver) = mpsc::unbounded_channel::<pwp::BlockInfo>();
    let (cancel_sender, mut cancel_receiver) = mpsc::unbounded_channel::<pwp::BlockInfo>();

    task::spawn_local(async move {
        let mut cancelled_blocks = HashSet::<pwp::BlockInfo>::new();
        loop {
            if let Ok(cancelled_block) = cancel_receiver.try_recv() {
                cancelled_blocks.insert(cancelled_block);
            }
            if let Some(requested_block) = request_receiver.recv().await {
                if !cancelled_blocks.remove(&requested_block) {
                    let global_offset = info
                        .global_offset(
                            requested_block.piece_index,
                            requested_block.in_piece_offset,
                            requested_block.block_length,
                        )
                        .unwrap_or_else(|_| {
                            panic!("{local_addr} received invalid request {requested_block}")
                        });
                    let data = storage
                        .read_block(global_offset, requested_block.block_length)
                        .await
                        .unwrap_or_else(|e| {
                            panic!(
                                "Couldn't read {} bytes at offset {}: {}",
                                requested_block.block_length, global_offset, e
                            )
                        });
                    if tx
                        .send_message(pwp::UploaderMessage::Block(requested_block, data))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            } else {
                return;
            }
        }
    });

    while let Ok(msg) = rx.receive_message().await {
        match msg {
            pwp::DownloaderMessage::Request(block) => request_sender.send(block).unwrap(),
            pwp::DownloaderMessage::Cancel(block) => cancel_sender.send(block).unwrap(),
            _ => (),
        }
    }
    Ok(())
}

fn read_metainfo<P: AsRef<Path>>(metainfo_filepath: P) -> io::Result<Rc<Metainfo>> {
    let file_content = fs::read(metainfo_filepath)?;
    let root_entity = benc::Element::from_bytes(&file_content)?;
    let metainfo = Metainfo::try_from(root_entity)
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "Invalid metainfo file"))?;
    Ok(Rc::new(metainfo))
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

fn main() -> io::Result<()> {
    simple_logger::SimpleLogger::new()
        .with_threads(true)
        .with_level(log::LevelFilter::Off)
        .with_module_level("mtorrent", log::LevelFilter::Debug)
        .with_module_level("seeders", log::LevelFilter::Debug)
        .init()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}", e)))?;

    let metainfo = read_metainfo("tests/example.torrent")?;

    let pieces = Rc::new(data::PieceInfo::new(
        metainfo.pieces().ok_or_else(|| io::Error::from(io::ErrorKind::Other))?,
        metainfo.piece_length().ok_or_else(|| io::Error::from(io::ErrorKind::Other))?,
        metainfo
            .files()
            .ok_or_else(|| io::Error::from(io::ErrorKind::Other))?
            .map(|(len, _path)| len)
            .sum(),
    ));

    let (storage, _storage_handle) = {
        let output_dir = "seeders_input";
        let storage = if let Some(files) = metainfo.files() {
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
        start_storage(storage)
    };

    runtime::Builder::new_current_thread().enable_all().build()?.block_on(
        task::LocalSet::new().run_until(async move {
            future::join_all(env::args().skip(1).filter_map(|arg| {
                let port = str::parse::<u16>(&arg).ok()?;
                let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
                Some(run_one_seeder(addr, storage.clone(), pieces.clone()))
            }))
            .await;
        }),
    );

    Ok(())
}
