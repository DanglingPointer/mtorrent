use crate::ops;
use crate::utils::peer_id::PeerId;
use crate::utils::{ip, startup, upnp};
use std::io;
use std::net::{SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use tokio::net::TcpStream;
use tokio::{runtime, task};

#[tokio::main(flavor = "current_thread")]
pub async fn single_torrent_main(
    local_peer_id: PeerId,
    metainfo_filepath: impl AsRef<Path>,
    output_filedir: impl AsRef<Path>,
    pwp_runtime: runtime::Handle,
    storage_runtime: runtime::Handle,
) -> io::Result<()> {
    let metainfo = startup::read_metainfo(metainfo_filepath)?;

    let output_filedir = {
        let mut tmp = PathBuf::new();
        tmp.push(output_filedir);
        tmp.push(metainfo.name().unwrap_or(&format!("{local_peer_id}")));
        tmp.into_boxed_path()
    };

    let (storage, storage_server) = startup::create_storage(&metainfo, output_filedir)?;
    storage_runtime.spawn(async move {
        storage_server.run().await;
    });

    let local_task = task::LocalSet::new();

    let listener_addr = ip::any_socketaddr_from_hash(&metainfo);
    // get public ip to send correct listening port to trackers later
    let public_pwp_ip = match upnp::PortOpener::new(
        SocketAddrV4::new(ip::get_local_addr()?, listener_addr.port()),
        igd::PortMappingProtocol::TCP,
    )
    .await
    {
        Ok(port_opener) => {
            let public_ipv4 = port_opener.external_ip();
            log::info!("UPnP succeeded, public ip: {}", public_ipv4);
            local_task.spawn_local(async move {
                if let Err(e) = port_opener.do_continuous_renewal().await {
                    log::error!("UPnP port renewal failed: {e}");
                }
            });
            SocketAddr::V4(public_ipv4)
        }
        Err(e) => {
            log::error!("UPnP failed: {e}");
            listener_addr
        }
    };

    let ctx = ops::new_ctx(metainfo, local_peer_id)?;

    let incoming_connection_storage = storage.clone();
    let incoming_connection_pwp_runtime = pwp_runtime.clone();
    let incoming_connection_ctx = ctx.clone();
    let on_incoming_connection = move |stream: TcpStream| {
        let storage = incoming_connection_storage.clone();
        let pwp_runtime = incoming_connection_pwp_runtime.clone();
        let ctx = incoming_connection_ctx.clone();
        task::spawn_local(async move {
            match ops::incoming_pwp_connection(stream, storage, ctx, pwp_runtime).await {
                Ok(_) => (),
                Err(e) => log::error!("Incoming peer connection exited: {e}"),
            }
        });
    };

    local_task.spawn_local(async move {
        match ops::run_pwp_listener(listener_addr, on_incoming_connection).await {
            Ok(_) => (),
            Err(e) => log::error!("TCP listener exited: {e}"),
        }
    });

    let announce_response_storage = storage.clone();
    let announce_response_pwp_runtime = pwp_runtime.clone();
    let announce_response_ctx = ctx.clone();
    let on_announce_response = move |response: ops::TrackerResponse| {
        for peer_ip in response.peers {
            let storage = announce_response_storage.clone();
            let pwp_runtime = announce_response_pwp_runtime.clone();
            let ctx = announce_response_ctx.clone();
            task::spawn_local(async move {
                match ops::outgoing_pwp_connection(peer_ip, storage, ctx, pwp_runtime).await {
                    Ok(_) => (),
                    Err(e) => log::error!("Outgoing peer connection exited: {e}"),
                }
            });
        }
    };

    let tracker_ctx = ctx.clone();
    local_task.spawn_local(async move {
        ops::run_periodic_announces(tracker_ctx, public_pwp_ip.port(), on_announce_response).await;
    });

    local_task
        .run_until(async move {
            ops::periodic_state_dump(ctx).await;
        })
        .await;
    Ok(())
}
