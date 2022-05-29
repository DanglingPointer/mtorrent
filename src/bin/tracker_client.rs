use async_io::Async;
use futures::join;
use futures::prelude::*;
use igd;
use igd::{PortMappingProtocol, SearchOptions};
use log::{debug, error, info, Level};
use mtorrent::peers;
use mtorrent::tracker::udp::{AnnounceEvent, AnnounceRequest, UdpTrackerConnection};
use mtorrent::{benc, meta};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::time::Duration;
use std::{env, fs};

fn open_external_port(local_addr: SocketAddrV4) -> Result<u16, igd::Error> {
    info!("Searching gateway...");
    let gateway = igd::search_gateway(SearchOptions {
        timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    })?;
    info!("Found gateway: {}", gateway);

    info!("Adding port...");
    let external_port = gateway.add_any_port(PortMappingProtocol::UDP, local_addr, 5, "")?;
    info!("Port {} added!", external_port);

    Ok(external_port)
}

async fn receive_from_peer(mut downlink: peers::DownloadChannel, mut uplink: peers::UploadChannel) {
    let downlink_fut = async move {
        while let Ok(msg) = downlink.receive_message().await {
            info!("{} => {}", downlink.remote_ip(), msg);
        }
    };
    let uplink_fut = async move {
        while let Ok(msg) = uplink.receive_message().await {
            info!("{} => {}", uplink.remote_ip(), msg);
        }
    };
    join!(downlink_fut, uplink_fut);
}

async fn connect_to_peer(local_peer_id: &[u8; 20], info_hash: &[u8; 20], ip: SocketAddr) {
    info!("{} connecting...", ip);
    match peers::establish_outbound(&local_peer_id, info_hash, ip, None).await {
        Ok((downlink, uplink, runner)) => {
            info!("{} connected", ip);
            let run_fut = async move {
                if let Err(e) = runner.run().await {
                    error!("{} runner exited: {}", ip, e);
                }
            };
            let _ = join!(receive_from_peer(downlink, uplink), run_fut);
        }
        Err(e) => {
            error!("{} connect failed: {}", ip, e);
        }
    }
    info!("{} finished", ip);
}

fn main() {
    simple_logger::init_with_level(Level::Debug).unwrap();

    let metainfo = {
        let metainfo_file_name = if let Some(arg) = env::args().nth(1) {
            arg
        } else {
            "/home/mikhailv/Movies/torrents/example.torrent".to_string()
        };

        let source_content = fs::read(metainfo_file_name).unwrap();
        let root_entity = benc::Element::from_bytes(&source_content).unwrap();

        meta::MetaInfo::try_from(root_entity).unwrap()
    };

    let udp_tracker_addrs = {
        let mut udp_trackers = Vec::<String>::new();
        for list in metainfo.announce_list().unwrap() {
            for addr in list {
                if let Some(addr) = addr.strip_prefix("udp://") {
                    if let Some(stripped_addr) = addr.strip_suffix("/announce") {
                        udp_trackers.push(stripped_addr.to_string());
                    } else {
                        udp_trackers.push(addr.to_string());
                    }
                }
            }
        }
        info!("UDP trackers:");
        for tracker in &udp_trackers {
            info!("Tracker: {}", tracker);
        }
        udp_trackers
    };

    let local_ip_str = {
        let hostname_cmd = std::process::Command::new("hostname")
            .arg("-I")
            .output()
            .expect("'hostname' failed");
        let hostname_output = String::from_utf8_lossy(&hostname_cmd.stdout);
        debug!("'hostname -I' output: {}", hostname_output);

        hostname_output
            .split_once(" ")
            .expect("Unexpected output from 'hostname -I'")
            .0
            .to_string()
    };

    let local_addr = SocketAddrV4::new(
        local_ip_str
            .parse::<Ipv4Addr>()
            .expect("Failed to parse local ip"),
        6889,
    );

    let external_port = open_external_port(local_addr).unwrap_or_else(|e| {
        error!("UPnP failed: {}", e);
        local_addr.port()
    });

    debug!("Info hash: {:?}", metainfo.info_hash());

    let local_peer_id = {
        let local_peer_id_str = "-m i k h a i l  B T-";
        assert_eq!(20, local_peer_id_str.len());
        let mut id = [0u8; 20];
        id.copy_from_slice(local_peer_id_str.as_bytes());
        id
    };

    let announce_request = AnnounceRequest {
        info_hash: *metainfo.info_hash(),
        peer_id: local_peer_id,
        downloaded: 0,
        left: 200,
        uploaded: 0,
        event: AnnounceEvent::None,
        ip: None,
        key: 0,
        num_want: Some(5),
        port: external_port,
    };

    for tracker_addr in &udp_tracker_addrs {
        info!("-----------------------------------------------------------------------");
        info!("Creating client for tracker {} ...", tracker_addr);

        let client_socket = UdpSocket::bind(local_addr).unwrap();
        client_socket.connect(tracker_addr).unwrap();
        let client_socket = Async::<UdpSocket>::try_from(client_socket).unwrap();

        info!(
            "Local socket at {} successfully bound to tracker at {}",
            local_addr, tracker_addr
        );

        async_io::block_on(async {
            let client = UdpTrackerConnection::from_connected_socket(client_socket)
                .await
                .unwrap();

            let response = match client.do_announce_request(announce_request.clone()).await {
                Ok(response) => {
                    info!("Announce response: {:?}", response);
                    Some(response)
                }
                Err(e) => {
                    error!("Announce error: {}", e);
                    None
                }
            };

            if let Some(response) = response {
                async_io::block_on(future::join_all(
                    response
                        .ips
                        .into_iter()
                        .map(|ip| connect_to_peer(&local_peer_id, metainfo.info_hash(), ip)),
                ));
            }
        });
    }
}
