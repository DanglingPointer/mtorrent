use async_io::Async;
use igd;
use igd::PortMappingProtocol;
use log::{debug, error, info, Level};
use mtorrent::tracker::udp::{AnnounceEvent, AnnounceRequest, UdpTrackerConnection};
use mtorrent::{benc, meta};
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::{env, fs};

fn open_external_port(local_addr: SocketAddrV4) -> Result<u16, igd::Error> {
    info!("Searching gateway...");
    let gateway = igd::search_gateway(Default::default())?;
    info!("Found gateway: {}", gateway);

    info!("Adding port...");
    let external_port = gateway.add_any_port(PortMappingProtocol::UDP, local_addr, 5, "")?;
    info!("Port {} added!", external_port);

    Ok(external_port)
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
        error!("Couldn't open external port: {}", e);
        local_addr.port()
    });

    let info_hash = {
        let mut hash = [0u8; 20];
        if let Some(mut it) = metainfo.pieces() {
            if let Some(hash_array) = it.next() {
                hash.copy_from_slice(hash_array);
            }
        }
        hash
    };
    debug!("Info hash: {:?}", info_hash);

    let announce_request = AnnounceRequest {
        info_hash,
        peer_id: [0xae; 20],
        downloaded: 0,
        left: 200,
        uploaded: 0,
        event: AnnounceEvent::None,
        ip: None,
        key: 0,
        num_want: Some(5),
        port: external_port,
    };

    let mut pieces_it = metainfo.pieces().unwrap();

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

            let mut request = announce_request.clone();
            request.info_hash.copy_from_slice(pieces_it.next().unwrap());

            match client.do_announce_request(request).await {
                Ok(response) => info!("Announce response: {:?}", response),
                Err(e) => error!("Announce error: {}", e),
            }
            info!("Client stopped");
        });
    }
}
