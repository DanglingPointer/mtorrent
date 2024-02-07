use mtorrent::tracker::{http, udp, utils};
use mtorrent::utils::{benc, ip, meta};
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use tokio::net::UdpSocket;

fn read_metainfo(path: &str) -> meta::Metainfo {
    let data = fs::read(path).unwrap();
    let entity = benc::Element::from_bytes(&data).unwrap();
    if let benc::Element::Dictionary(ref dict) = entity {
        assert!(!dict.is_empty());
    } else {
        panic!("Not a dictionary");
    }
    meta::Metainfo::try_from(entity).unwrap()
}

#[ignore]
#[tokio::test]
async fn test_udp_announce() {
    let metainfo = read_metainfo("tests/example.torrent");
    let udp_tracker_addrs = utils::get_udp_tracker_addrs(&metainfo);

    let local_ip = SocketAddr::V4(SocketAddrV4::new(ip::get_local_addr().unwrap(), 6666));

    let announce_request = udp::AnnounceRequest {
        info_hash: *metainfo.info_hash(),
        peer_id: [b'm'; 20],
        downloaded: 0,
        left: 200,
        uploaded: 0,
        event: udp::AnnounceEvent::None,
        ip: None,
        key: 0,
        num_want: Some(5),
        port: local_ip.port(),
    };

    for tracker_addr in &udp_tracker_addrs {
        let client_socket = UdpSocket::bind(local_ip).await.unwrap();
        client_socket
            .connect(&tracker_addr)
            .await
            .unwrap_or_else(|e| panic!("Failed to connect to {}: {}", &tracker_addr, e));

        let client = udp::UdpTrackerConnection::from_connected_socket(client_socket).await.unwrap();

        let response = client
            .do_announce_request(announce_request.clone())
            .await
            .unwrap_or_else(|e| panic!("Announce error: {e}"));

        println!("Announce response: {:?}", response);
        let peer_count = response.ips.len();
        let seeders = response.seeders as usize;
        let leechers = response.leechers as usize;
        assert_eq!(peer_count, seeders + leechers);
    }
}

#[ignore]
#[tokio::test]
async fn test_udp_scrape() {
    let udp_tracker_addrs = [
        "udp://open.stealth.si:80/announce",
        "udp://tracker.opentrackr.org:1337/announce",
        "udp://tracker.tiny-vps.com:6969/announce",
        "udp://tracker.internetwarriors.net:1337/announce",
        "udp://tracker.skyts.net:6969/announce",
    ];

    let local_set = tokio::task::LocalSet::new();
    for (i, tracker_addr) in udp_tracker_addrs
        .iter()
        .filter_map(|uri| {
            let udp_addr = uri.strip_prefix("udp://")?;
            if let Some(stripped) = udp_addr.strip_suffix("/announce") {
                Some(stripped.to_string())
            } else {
                Some(udp_addr.to_string())
            }
        })
        .enumerate()
    {
        local_set.spawn_local(async move {
            let bind_addr =
                SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 6666 + i as u16));
            let client_socket = UdpSocket::bind(bind_addr).await.unwrap();
            if client_socket.connect(&tracker_addr).await.is_err() {
                println!("Failed to connect to {tracker_addr}");
                return;
            }
            let client =
                udp::UdpTrackerConnection::from_connected_socket(client_socket).await.unwrap();
            let response = client
                .do_scrape_request(udp::ScrapeRequest {
                    info_hashes: Vec::new(),
                })
                .await
                .unwrap();
            println!("Response from {tracker_addr}: {response:?}");
        });
    }
    local_set.await;
}

#[ignore]
#[tokio::test]
async fn test_https_announce() {
    let metainfo = read_metainfo("tests/ubuntu-22.04.3-live-server-amd64.iso.torrent");

    for tracker_url in utils::get_http_tracker_addrs(&metainfo) {
        let mut request = http::TrackerRequestBuilder::try_from(tracker_url.as_str()).unwrap();
        request
            .info_hash(metainfo.info_hash())
            .peer_id(&[b'm'; 20])
            .bytes_left(0)
            .bytes_uploaded(0)
            .bytes_downloaded(0)
            .port(6666);

        let response = http::do_announce_request(request)
            .await
            .unwrap_or_else(|e| panic!("Announce error: {e}"));

        println!("Announce response: {}", response);
        let peer_count = response.peers().unwrap().len();
        let seeders = response.complete().unwrap();
        let leechers = response.incomplete().unwrap();
        assert!(peer_count <= seeders + leechers);
    }
}

#[ignore]
#[tokio::test]
async fn test_https_scrape() {
    let request =
        http::TrackerRequestBuilder::try_from("https://torrent.ubuntu.com/announce").unwrap();
    let response = http::do_scrape_request(request)
        .await
        .unwrap_or_else(|e| panic!("Scrape error: {e}"));
    println!("{response}");
    assert!(matches!(response, benc::Element::Dictionary(_)));
}
