use mtorrent::tracker::{http, udp, utils};
use mtorrent::utils::{benc, meta};
use std::fs;
use std::net::{SocketAddr, SocketAddrV4};
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

    let local_ip = SocketAddr::V4(SocketAddrV4::new(utils::get_local_ip().unwrap(), 6666));

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
async fn test_http_announce() {
    // http://tracker.openbittorrent.com/announce?info_hash=%03%d1U%23cSD%5c%dc%80J%2b%25%05Rsin%cf%9c&peer_id=-qB4410-nMgOHMqB8E2h&port=23005&uploaded=0&downloaded=0&left=0&corrupt=0&key=5352D2ED&event=started&numwant=200&compact=1&no_peer_id=1&supportcrypto=1&redundant=0

    let mut request =
        http::TrackerRequestBuilder::try_from("http://tracker.openbittorrent.com/announce")
            .unwrap();
    request
        .info_hash(
            b"\x03\xd1\x55\x23\x63\x53\x44\x5c\xdc\x80\x4a\x2b\x25\x05\x52\x73\x69\x6e\xcf\x9c",
        )
        .peer_id(&[b'm'; 20])
        .bytes_left(0)
        .bytes_uploaded(0)
        .bytes_downloaded(0)
        .event(http::Event::Started)
        .port(6666);

    let response = http::do_announce_request(request)
        .await
        .unwrap_or_else(|e| panic!("Announce error: {e}"));

    println!("Announce response: {}", response);
    let peer_count = response.peers().unwrap().len();
    let seeders = response.complete().unwrap();
    let leechers = response.incomplete().unwrap();
    assert_eq!(peer_count, seeders + leechers);
}

#[ignore]
#[tokio::test]
async fn test_https_announce() {
    let metainfo = read_metainfo("tests/ubuntu-22.04.2-desktop-amd64.iso.torrent");

    for tracker_url in utils::get_http_tracker_addrs(&metainfo) {
        let mut request = http::TrackerRequestBuilder::try_from(tracker_url.as_str()).unwrap();
        request
            .info_hash(metainfo.info_hash())
            .peer_id(&[b'm'; 20])
            .bytes_left(0)
            .bytes_uploaded(0)
            .bytes_downloaded(0)
            .event(http::Event::Started)
            .port(6666);

        let response = http::do_announce_request(request)
            .await
            .unwrap_or_else(|e| panic!("Announce error: {e}"));

        println!("Announce response: {}", response);
        let peer_count = response.peers().unwrap().len();
        let seeders = response.complete().unwrap();
        let leechers = response.incomplete().unwrap();
        assert_eq!(peer_count, seeders + leechers);
    }
}

#[ignore]
#[tokio::test]
async fn test_http_scrape() {
    let mut request =
        http::TrackerRequestBuilder::try_from("http://tracker.openbittorrent.com/announce")
            .unwrap();
    request.info_hash(
        b"\x03\xd1\x55\x23\x63\x53\x44\x5c\xdc\x80\x4a\x2b\x25\x05\x52\x73\x69\x6e\xcf\x9c",
    );
    let response = http::do_scrape_request(request)
        .await
        .unwrap_or_else(|e| panic!("Scrape error: {e}"));
    assert!(!response.is_empty());
}
