use super::*;
use std::cell::Cell;
use std::iter;
use std::net::{Ipv4Addr, SocketAddrV4};
use tokio::task::{self, yield_now};
use tokio::time::sleep;
use tokio_test::task::spawn;
use tokio_test::{assert_pending, assert_ready};

thread_local! {
    // To avoid having to add #[tokio::test] to every test, we need a way to disable calls to tokio::time::sleep(),
    // (which would panic without a tokio runtime).
    pub static SLEEP_ENABLED: Cell<bool> = const { Cell::new(false) };
}

const IP: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 12345));

fn tid(num: u8) -> Vec<u8> {
    vec![0u8, 0u8, 0u8, num]
}

fn setup_routing(
    outgoing_msgs_sink: mpsc::Sender<(Message, SocketAddr)>,
    incoming_msgs_source: mpsc::Receiver<(Message, SocketAddr)>,
) -> (OutboundQueries, InboundQueries, QueryRouter) {
    super::setup_queries(
        udp::MessageChannelSender(outgoing_msgs_sink),
        udp::MessageChannelReceiver(incoming_msgs_source),
        None,
    )
}

#[test]
fn test_outgoing_ping_success() {
    let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
    let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
    let (client, _server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
    let mut runner_fut = spawn(runner.run());

    let mut ping_fut = spawn(client.ping(
        IP,
        PingArgs {
            id: U160::from([1u8; 20]),
        },
    ));
    assert_pending!(ping_fut.poll());

    assert_pending!(runner_fut.poll());
    let (outgoing_ping, _) = outgoing_msgs_source.try_recv().unwrap();
    assert_eq!(outgoing_ping.transaction_id, tid(1));
    assert_eq!(outgoing_ping.version, None);
    if let MessageData::Query(QueryMsg::Ping(args)) = outgoing_ping.data {
        assert_eq!(args.id, U160::from([1u8; 20]));
    } else {
        panic!("outgoing message has incorrect type");
    }

    incoming_msgs_sink
        .try_send((
            Message {
                transaction_id: outgoing_ping.transaction_id,
                version: None,
                data: MessageData::Response(
                    PingResponse {
                        id: U160::from([2u8; 20]),
                    }
                    .into(),
                ),
            },
            IP,
        ))
        .unwrap();
    assert!(runner_fut.is_woken());
    assert_pending!(runner_fut.poll());
    assert!(ping_fut.is_woken());
    let ping_result = assert_ready!(ping_fut.poll());
    let ping_response = ping_result.unwrap();
    assert_eq!(ping_response.id, U160::from([2u8; 20]));
}

#[test]
fn test_outgoing_find_node_success() {
    let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
    let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
    let (client, _server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
    let mut runner_fut = spawn(runner.run());

    let mut find_node_fut = spawn(client.find_node(
        IP,
        FindNodeArgs {
            id: U160::from([1u8; 20]),
            target: U160::from([2u8; 20]),
        },
    ));
    assert_pending!(find_node_fut.poll());

    assert_pending!(runner_fut.poll());
    let (outgoing_find_node, _) = outgoing_msgs_source.try_recv().unwrap();
    assert_eq!(outgoing_find_node.transaction_id, tid(1));
    assert_eq!(outgoing_find_node.version, None);
    if let MessageData::Query(QueryMsg::FindNode(args)) = outgoing_find_node.data {
        assert_eq!(args.id, U160::from([1u8; 20]));
        assert_eq!(args.target, U160::from([2u8; 20]));
    } else {
        panic!("outgoing message has incorrect type");
    }

    incoming_msgs_sink
        .try_send((
            Message {
                transaction_id: outgoing_find_node.transaction_id,
                version: None,
                data: MessageData::Response(
                    FindNodeResponse {
                        id: U160::from([3u8; 20]),
                        nodes: vec![(
                            [4u8; 20].into(),
                            SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1234),
                        )],
                    }
                    .into(),
                ),
            },
            IP,
        ))
        .unwrap();
    assert_pending!(runner_fut.poll());
    let find_node_result = assert_ready!(find_node_fut.poll());
    let find_node_response = find_node_result.unwrap();
    assert_eq!(find_node_response.id, U160::from([3u8; 20]));
    assert_eq!(
        find_node_response.nodes,
        vec![([4u8; 20].into(), SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1234))]
    );
}

#[test]
fn test_outgoing_get_peers_success() {
    let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
    let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
    let (client, _server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
    let mut runner_fut = spawn(runner.run());

    let mut get_peers_fut = spawn(client.get_peers(
        IP,
        GetPeersArgs {
            id: U160::from([1u8; 20]),
            info_hash: U160::from([2u8; 20]),
        },
    ));
    assert_pending!(get_peers_fut.poll());

    assert_pending!(runner_fut.poll());
    let (outgoing_get_peers, _) = outgoing_msgs_source.try_recv().unwrap();
    assert_eq!(outgoing_get_peers.transaction_id, tid(1));
    assert_eq!(outgoing_get_peers.version, None);
    if let MessageData::Query(QueryMsg::GetPeers(args)) = outgoing_get_peers.data {
        assert_eq!(args.id, U160::from([1u8; 20]));
        assert_eq!(args.info_hash, U160::from([2u8; 20]));
    } else {
        panic!("outgoing message has incorrect type");
    }

    incoming_msgs_sink
        .try_send((
            Message {
                transaction_id: outgoing_get_peers.transaction_id,
                version: None,
                data: MessageData::Response(
                    GetPeersResponse {
                        id: U160::from([3u8; 20]),
                        token: Some(vec![4u8; 2]),
                        data: GetPeersResponseData::Peers(vec![SocketAddrV4::new(
                            Ipv4Addr::LOCALHOST,
                            1234,
                        )]),
                    }
                    .into(),
                ),
            },
            IP,
        ))
        .unwrap();
    assert_pending!(runner_fut.poll());
    let get_peers_result = assert_ready!(get_peers_fut.poll());
    let get_peers_response = get_peers_result.unwrap();
    assert_eq!(get_peers_response.id, U160::from([3u8; 20]));
    assert_eq!(get_peers_response.token, Some(vec![4u8; 2]));
    assert!(
        matches!(get_peers_response.data, GetPeersResponseData::Peers(peers) if peers == vec![SocketAddrV4::new(
            Ipv4Addr::LOCALHOST,
            1234,
        )])
    );
}

#[test]
fn test_outgoing_announce_peer_success() {
    let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
    let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
    let (client, _server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
    let mut runner_fut = spawn(runner.run());

    let mut announce_peer_fut = spawn(client.announce_peer(
        IP,
        AnnouncePeerArgs {
            id: U160::from([1u8; 20]),
            info_hash: U160::from([2u8; 20]),
            port: Some(1234),
            token: vec![3u8; 2],
        },
    ));
    assert_pending!(announce_peer_fut.poll());

    assert_pending!(runner_fut.poll());
    let (outgoing_announce_peer, _) = outgoing_msgs_source.try_recv().unwrap();
    assert_eq!(outgoing_announce_peer.transaction_id, tid(1));
    assert_eq!(outgoing_announce_peer.version, None);
    if let MessageData::Query(QueryMsg::AnnouncePeer(args)) = outgoing_announce_peer.data {
        assert_eq!(args.id, U160::from([1u8; 20]));
        assert_eq!(args.info_hash, U160::from([2u8; 20]));
        assert_eq!(args.port, Some(1234));
        assert_eq!(args.token, vec![3u8; 2]);
    } else {
        panic!("outgoing message has incorrect type");
    }

    incoming_msgs_sink
        .try_send((
            Message {
                transaction_id: outgoing_announce_peer.transaction_id,
                version: None,
                data: MessageData::Response(
                    AnnouncePeerResponse {
                        id: U160::from([3u8; 20]),
                    }
                    .into(),
                ),
            },
            IP,
        ))
        .unwrap();
    assert_pending!(runner_fut.poll());
    let announce_peer_result = assert_ready!(announce_peer_fut.poll());
    let announce_peer_response = announce_peer_result.unwrap();
    assert_eq!(announce_peer_response.id, U160::from([3u8; 20]));
}

#[test]
fn test_concurrent_outgoing_queries_out_of_order() {
    let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
    let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
    let (client, _server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
    let mut runner_fut = spawn(runner.run());

    // start ping
    let mut ping_fut = spawn(client.ping(
        IP,
        PingArgs {
            id: U160::from([1u8; 20]),
        },
    ));
    assert_pending!(ping_fut.poll());

    // start announce peer
    let mut announce_peer_fut = spawn(client.announce_peer(
        IP,
        AnnouncePeerArgs {
            id: U160::from([1u8; 20]),
            info_hash: U160::from([2u8; 20]),
            port: Some(1234),
            token: vec![3u8; 2],
        },
    ));
    assert_pending!(announce_peer_fut.poll());

    // verify ping query
    assert_pending!(runner_fut.poll());
    let (outgoing_ping, _) = outgoing_msgs_source.try_recv().unwrap();
    assert_eq!(outgoing_ping.transaction_id, tid(1));
    assert_eq!(outgoing_ping.version, None);
    if let MessageData::Query(QueryMsg::Ping(args)) = outgoing_ping.data {
        assert_eq!(args.id, U160::from([1u8; 20]));
    } else {
        panic!("outgoing message has incorrect type");
    }

    // verify announce peer
    assert_pending!(announce_peer_fut.poll());
    let (outgoing_announce_peer, _) = outgoing_msgs_source.try_recv().unwrap();
    assert_eq!(outgoing_announce_peer.transaction_id, tid(2));
    assert_eq!(outgoing_announce_peer.version, None);
    if let MessageData::Query(QueryMsg::AnnouncePeer(args)) = outgoing_announce_peer.data {
        assert_eq!(args.id, U160::from([1u8; 20]));
        assert_eq!(args.info_hash, U160::from([2u8; 20]));
        assert_eq!(args.port, Some(1234));
        assert_eq!(args.token, vec![3u8; 2]);
    } else {
        panic!("outgoing message has incorrect type");
    }

    // respond to announce peer
    incoming_msgs_sink
        .try_send((
            Message {
                transaction_id: outgoing_announce_peer.transaction_id,
                version: None,
                data: MessageData::Response(
                    AnnouncePeerResponse {
                        id: U160::from([3u8; 20]),
                    }
                    .into(),
                ),
            },
            IP,
        ))
        .unwrap();
    assert!(runner_fut.is_woken());
    assert_pending!(runner_fut.poll());
    assert_pending!(ping_fut.poll());
    let announce_peer_result = assert_ready!(announce_peer_fut.poll());
    let announce_peer_response = announce_peer_result.unwrap();
    assert_eq!(announce_peer_response.id, U160::from([3u8; 20]));

    // respond to ping
    incoming_msgs_sink
        .try_send((
            Message {
                transaction_id: outgoing_ping.transaction_id,
                version: None,
                data: MessageData::Response(
                    PingResponse {
                        id: U160::from([2u8; 20]),
                    }
                    .into(),
                ),
            },
            IP,
        ))
        .unwrap();
    assert!(runner_fut.is_woken());
    assert_pending!(runner_fut.poll());
    let ping_result = assert_ready!(ping_fut.poll());
    let ping_response = ping_result.unwrap();
    assert_eq!(ping_response.id, U160::from([2u8; 20]));
}

#[tokio::test(start_paused = true)]
async fn test_outgoing_query_retransmissions() {
    task::LocalSet::new()
        .run_until(async {
            // let _ = simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Trace).init();
            SLEEP_ENABLED.with(|sleep_enabled| sleep_enabled.set(true));
            let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
            let (_incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
            let (client, _server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);

            task::spawn_local(runner.run());

            // start ping query
            let mut ping_fut = spawn(client.ping(
                IP,
                PingArgs {
                    id: U160::from([1u8; 20]),
                },
            ));
            assert_pending!(ping_fut.poll());

            // verify ping sent out on the network
            yield_now().await;
            let (outgoing_ping, _) = outgoing_msgs_source.try_recv().unwrap();
            assert_eq!(outgoing_ping.transaction_id, tid(1));
            assert!(matches!(outgoing_ping.data, MessageData::Query(QueryMsg::Ping(_))));

            let rtos_ms = [1500, 3000, 6000];
            for &interval_ms in &rtos_ms[..2] {
                sleep(millisec!(interval_ms)).await;
                assert!(outgoing_msgs_source.try_recv().is_err());
                yield_now().await;
                let (retransmitted_ping, _) = outgoing_msgs_source.try_recv().unwrap();
                assert_eq!(retransmitted_ping.transaction_id, outgoing_ping.transaction_id);
                assert!(matches!(retransmitted_ping.data, MessageData::Query(QueryMsg::Ping(_))));
                assert_pending!(ping_fut.poll());
            }

            sleep(millisec!(rtos_ms[2])).await;
            assert_pending!(ping_fut.poll());
            yield_now().await;
            assert!(outgoing_msgs_source.try_recv().is_err());
            let ping_result = assert_ready!(ping_fut.poll());
            let ping_error = ping_result.unwrap_err();
            assert!(matches!(ping_error, Error::Timeout));
        })
        .await;
}

macro_rules! verify_outgoing_msg_count {
    ($ping_count:expr, $get_peers_count:expr, $outgoing_msgs_source:expr) => {{
        yield_now().await;
        let msgs: Vec<Message> =
            iter::from_fn(|| $outgoing_msgs_source.try_recv().map(|(msg, _addr)| msg).ok())
                .collect();
        assert_eq!(
            msgs.iter()
                .filter(|msg| matches!(msg.data, MessageData::Query(QueryMsg::Ping(_)))
                    && msg.transaction_id == tid(1))
                .count(),
            $ping_count,
            "incorrect ping count"
        );
        assert_eq!(
            msgs.iter()
                .filter(|msg| matches!(msg.data, MessageData::Query(QueryMsg::GetPeers(_)))
                    && msg.transaction_id == tid(2))
                .count(),
            $get_peers_count,
            "incorrect get_peers count"
        );
        assert_eq!(msgs.len(), $ping_count + $get_peers_count, "incorrect total count");
    }};
}

#[tokio::test(start_paused = true)]
async fn test_outgoing_simultaneous_retransmissions() {
    // Timeline:
    // 0 ping
    // 1500 ping retransmit
    // 3000 get peers
    // 4500 ping retransmit + get peers retransmit
    // 7500 get peers retransmit
    // 10500 ping timeout
    // 13500 get peers timeout
    task::LocalSet::new()
        .run_until(async {
            // let _ = simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Trace).init();
            SLEEP_ENABLED.with(|sleep_enabled| sleep_enabled.set(true));
            let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
            let (_incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
            let (client, _server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);

            task::spawn_local(runner.run());

            macro_rules! verify_msg_count {
                ($ping_count:expr, $get_peers_count:expr) => {
                    verify_outgoing_msg_count!($ping_count, $get_peers_count, outgoing_msgs_source)
                };
            }

            // start ping query
            let mut ping_fut = spawn(client.ping(
                IP,
                PingArgs {
                    id: U160::from([1u8; 20]),
                },
            ));
            assert_pending!(ping_fut.poll());

            // verify ping sent out on the network
            verify_msg_count!(1, 0);

            // start get peers 3ms later
            sleep(millisec!(3000)).await;
            let mut get_peers_fut = spawn(client.get_peers(
                IP,
                GetPeersArgs {
                    id: U160::from([1u8; 20]),
                    info_hash: U160::from([2u8; 20]),
                },
            ));
            assert_pending!(get_peers_fut.poll());

            // verify get peers sent out on the network + 1 earlier ping retransmission
            verify_msg_count!(1, 1);

            // verify one more retransmission of each
            sleep(millisec!(1500)).await;
            verify_msg_count!(1, 1);

            // one more get_peers retransmit
            sleep(sec!(3)).await;
            verify_msg_count!(0, 1);
            assert_pending!(ping_fut.poll());

            // ping times out first
            sleep(sec!(3)).await;
            assert_pending!(ping_fut.poll());
            yield_now().await;
            assert!(outgoing_msgs_source.try_recv().is_err());
            let ping_result = assert_ready!(ping_fut.poll());
            let ping_error = ping_result.unwrap_err();
            assert!(matches!(ping_error, Error::Timeout));

            // get peers times out 3s later
            sleep(sec!(3)).await;
            assert_pending!(get_peers_fut.poll());
            yield_now().await;
            assert!(outgoing_msgs_source.try_recv().is_err());
            let get_peers_result = assert_ready!(get_peers_fut.poll());
            let get_peers_error = get_peers_result.unwrap_err();
            assert!(matches!(get_peers_error, Error::Timeout));
        })
        .await;
}

#[tokio::test(start_paused = true)]
async fn test_outgoing_interleaved_retransmissions_and_timer_cleanup() {
    // Timeline:
    // 0ms ping
    // 500 get_peers
    // 1500 ping retransmit
    // 2000 get_peers retransmit + ping response
    // 5000 get_peers retransmit
    // 11000 get_peers timeout
    task::LocalSet::new()
        .run_until(async {
            // let _ = simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Trace).init();
            SLEEP_ENABLED.with(|sleep_enabled| sleep_enabled.set(true));
            let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
            let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
            let (client, _server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);

            macro_rules! verify_msg_count {
                ($ping_count:expr, $get_peers_count:expr) => {
                    verify_outgoing_msg_count!($ping_count, $get_peers_count, outgoing_msgs_source)
                };
            }

            task::spawn_local(runner.run());

            // start ping query
            let mut ping_fut = spawn(client.ping(
                IP,
                PingArgs {
                    id: U160::from([1u8; 20]),
                },
            ));
            assert_pending!(ping_fut.poll());

            // verify ping sent out on the network
            yield_now().await;
            let (outgoing_ping, _) = outgoing_msgs_source.try_recv().unwrap();
            assert!(matches!(outgoing_ping.data, MessageData::Query(QueryMsg::Ping(_))));

            // start get peers 500ms later
            sleep(millisec!(500)).await;
            let mut get_peers_fut = spawn(client.get_peers(
                IP,
                GetPeersArgs {
                    id: U160::from([1u8; 20]),
                    info_hash: U160::from([2u8; 20]),
                },
            ));
            assert_pending!(get_peers_fut.poll());

            // verify get peers sent out on the network
            yield_now().await;
            let (outgoing_get_peers, _) = outgoing_msgs_source.try_recv().unwrap();
            assert!(matches!(outgoing_get_peers.data, MessageData::Query(QueryMsg::GetPeers(_))));

            // 1 get_peers retransmit + 1 earlier ping retransmit
            sleep(millisec!(1500)).await;
            verify_msg_count!(1, 1);

            // respond to ping
            incoming_msgs_sink
                .try_send((
                    Message {
                        transaction_id: outgoing_ping.transaction_id,
                        version: None,
                        data: MessageData::Response(
                            PingResponse {
                                id: [69u8; 20].into(),
                            }
                            .into(),
                        ),
                    },
                    IP,
                ))
                .unwrap();
            yield_now().await;
            let ping_result = assert_ready!(ping_fut.poll());
            let ping_response = ping_result.unwrap();
            assert_eq!(ping_response.id, U160::from([69u8; 20]));

            // get_peers retransmission
            sleep(sec!(3)).await;
            assert!(outgoing_msgs_source.try_recv().is_err());
            verify_msg_count!(0, 1);

            // get_peers times out
            sleep(sec!(6)).await;
            assert_pending!(get_peers_fut.poll());
            yield_now().await;
            assert!(outgoing_msgs_source.try_recv().is_err());
            let get_peers_result = assert_ready!(get_peers_fut.poll());
            let get_peers_error = get_peers_result.unwrap_err();
            assert!(matches!(get_peers_error, Error::Timeout));
        })
        .await;
}

#[test]
fn test_outgoing_ping_error_response() {
    let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
    let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
    let (client, _server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
    let mut runner_fut = spawn(runner.run());

    let mut ping_fut = spawn(client.ping(
        IP,
        PingArgs {
            id: U160::from([1u8; 20]),
        },
    ));
    assert_pending!(ping_fut.poll());

    assert_pending!(runner_fut.poll());
    let (outgoing_ping, _) = outgoing_msgs_source.try_recv().unwrap();
    assert_eq!(outgoing_ping.transaction_id, tid(1));
    assert_eq!(outgoing_ping.version, None);
    if let MessageData::Query(QueryMsg::Ping(args)) = outgoing_ping.data {
        assert_eq!(args.id, U160::from([1u8; 20]));
    } else {
        panic!("outgoing message has incorrect type");
    }

    incoming_msgs_sink
        .try_send((
            Message {
                transaction_id: outgoing_ping.transaction_id,
                version: None,
                data: MessageData::Error(ErrorMsg {
                    error_code: ErrorCode::Generic,
                    error_msg: "Something went wrong".to_string(),
                }),
            },
            IP,
        ))
        .unwrap();
    assert_pending!(runner_fut.poll());
    let ping_result = assert_ready!(ping_fut.poll());
    let ping_error = ping_result.unwrap_err();
    if let Error::ErrorResponse(ping_error) = ping_error {
        assert_eq!(ping_error.error_code, ErrorCode::Generic);
        assert_eq!(ping_error.error_msg, "Something went wrong");
    } else {
        panic!("unexpected error type");
    }
}

#[test]
fn test_outgoing_queries_limit_is_respected() {
    let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
    let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);

    // given: max 1 outstanding query
    let (client, _server, runner) = super::setup_queries(
        udp::MessageChannelSender(outgoing_msgs_sink),
        udp::MessageChannelReceiver(incoming_msgs_source),
        Some(1),
    );
    let mut runner_fut = spawn(runner.run());

    // when: first outgoing ping sent out
    let mut ping1_fut = spawn(client.ping(
        IP,
        PingArgs {
            id: U160::from([1u8; 20]),
        },
    ));
    assert_pending!(ping1_fut.poll());
    assert_pending!(runner_fut.poll());
    let (outgoing_ping1, _) = outgoing_msgs_source.try_recv().unwrap();
    assert!(matches!(outgoing_ping1.data, MessageData::Query(QueryMsg::Ping(_))));

    // then: second outgoing ping held back
    let mut ping2_fut = spawn(client.ping(
        IP,
        PingArgs {
            id: U160::from([1u8; 20]),
        },
    ));
    assert_pending!(ping2_fut.poll());
    assert_pending!(runner_fut.poll());
    assert!(outgoing_msgs_source.try_recv().is_err());

    // when: first ping response received
    incoming_msgs_sink
        .try_send((
            Message {
                transaction_id: outgoing_ping1.transaction_id,
                version: None,
                data: MessageData::Response(
                    PingResponse {
                        id: U160::from([2u8; 20]),
                    }
                    .into(),
                ),
            },
            IP,
        ))
        .unwrap();
    assert_pending!(runner_fut.poll());
    let ping_result = assert_ready!(ping1_fut.poll());
    let ping_response = ping_result.unwrap();
    assert_eq!(ping_response.id, U160::from([2u8; 20]));

    // then: second ping sent out
    assert!(ping2_fut.is_woken());
    assert_pending!(ping2_fut.poll());
    assert!(runner_fut.is_woken());
    assert_pending!(runner_fut.poll());
    let (outgoing_ping2, _) = outgoing_msgs_source.try_recv().unwrap();
    assert!(matches!(outgoing_ping2.data, MessageData::Query(QueryMsg::Ping(_))));
}

#[test]
fn test_outgoing_ping_channel_error() {
    let (outgoing_msgs_sink, outgoing_msgs_source) = mpsc::channel(8);
    let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
    let (client, _server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
    let mut runner_fut = spawn(runner.run());

    let mut ping_fut = spawn(client.ping(
        IP,
        PingArgs {
            id: U160::from([1u8; 20]),
        },
    ));
    assert_pending!(ping_fut.poll());

    drop(outgoing_msgs_source);
    drop(incoming_msgs_sink);
    assert_ready!(runner_fut.poll());
    let ping_result = assert_ready!(ping_fut.poll());
    let ping_error = ping_result.unwrap_err();
    assert!(matches!(ping_error, Error::ChannelClosed));
}

#[test]
fn test_incoming_ping_success() {
    let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
    let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
    let (_client, server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
    let mut runner_fut = spawn(runner.run());

    let mut incoming_query_fut = spawn(server.0);
    assert_pending!(incoming_query_fut.poll_next());
    assert_pending!(runner_fut.poll());

    incoming_msgs_sink
        .try_send((
            Message {
                transaction_id: vec![123u8, 234u8],
                version: None,
                data: MessageData::Query(
                    PingArgs {
                        id: U160::from([1u8; 20]),
                    }
                    .into(),
                ),
            },
            IP,
        ))
        .unwrap();
    assert_pending!(runner_fut.poll());
    let incoming_query = assert_ready!(incoming_query_fut.poll_next());
    let incoming_ping = match incoming_query {
        Some(IncomingQuery::Ping(ping)) => ping,
        Some(_) => panic!("incoming message has wrong type"),
        None => panic!("channel closed"),
    };
    assert_eq!(incoming_ping.args().id, U160::from([1u8; 20]));
    assert_eq!(incoming_ping.source_addr(), &IP);

    incoming_ping
        .respond(PingResponse {
            id: U160::from([2u8; 20]),
        })
        .unwrap();
    assert_pending!(runner_fut.poll());
    let (outgoing_response, _) = outgoing_msgs_source.try_recv().unwrap();
    assert_eq!(outgoing_response.transaction_id, vec![123u8, 234u8]);
    assert_eq!(outgoing_response.version, None);
    let ping_response: PingResponse = match outgoing_response.data {
        MessageData::Response(response_msg) => response_msg.try_into().unwrap(),
        _ => panic!("outgoing message is not a response"),
    };
    assert_eq!(ping_response.id, U160::from([2u8; 20]));
}

#[test]
fn test_incoming_find_node_success() {
    let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
    let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
    let (_client, server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
    let mut runner_fut = spawn(runner.run());

    let mut incoming_query_fut = spawn(server.0);
    assert_pending!(incoming_query_fut.poll_next());
    assert_pending!(runner_fut.poll());

    incoming_msgs_sink
        .try_send((
            Message {
                transaction_id: vec![234u8, 123u8],
                version: None,
                data: MessageData::Query(
                    FindNodeArgs {
                        id: U160::from([1u8; 20]),
                        target: U160::from([2u8; 20]),
                    }
                    .into(),
                ),
            },
            IP,
        ))
        .unwrap();
    assert_pending!(runner_fut.poll());
    let incoming_query = assert_ready!(incoming_query_fut.poll_next());
    let incoming_find_node = match incoming_query {
        Some(IncomingQuery::FindNode(find_node)) => find_node,
        Some(_) => panic!("incoming message has wrong type"),
        None => panic!("channel closed"),
    };
    assert_eq!(incoming_find_node.args().id, U160::from([1u8; 20]));
    assert_eq!(incoming_find_node.args().target, U160::from([2u8; 20]));

    incoming_find_node
        .respond(FindNodeResponse {
            id: U160::from([3u8; 20]),
            nodes: vec![([4u8; 20].into(), SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1234))],
        })
        .unwrap();
    assert_pending!(runner_fut.poll());
    let (outgoing_response, _) = outgoing_msgs_source.try_recv().unwrap();
    assert_eq!(outgoing_response.transaction_id, vec![234u8, 123u8]);
    assert_eq!(outgoing_response.version, None);
    let find_node_response: FindNodeResponse = match outgoing_response.data {
        MessageData::Response(response_msg) => response_msg.try_into().unwrap(),
        _ => panic!("outgoing message is not a response"),
    };
    assert_eq!(find_node_response.id, U160::from([3u8; 20]));
    assert_eq!(
        find_node_response.nodes,
        vec![([4u8; 20].into(), SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1234))]
    );
}

#[test]
fn test_incoming_get_peers_success() {
    let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
    let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
    let (_client, server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
    let mut runner_fut = spawn(runner.run());

    let mut incoming_query_fut = spawn(server.0);
    assert_pending!(incoming_query_fut.poll_next());
    assert_pending!(runner_fut.poll());

    incoming_msgs_sink
        .try_send((
            Message {
                transaction_id: vec![234u8, 123u8],
                version: None,
                data: MessageData::Query(
                    GetPeersArgs {
                        id: U160::from([1u8; 20]),
                        info_hash: U160::from([2u8; 20]),
                    }
                    .into(),
                ),
            },
            IP,
        ))
        .unwrap();
    assert_pending!(runner_fut.poll());
    let incoming_query = assert_ready!(incoming_query_fut.poll_next());
    let incoming_get_peers = match incoming_query {
        Some(IncomingQuery::GetPeers(get_peers)) => get_peers,
        Some(_) => panic!("incoming message has wrong type"),
        None => panic!("channel closed"),
    };
    assert_eq!(incoming_get_peers.args().id, U160::from([1u8; 20]));
    assert_eq!(incoming_get_peers.args().info_hash, U160::from([2u8; 20]));

    incoming_get_peers
        .respond(GetPeersResponse {
            id: U160::from([3u8; 20]),
            token: Some(vec![4u8; 2]),
            data: GetPeersResponseData::Peers(vec![SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1234)]),
        })
        .unwrap();
    assert_pending!(runner_fut.poll());
    let (outgoing_response, _) = outgoing_msgs_source.try_recv().unwrap();
    assert_eq!(outgoing_response.transaction_id, vec![234u8, 123u8]);
    assert_eq!(outgoing_response.version, None);
    let get_peers_response: GetPeersResponse = match outgoing_response.data {
        MessageData::Response(response_msg) => response_msg.try_into().unwrap(),
        _ => panic!("outgoing message is not a response"),
    };
    assert_eq!(get_peers_response.id, U160::from([3u8; 20]));
    assert_eq!(get_peers_response.token, Some(vec![4u8; 2]));
    assert!(
        matches!(get_peers_response.data, GetPeersResponseData::Peers(peers) if peers == vec![SocketAddrV4::new(
            Ipv4Addr::LOCALHOST,
            1234,
        )])
    );
}

#[test]
fn test_incoming_announce_peer_success() {
    let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
    let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
    let (_client, server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
    let mut runner_fut = spawn(runner.run());

    let mut incoming_query_fut = spawn(server.0);
    assert_pending!(incoming_query_fut.poll_next());
    assert_pending!(runner_fut.poll());

    incoming_msgs_sink
        .try_send((
            Message {
                transaction_id: vec![234u8, 123u8],
                version: None,
                data: MessageData::Query(
                    AnnouncePeerArgs {
                        id: U160::from([1u8; 20]),
                        info_hash: U160::from([2u8; 20]),
                        port: Some(1234),
                        token: vec![3u8; 2],
                    }
                    .into(),
                ),
            },
            IP,
        ))
        .unwrap();
    assert_pending!(runner_fut.poll());
    let incoming_query = assert_ready!(incoming_query_fut.poll_next());
    let incoming_announce_peer = match incoming_query {
        Some(IncomingQuery::AnnouncePeer(announce_peer)) => announce_peer,
        Some(_) => panic!("incoming message has wrong type"),
        None => panic!("channel closed"),
    };
    assert_eq!(incoming_announce_peer.args().id, U160::from([1u8; 20]));
    assert_eq!(incoming_announce_peer.args().info_hash, U160::from([2u8; 20]));
    assert_eq!(incoming_announce_peer.args().port, Some(1234));
    assert_eq!(incoming_announce_peer.args().token, vec![3u8; 2]);

    incoming_announce_peer
        .respond(AnnouncePeerResponse {
            id: [4u8; 20].into(),
        })
        .unwrap();
    assert_pending!(runner_fut.poll());
    let (outgoing_response, _) = outgoing_msgs_source.try_recv().unwrap();
    assert_eq!(outgoing_response.transaction_id, vec![234u8, 123u8]);
    assert_eq!(outgoing_response.version, None);
    let announce_peer_response: AnnouncePeerResponse = match outgoing_response.data {
        MessageData::Response(response_msg) => response_msg.try_into().unwrap(),
        _ => panic!("outgoing message is not a response"),
    };
    assert_eq!(announce_peer_response.id, U160::from([4u8; 20]));
}

#[test]
fn test_concurrent_incoming_queries_out_of_order() {
    let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
    let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
    let (_client, server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
    let mut runner_fut = spawn(runner.run());

    let mut incoming_query_fut = spawn(server.0);
    assert_pending!(incoming_query_fut.poll_next());
    assert_pending!(runner_fut.poll());

    // incoming find node
    incoming_msgs_sink
        .try_send((
            Message {
                transaction_id: vec![234u8, 123u8],
                version: None,
                data: MessageData::Query(
                    FindNodeArgs {
                        id: U160::from([1u8; 20]),
                        target: U160::from([2u8; 20]),
                    }
                    .into(),
                ),
            },
            IP,
        ))
        .unwrap();
    assert_pending!(runner_fut.poll());
    let incoming_query = assert_ready!(incoming_query_fut.poll_next());
    let incoming_find_node = match incoming_query {
        Some(IncomingQuery::FindNode(find_node)) => find_node,
        Some(_) => panic!("incoming message has wrong type"),
        None => panic!("channel closed"),
    };
    assert_eq!(incoming_find_node.args().id, U160::from([1u8; 20]));
    assert_eq!(incoming_find_node.args().target, U160::from([2u8; 20]));

    // incoming ping
    incoming_msgs_sink
        .try_send((
            Message {
                transaction_id: vec![234u8, 124u8],
                version: None,
                data: MessageData::Query(
                    PingArgs {
                        id: U160::from([3u8; 20]),
                    }
                    .into(),
                ),
            },
            IP,
        ))
        .unwrap();
    assert_pending!(runner_fut.poll());
    let incoming_query = assert_ready!(incoming_query_fut.poll_next());
    let incoming_ping = match incoming_query {
        Some(IncomingQuery::Ping(ping)) => ping,
        Some(_) => panic!("incoming message has wrong type"),
        None => panic!("channel closed"),
    };
    assert_eq!(incoming_ping.args().id, U160::from([3u8; 20]));

    // respond to ping
    incoming_ping
        .respond(PingResponse {
            id: U160::from([2u8; 20]),
        })
        .unwrap();
    assert_pending!(runner_fut.poll());
    let (outgoing_response, _) = outgoing_msgs_source.try_recv().unwrap();
    assert_eq!(outgoing_response.transaction_id, vec![234u8, 124u8]);
    assert_eq!(outgoing_response.version, None);
    let ping_response: PingResponse = match outgoing_response.data {
        MessageData::Response(response_msg) => response_msg.try_into().unwrap(),
        _ => panic!("outgoing message is not a response"),
    };
    assert_eq!(ping_response.id, U160::from([2u8; 20]));

    // respond to find node
    incoming_find_node
        .respond(FindNodeResponse {
            id: U160::from([3u8; 20]),
            nodes: vec![],
        })
        .unwrap();
    assert_pending!(runner_fut.poll());
    let (outgoing_response, _) = outgoing_msgs_source.try_recv().unwrap();
    assert_eq!(outgoing_response.transaction_id, vec![234u8, 123u8]);
    assert_eq!(outgoing_response.version, None);
    let find_node_response: FindNodeResponse = match outgoing_response.data {
        MessageData::Response(response_msg) => response_msg.try_into().unwrap(),
        _ => panic!("outgoing message is not a response"),
    };
    assert_eq!(find_node_response.id, U160::from([3u8; 20]));
    assert!(find_node_response.nodes.is_empty());
}

#[test]
fn test_incoming_ping_error_response() {
    let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
    let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
    let (_client, server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
    let mut runner_fut = spawn(runner.run());

    let mut incoming_query_fut = spawn(server.0);
    assert_pending!(incoming_query_fut.poll_next());
    assert_pending!(runner_fut.poll());

    incoming_msgs_sink
        .try_send((
            Message {
                transaction_id: vec![234u8, 123u8],
                version: None,
                data: MessageData::Query(
                    PingArgs {
                        id: U160::from([1u8; 20]),
                    }
                    .into(),
                ),
            },
            IP,
        ))
        .unwrap();
    assert_pending!(runner_fut.poll());
    let incoming_query = assert_ready!(incoming_query_fut.poll_next());
    let incoming_ping = match incoming_query {
        Some(IncomingQuery::Ping(ping)) => ping,
        Some(_) => panic!("incoming message has wrong type"),
        None => panic!("channel closed"),
    };
    assert_eq!(incoming_ping.args().id, U160::from([1u8; 20]));

    incoming_ping
        .respond_error(ErrorMsg {
            error_code: ErrorCode::Generic,
            error_msg: "Something went wrong".to_string(),
        })
        .unwrap();
    assert_pending!(runner_fut.poll());
    let (outgoing_response, _) = outgoing_msgs_source.try_recv().unwrap();
    assert_eq!(outgoing_response.transaction_id, vec![234u8, 123u8]);
    assert_eq!(outgoing_response.version, None);
    let ping_error: ErrorMsg = match outgoing_response.data {
        MessageData::Error(error_msg) => error_msg,
        _ => panic!("outgoing message is not an error"),
    };
    assert_eq!(ping_error.error_code, ErrorCode::Generic);
    assert_eq!(ping_error.error_msg, "Something went wrong");
}

#[test]
fn test_incoming_ping_error_response_when_dropped() {
    let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(8);
    let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
    let (_client, server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
    let mut runner_fut = spawn(runner.run());

    let mut incoming_query_fut = spawn(server.0);
    assert_pending!(incoming_query_fut.poll_next());
    assert_pending!(runner_fut.poll());

    incoming_msgs_sink
        .try_send((
            Message {
                transaction_id: vec![234u8, 123u8],
                version: None,
                data: MessageData::Query(
                    PingArgs {
                        id: U160::from([1u8; 20]),
                    }
                    .into(),
                ),
            },
            IP,
        ))
        .unwrap();
    assert_pending!(runner_fut.poll());
    let incoming_query = assert_ready!(incoming_query_fut.poll_next());
    let incoming_ping = match incoming_query {
        Some(IncomingQuery::Ping(ping)) => ping,
        Some(_) => panic!("incoming message has wrong type"),
        None => panic!("channel closed"),
    };
    assert_eq!(incoming_ping.args().id, U160::from([1u8; 20]));

    drop(incoming_ping);
    assert_pending!(runner_fut.poll());
    let (outgoing_response, _) = outgoing_msgs_source.try_recv().unwrap();
    assert_eq!(outgoing_response.transaction_id, vec![234u8, 123u8]);
    assert_eq!(outgoing_response.version, None);
    let ping_error: ErrorMsg = match outgoing_response.data {
        MessageData::Error(error_msg) => error_msg,
        _ => panic!("outgoing message is not an error"),
    };
    assert_eq!(ping_error.error_code, ErrorCode::Server);
    assert_eq!(ping_error.error_msg, "Unable to handle query");
}

#[test]
fn test_incoming_ping_channel_error() {
    let (outgoing_msgs_sink, outgoing_msgs_source) = mpsc::channel(8);
    let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(8);
    let (_client, server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);
    let mut runner_fut = spawn(runner.run());

    let mut incoming_query_fut = spawn(server.0);
    assert_pending!(incoming_query_fut.poll_next());
    assert_pending!(runner_fut.poll());

    incoming_msgs_sink
        .try_send((
            Message {
                transaction_id: vec![234u8, 123u8],
                version: None,
                data: MessageData::Query(
                    PingArgs {
                        id: U160::from([1u8; 20]),
                    }
                    .into(),
                ),
            },
            IP,
        ))
        .unwrap();
    assert_pending!(runner_fut.poll());
    let incoming_query = assert_ready!(incoming_query_fut.poll_next());
    let incoming_ping = match incoming_query {
        Some(IncomingQuery::Ping(ping)) => ping,
        Some(_) => panic!("incoming message has wrong type"),
        None => panic!("channel closed"),
    };
    assert_eq!(incoming_ping.args().id, U160::from([1u8; 20]));

    drop(outgoing_msgs_source);
    drop(incoming_msgs_sink);
    assert_ready!(runner_fut.poll());
    let error = incoming_ping
        .respond(PingResponse {
            id: U160::from([2u8; 20]),
        })
        .unwrap_err();
    assert!(matches!(error, Error::ChannelClosed));
}

#[test]
fn test_router_prioritizes_outgoing_messages() {
    let (outgoing_msgs_sink, mut outgoing_msgs_source) = mpsc::channel(1);
    let (incoming_msgs_sink, incoming_msgs_source) = mpsc::channel(1);
    let (client, server, runner) = setup_routing(outgoing_msgs_sink, incoming_msgs_source);

    let mut runner_fut = spawn(runner.run());
    let mut incoming_query_fut = spawn(server.0);

    // enqueue incoming ping
    incoming_msgs_sink
        .try_send((
            Message {
                transaction_id: vec![234u8, 123u8],
                version: None,
                data: MessageData::Query(
                    PingArgs {
                        id: U160::from([1u8; 20]),
                    }
                    .into(),
                ),
            },
            IP,
        ))
        .unwrap();
    assert_pending!(incoming_query_fut.poll_next());

    // start outgoing ping
    let mut ping_fut = spawn(client.ping(
        IP,
        PingArgs {
            id: U160::from([1u8; 20]),
        },
    ));
    assert_pending!(ping_fut.poll());

    // poll router once
    assert_pending!(runner_fut.poll());

    // verify outgoing ping and no incoming ping
    let (outgoing_ping, _) = outgoing_msgs_source.try_recv().unwrap();
    assert_eq!(outgoing_ping.transaction_id, tid(1));
    assert_eq!(outgoing_ping.version, None);
    if let MessageData::Query(QueryMsg::Ping(args)) = outgoing_ping.data {
        assert_eq!(args.id, U160::from([1u8; 20]));
    } else {
        panic!("outgoing message has incorrect type");
    }
    assert_pending!(incoming_query_fut.poll_next());

    // poll router once more
    assert_pending!(runner_fut.poll());

    // verify incoming ping
    let incoming_query = assert_ready!(incoming_query_fut.poll_next());
    let incoming_ping = match incoming_query {
        Some(IncomingQuery::Ping(ping)) => ping,
        Some(_) => panic!("incoming message has wrong type"),
        None => panic!("channel closed"),
    };
    assert_eq!(incoming_ping.args().id, U160::from([1u8; 20]));
}
