use local_async_utils::sec;
use mtorrent::app::dht;
use mtorrent_dht::Command;
use serde::Deserialize;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::str::FromStr;
use std::time::Instant;
use std::{fs, process};
use tokio::sync::mpsc;

#[derive(Deserialize)]
struct ConfigContent {
    local_id: String,
    nodes: Vec<String>,
}

#[test]
fn test_bootstrap_dht_node() {
    let working_dir = "test_bootstrap_dht_node";
    fs::create_dir_all(working_dir).unwrap();

    let config_file = Path::new(working_dir).join(".mtorrent_dht");
    assert!(matches!(fs::exists(&config_file), Ok(false)));

    let mut dht_node = process::Command::new(env!("CARGO_BIN_EXE_dht_node"))
        .current_dir(working_dir)
        .arg("--duration=5")
        .arg("--no-upnp")
        .spawn()
        .expect("failed to launch DHT node");

    std::thread::sleep(sec!(5));
    let mut exit_status = None;
    for _ in 0..10 {
        if let Some(status) = dht_node.try_wait().unwrap() {
            exit_status = Some(status);
            break;
        }
        std::thread::sleep(sec!(1));
    }
    match exit_status {
        None => {
            dht_node.kill().unwrap();
            panic!("DHT node never exited");
        }
        Some(status) => {
            if !status.success() {
                panic!("DHT node exited with error code: {:?}", status.code());
            }
        }
    }

    let config_str = fs::read_to_string(config_file).unwrap();
    let content: ConfigContent = serde_json::from_str(&config_str).unwrap();
    assert_eq!(content.local_id.len(), 40);
    assert!(content.nodes.len() > 6, "Only {} nodes in config", content.nodes.len());

    fs::remove_dir_all(working_dir).unwrap();
}

#[test]
fn test_two_dht_nodes_discover_and_announce() {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Trace)
        .init()
        .unwrap();

    let working_dir1 = Path::new("test_two_dht_nodes_1");
    let working_dir2 = Path::new("test_two_dht_nodes_2");
    fs::create_dir_all(working_dir1).unwrap();
    fs::create_dir_all(working_dir2).unwrap();

    let node1_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 50193).into();
    let node2_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 50194).into();

    let (node1_worker, node1_cmds) = dht::launch_dht_node_runtime(dht::Config {
        local_port: node1_addr.port(),
        max_concurrent_queries: None,
        config_dir: working_dir1.to_owned(),
        use_upnp: false,
        bootstrap_nodes_override: Some(Vec::new()),
    })
    .unwrap();

    let (node2_worker, node2_cmds) = dht::launch_dht_node_runtime(dht::Config {
        local_port: node2_addr.port(),
        max_concurrent_queries: None,
        config_dir: working_dir2.to_owned(),
        use_upnp: false,
        bootstrap_nodes_override: Some(Vec::new()),
    })
    .unwrap();

    node1_cmds.blocking_send(Command::AddNode { addr: node2_addr }).unwrap();

    node2_cmds.blocking_send(Command::AddNode { addr: node1_addr }).unwrap();

    std::thread::sleep(sec!(2));

    let (result_sender, mut result_receiver1) = mpsc::channel(10);
    node1_cmds
        .blocking_send(Command::FindPeers {
            info_hash: [0u8; 20],
            callback: result_sender,
            local_peer_port: node1_addr.port(),
        })
        .unwrap();

    let (result_sender, mut result_receiver2) = mpsc::channel(10);
    node2_cmds
        .blocking_send(Command::FindPeers {
            info_hash: [0u8; 20],
            callback: result_sender,
            local_peer_port: node2_addr.port(),
        })
        .unwrap();

    let mut discovered_by_node1 = vec![];
    let mut discovered_by_node2 = vec![];

    let start_time = Instant::now();
    loop {
        while let Ok(peer_addr) = result_receiver1.try_recv() {
            discovered_by_node1.push(peer_addr);
        }
        while let Ok(peer_addr) = result_receiver2.try_recv() {
            discovered_by_node2.push(peer_addr);
        }
        if !discovered_by_node1.is_empty() && !discovered_by_node2.is_empty() {
            break;
        }
        if start_time.elapsed() > sec!(5) {
            panic!("Timeout waiting for nodes to discover each other");
        }
        std::thread::sleep(sec!(1));
    }

    assert!(discovered_by_node1.contains(&node2_addr), "Node 1 did not discover Node 2");
    assert!(discovered_by_node2.contains(&node1_addr), "Node 2 did not discover Node 1");

    drop(node1_worker);
    drop(node2_worker);

    let config1_str = fs::read_to_string(working_dir1.join(".mtorrent_dht")).unwrap();
    let content1: ConfigContent = serde_json::from_str(&config1_str).unwrap();
    assert_eq!(content1.local_id.len(), 40);
    assert_eq!(
        SocketAddr::from_str(&content1.nodes[0]).unwrap(),
        node2_addr,
        "{:?}",
        content1.nodes
    );

    let config2_str = fs::read_to_string(working_dir2.join(".mtorrent_dht")).unwrap();
    let content2: ConfigContent = serde_json::from_str(&config2_str).unwrap();
    assert_eq!(content2.local_id.len(), 40);
    assert_eq!(
        SocketAddr::from_str(&content2.nodes[0]).unwrap(),
        node1_addr,
        "{:?}",
        content2.nodes
    );

    fs::remove_dir_all(working_dir1).unwrap();
    fs::remove_dir_all(working_dir2).unwrap();
}
