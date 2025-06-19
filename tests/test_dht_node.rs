use local_async_utils::sec;
use serde::Deserialize;
use std::{fs, process};

#[derive(Deserialize)]
struct ConfigContent {
    local_id: String,
    nodes: Vec<String>,
}

#[test]
fn test_bootstrap_dht_node() {
    let config_file = ".mtorrent_dht";
    assert!(matches!(fs::exists(config_file), Ok(false)));

    let mut dht_node = process::Command::new(env!("CARGO_BIN_EXE_dht_node"))
        .arg("--duration=10")
        .spawn()
        .expect("failed to launch DHT node");

    std::thread::sleep(sec!(11));
    if let Err(e) = dht_node.try_wait() {
        dht_node.kill().unwrap();
        panic!("DHT node didn't exit in time: {e}");
    }

    let config_str = fs::read_to_string(config_file).unwrap();
    let content: ConfigContent = serde_json::from_str(&config_str).unwrap();
    assert_eq!(content.local_id.len(), 40);
    assert!(content.nodes.len() > 3, "only {} nodes in config", content.nodes.len());
    fs::remove_file(config_file).unwrap();
}
