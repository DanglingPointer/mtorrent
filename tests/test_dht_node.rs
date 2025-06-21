use local_async_utils::{millisec, sec};
use serde::Deserialize;
use std::{fs, path::Path, process};

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
        .arg("--duration=10")
        .arg("--no-upnp")
        .spawn()
        .expect("failed to launch DHT node");

    std::thread::sleep(sec!(10));
    let mut exit_status = None;
    for _ in 0..10 {
        if let Some(status) = dht_node.try_wait().unwrap() {
            exit_status = Some(status);
            break;
        }
        std::thread::sleep(millisec!(500));
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
    assert!(content.nodes.len() > 3, "only {} nodes in config", content.nodes.len());

    fs::remove_dir_all(working_dir).unwrap();
}
