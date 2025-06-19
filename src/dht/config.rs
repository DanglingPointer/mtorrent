use super::U160;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::{fs, io};

const FILENAME: &str = ".mtorrent_dht";

const DEFAULT_NODES: [&str; 2] = ["dht.transmissionbt.com:6881", "router.bittorrent.com:6881"];

#[derive(Serialize, Deserialize)]
pub struct Config {
    #[serde(
        serialize_with = "serialize_node_id",
        deserialize_with = "deserialize_node_id"
    )]
    pub(super) local_id: U160,
    pub(super) nodes: Vec<String>,
}

impl Config {
    pub fn load(config_dir: impl AsRef<Path>) -> io::Result<Self> {
        let filepath = config_dir.as_ref().join(FILENAME);
        let file = fs::File::open(filepath)?;
        let reader = BufReader::new(file);
        Ok(serde_json::from_reader(reader)?)
    }

    pub fn save(&self, config_dir: impl AsRef<Path>) -> io::Result<()> {
        let filepath = config_dir.as_ref().join(FILENAME);
        let file = fs::File::create(filepath)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, self)?;
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            local_id: U160::from(rand::random::<[u8; 20]>()),
            nodes: DEFAULT_NODES.into_iter().map(ToOwned::to_owned).collect(),
        }
    }
}

fn serialize_node_id<S>(id: &U160, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    id.to_string().serialize(s)
}

fn deserialize_node_id<'de, D>(deserializer: D) -> Result<U160, D::Error>
where
    D: Deserializer<'de>,
{
    let hex_str = String::deserialize(deserializer)?;
    hex_str.parse().map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_config() {
        let cfg = Config {
            local_id: [0x0f; 20].into(),
            nodes: [
                "dht.transmissionbt.com:6881".to_string(),
                "router.bittorrent.com:6881".to_string(),
            ]
            .into(),
        };

        let serialized = serde_json::to_string(&cfg).unwrap();
        assert_eq!(
            serialized,
            r#"{"local_id":"0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f","nodes":["dht.transmissionbt.com:6881","router.bittorrent.com:6881"]}"#
        );
    }

    #[test]
    fn test_deserialize_config() {
        let cfg = r#"{
            "local_id": "afafafafafafafafafafafafafafafafafafafaf",
            "nodes": [
                "dht.transmissionbt.com:6881",
                "router.bittorrent.com:6881"
            ]
        }"#;
        let deserialized: Config = serde_json::from_str(cfg).unwrap();
        assert_eq!(deserialized.local_id, [0xaf; 20].into());
        assert_eq!(
            deserialized.nodes,
            vec![
                "dht.transmissionbt.com:6881".to_string(),
                "router.bittorrent.com:6881".to_string(),
            ]
        );
    }

    #[test]
    fn test_write_config_file() {
        let dir = "dht_test_write_config_file";
        fs::create_dir_all(dir).unwrap();

        let cfg = Config {
            local_id: U160::from([0xaf; 20]),
            ..Default::default()
        };
        cfg.save(dir).unwrap();

        let content = fs::read(Path::new(dir).join(FILENAME)).unwrap();
        let content = String::from_utf8(content).unwrap();
        fs::remove_dir_all(dir).unwrap();

        assert_eq!(
            content,
            r#"{
  "local_id": "afafafafafafafafafafafafafafafafafafafaf",
  "nodes": [
    "dht.transmissionbt.com:6881",
    "router.bittorrent.com:6881"
  ]
}"#,
        );
    }

    #[test]
    fn test_read_config_file() {
        let dir = "dht_test_read_config_file";
        fs::create_dir_all(dir).unwrap();

        let content = r#"{"local_id":"dededededededededededededededededededede",
        "nodes":["dht.transmissionbt.com:6666","router.bittorrent.com:6666"]}"#;
        fs::write(Path::new(dir).join(FILENAME), content).unwrap();

        let cfg = Config::load(dir).unwrap();
        fs::remove_dir_all(dir).unwrap();

        assert_eq!(cfg.local_id, [0xde; 20].into());
        assert_eq!(
            cfg.nodes,
            vec![
                "dht.transmissionbt.com:6666".to_string(),
                "router.bittorrent.com:6666".to_string(),
            ]
        );
    }

    #[test]
    fn test_write_and_read_config_file() {
        let dir = "dht_test_write_and_read_config_file";
        fs::create_dir_all(dir).unwrap();

        let cfg1 = Config::default();
        cfg1.save(dir).unwrap();
        let cfg2 = Config::load(dir).unwrap();
        fs::remove_dir_all(dir).unwrap();

        assert_eq!(cfg2.local_id, cfg1.local_id);
        assert_eq!(cfg2.nodes, cfg1.nodes);
    }
}
