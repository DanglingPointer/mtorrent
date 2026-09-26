use mtorrent_base::pwp::{Bitfield, PeerOrigin, PeerState, TransportProto};
use serde::{Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::time::Duration;

/// Listener for monitoring progress of a single torrent download.
pub trait StateListener {
    /// Interval of periodic state snapshots.
    const INTERVAL: Duration;

    /// Callback invoked once every [`StateListener::INTERVAL`] sec. The download will stop if it
    /// returns [`ControlFlow::Break`].
    fn on_snapshot(&mut self, snapshot: StateSnapshot<'_>) -> ControlFlow<()>;
}

impl<L: StateListener> StateListener for &mut L {
    const INTERVAL: Duration = L::INTERVAL;

    fn on_snapshot(&mut self, snapshot: StateSnapshot<'_>) -> ControlFlow<()> {
        L::on_snapshot(*self, snapshot)
    }
}

/// Snapshot of the current state of the download.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateSnapshot<'s> {
    /// Connected peers.
    pub peers: BTreeMap<SocketAddr, &'s PeerState>,
    /// Pieces of the torrent.
    pub pieces: PiecesSnapshot,
    /// Data of the torrent.
    pub bytes: BytesSnapshot,
    /// Outstanding requests.
    pub requests: RequestsSnapshot,
    /// Metainfo file information.
    pub metainfo: MetainfoSnapshot,
}

/// Part of the periodic state snapshot related to pieces of the torrent.
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PiecesSnapshot {
    /// Total number of pieces in the torrent.
    pub total: usize,
    /// Number of pieces that have been downloaded.
    pub downloaded: usize,
    /// Downloaded pieces represented as an array of zeroes and ones.
    #[serde(serialize_with = "serialize_bitfield")]
    pub bitfield: Bitfield,
}

/// Part of the periodic state snapshot related to data of the torrent.
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BytesSnapshot {
    /// Total number of bytes in the torrent.
    pub total: usize,
    /// Number of bytes that have been downloaded.
    pub downloaded: usize,
}

/// Part of the periodic state snapshot related to outstanding requests.
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestsSnapshot {
    /// Total number of piece requests in-flight sent to peers.
    pub in_flight: usize,
    /// Number of distinct pieces being requested.
    pub distinct_pieces: usize,
}

/// Part of the periodic state snapshot related to downloading torrent metainfo.
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetainfoSnapshot {
    /// Total number of 16KiB pieces the metainfo file is divided into.
    pub total_pieces: usize,
    /// Number of metainfo file pieces that have been downloaded.
    pub downloaded_pieces: usize,
}

fn serialize_bitfield<S>(bitfield: &Bitfield, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let bitfield = bitfield.iter().map(|bit| if *bit { '1' } else { '0' }).collect::<String>();
    serializer.serialize_str(&bitfield)
}

impl fmt::Display for StateSnapshot<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn origin_str(origin: PeerOrigin) -> &'static str {
            match origin {
                PeerOrigin::Tracker => "tracker",
                PeerOrigin::Listener => "🫧listener🫧",
                PeerOrigin::Pex => "✨pex✨",
                PeerOrigin::Dht => "💎dht💎",
                PeerOrigin::Other => "other",
            }
        }
        fn proto_str(transport: Option<TransportProto>) -> &'static str {
            match transport {
                Some(TransportProto::Utp) => "uTP",
                Some(TransportProto::Tcp) => "TCP",
                None => "n/a",
            }
        }
        writeln!(
            f,
            "Local availability: pieces={}/{} bytes={}/{} metainfo={}/{}",
            self.pieces.downloaded,
            self.pieces.total,
            self.bytes.downloaded,
            self.bytes.total,
            self.metainfo.downloaded_pieces,
            self.metainfo.total_pieces
        )?;
        writeln!(
            f,
            "Outstanding requests: pieces/requests={}/{}",
            self.requests.distinct_pieces, self.requests.in_flight
        )?;
        write!(f, "Connected peers ({}):", self.peers.len())?;
        for (addr, state) in &self.peers {
            write!(
                f,
                "\n[ {:^21} ]     origin: {:<12} encrypted: {:<8} proto: {:<8}",
                addr,
                origin_str(state.origin),
                if state.encryption { "🔒" } else { "❌" },
                proto_str(state.transport)
            )?;
            if let Some(hs) = &state.extensions {
                write!(
                    f,
                    "client: {:<20} reqq: {}",
                    hs.client_type.as_deref().unwrap_or("n/a"),
                    hs.request_limit.unwrap_or_default()
                )?;
            }
            write!(f, "\n{} {}", state.download, state.upload)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn serialize_pieces_snapshot_bitfield_as_string() {
        let snapshot = PiecesSnapshot {
            total: 5,
            downloaded: 3,
            bitfield: Bitfield::from_iter([true, false, true, true, false]),
        };

        assert_eq!(
            serde_json::to_value(snapshot).unwrap(),
            json!({
                "total": 5,
                "downloaded": 3,
                "bitfield": "10110",
            })
        );
    }

    #[test]
    fn serialize_empty_pieces_snapshot_bitfield_as_empty_string() {
        assert_eq!(
            serde_json::to_value(PiecesSnapshot::default()).unwrap(),
            json!({
                "total": 0,
                "downloaded": 0,
                "bitfield": "",
            })
        );
    }
}
