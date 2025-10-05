use mtorrent_core::pwp::{PeerOrigin, PeerState};
use serde::Serialize;
use std::{collections::BTreeMap, fmt, net::SocketAddr, ops::ControlFlow, time::Duration};

/// Listener for monitoring progress of a single torrent download.
pub trait StateListener {
    /// Interval of periodic state snapshots.
    const INTERVAL: Duration;

    /// Callback invoked once every [`StateListener::INTERVAL`] sec. The download will stop if it returns [`ControlFlow::Break`].
    fn on_snapshot(&mut self, snapshot: StateSnapshot<'_>) -> ControlFlow<()>;
}

/// Snapshot of the current state of the download.
#[derive(Serialize)]
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
#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PiecesSnapshot {
    /// Total number of pieces in the torrent.
    pub total: usize,
    /// Number of pieces that have been downloaded.
    pub downloaded: usize,
}

/// Part of the periodic state snapshot related to data of the torrent.
#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BytesSnapshot {
    /// Total number of bytes in the torrent.
    pub total: usize,
    /// Number of bytes that have been downloaded.
    pub downloaded: usize,
}

/// Part of the periodic state snapshot related to outstanding requests.
#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestsSnapshot {
    /// Total number of piece requests in-flight sent to peers.
    pub in_flight: usize,
    /// Number of distinct pieces being requested.
    pub distinct_pieces: usize,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetainfoSnapshot {
    /// Total number of 16KiB pieces the metainfo file is divided into.
    pub total_pieces: usize,
    /// Number of metainfo file pieces that have been downloaded.
    pub downloaded_pieces: usize,
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
            write!(f, "\n[ {:^21} ]     origin: {:<12}", addr, origin_str(state.origin))?;
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
