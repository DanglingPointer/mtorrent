use mtorrent_core::pwp::{PeerOrigin, PeerState};
use serde::Serialize;
use std::{collections::BTreeMap, fmt, net::SocketAddr};

pub trait StateListener {
    fn on_snapshot(&mut self, snapshot: StateSnapshot<'_>);
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateSnapshot<'s> {
    pub peers: BTreeMap<SocketAddr, &'s PeerState>,
    pub pieces: PiecesSnapshot,
    pub bytes: BytesSnapshot,
    pub requests: RequestsSnapshot,
    pub metainfo: MetainfoSnapshot,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PiecesSnapshot {
    pub total: usize,
    pub downloaded: usize,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BytesSnapshot {
    pub total: usize,
    pub downloaded: usize,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestsSnapshot {
    pub in_flight: usize,
    pub distinct_pieces: usize,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetainfoSnapshot {
    pub total_pieces: usize,
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
