use core::fmt;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use tokio::time::Instant;

#[derive(Default, Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum PeerOrigin {
    Tracker,
    Listener,
    Pex,
    Dht,
    #[default]
    Other,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DownloadState {
    pub am_interested: bool,
    pub peer_choking: bool,
    pub bytes_received: usize,
    pub last_bitrate_bps: usize,
}

impl Default for DownloadState {
    fn default() -> Self {
        Self {
            am_interested: false,
            peer_choking: true,
            bytes_received: 0,
            last_bitrate_bps: 0,
        }
    }
}

impl fmt::Display for DownloadState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "am_interested={:<5} peer_choking={:<5} rx_bps={:<8} bytes_recv={:<12}",
            self.am_interested, self.peer_choking, self.last_bitrate_bps, self.bytes_received
        )?;
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct UploadState {
    pub am_choking: bool,
    pub peer_interested: bool,
    pub bytes_sent: usize,
    pub last_bitrate_bps: usize,
}

impl Default for UploadState {
    fn default() -> Self {
        Self {
            am_choking: true,
            peer_interested: false,
            bytes_sent: 0,
            last_bitrate_bps: 0,
        }
    }
}

impl fmt::Display for UploadState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "peer_interested={:<5} am_choking={:<5} tx_bps={:<8} bytes_sent={:<12}",
            self.peer_interested, self.am_choking, self.last_bitrate_bps, self.bytes_sent
        )?;
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PeerState {
    pub download: DownloadState,
    pub upload: UploadState,
    pub extensions: Option<Box<super::ExtendedHandshake>>,
    pub origin: PeerOrigin,
    pub last_download_time: Instant,
    pub last_upload_time: Instant,
}

impl Default for PeerState {
    fn default() -> Self {
        Self {
            download: Default::default(),
            upload: Default::default(),
            extensions: None,
            origin: Default::default(),
            last_download_time: Instant::now(),
            last_upload_time: Instant::now(),
        }
    }
}

#[derive(Default, Debug)]
pub struct PeerStates {
    peers: HashMap<SocketAddr, PeerState>,
    seeders: HashSet<SocketAddr>,
    leeches: HashSet<SocketAddr>,
    previously_uploaded_bytes: usize,
}

impl PeerStates {
    pub fn update_download(&mut self, remote_ip: &SocketAddr, new_state: &DownloadState) {
        let state = self.peers.entry(*remote_ip).or_default();
        if new_state.bytes_received > state.download.bytes_received {
            state.last_download_time = Instant::now();
        }
        state.download = new_state.clone();
        if state.download.am_interested && !state.download.peer_choking {
            self.seeders.insert(*remote_ip);
        } else {
            self.seeders.remove(remote_ip);
        }
    }

    pub fn update_upload(&mut self, remote_ip: &SocketAddr, new_state: &UploadState) {
        let state = self.peers.entry(*remote_ip).or_default();
        if new_state.bytes_sent > state.upload.bytes_sent {
            state.last_upload_time = Instant::now();
        }
        state.upload = new_state.clone();
        if state.upload.peer_interested && !state.upload.am_choking {
            self.leeches.insert(*remote_ip);
        } else {
            self.leeches.remove(remote_ip);
        }
    }

    pub fn set_extended_handshake(
        &mut self,
        remote_ip: &SocketAddr,
        extended_handshake: Box<super::ExtendedHandshake>,
    ) {
        let state = self.peers.entry(*remote_ip).or_default();
        if state.extensions.is_none() {
            state.extensions = Some(extended_handshake);
        }
    }

    pub fn set_origin(&mut self, remote_ip: &SocketAddr, origin: PeerOrigin) {
        let state = self.peers.entry(*remote_ip).or_default();
        state.origin = origin;
    }

    pub fn remove_peer(&mut self, remote_ip: &SocketAddr) {
        if let Some(state) = self.peers.get(remote_ip) {
            self.previously_uploaded_bytes += state.upload.bytes_sent;
            self.peers.remove(remote_ip);
            self.seeders.remove(remote_ip);
            self.leeches.remove(remote_ip);
        }
    }

    pub fn get(&self, peer_ip: &SocketAddr) -> Option<&PeerState> {
        self.peers.get(peer_ip)
    }

    pub fn seeders_count(&self) -> usize {
        self.seeders.len()
    }

    pub fn leeches_count(&self) -> usize {
        self.leeches.len()
    }

    pub fn leeches(&self) -> &HashSet<SocketAddr> {
        &self.leeches
    }

    pub fn iter(&self) -> impl Iterator<Item = (&SocketAddr, &PeerState)> {
        self.peers.iter()
    }

    pub fn uploaded_bytes(&self) -> usize {
        self.previously_uploaded_bytes
            + self.peers.values().map(|state| state.upload.bytes_sent).sum::<usize>()
    }
}

impl fmt::Display for PeerStates {
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

        write!(f, "Connected peers ({}):", self.peers.len())?;
        for (ip, state) in &self.peers {
            write!(f, "\n[ {:^21} ]     origin: {:<12}", ip, origin_str(state.origin))?;
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
