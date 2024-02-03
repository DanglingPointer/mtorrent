use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

#[derive(Clone)]
pub struct DownloadState {
    pub am_interested: bool,
    pub peer_choking: bool,
    pub bytes_received: usize,
}

impl Default for DownloadState {
    fn default() -> Self {
        Self {
            am_interested: false,
            peer_choking: true,
            bytes_received: 0,
        }
    }
}

#[derive(Clone)]
pub struct UploadState {
    pub am_choking: bool,
    pub peer_interested: bool,
    pub bytes_sent: usize,
}

impl Default for UploadState {
    fn default() -> Self {
        Self {
            am_choking: true,
            peer_interested: false,
            bytes_sent: 0,
        }
    }
}

#[derive(Default)]
pub struct PeerStates {
    peers: HashMap<SocketAddr, (DownloadState, UploadState)>,
    seeders: HashSet<SocketAddr>,
    leeches: HashSet<SocketAddr>,
}

impl PeerStates {
    pub fn update_download(&mut self, remote_ip: &SocketAddr, new_state: &DownloadState) {
        let (state, _) = self.peers.entry(*remote_ip).or_default();
        *state = new_state.clone();
        if state.am_interested && !state.peer_choking {
            self.seeders.insert(*remote_ip);
        } else {
            self.seeders.remove(remote_ip);
        }
    }

    pub fn update_upload(&mut self, remote_ip: &SocketAddr, new_state: &UploadState) {
        let (_, state) = self.peers.entry(*remote_ip).or_default();
        *state = new_state.clone();
        if state.peer_interested && !state.am_choking {
            self.leeches.insert(*remote_ip);
        } else {
            self.leeches.remove(remote_ip);
        }
    }

    pub fn remove_peer(&mut self, remote_ip: &SocketAddr) {
        self.peers.remove(remote_ip);
        self.seeders.remove(remote_ip);
        self.leeches.remove(remote_ip);
    }

    pub fn has_peer(&self, remote_ip: &SocketAddr) -> bool {
        self.peers.contains_key(remote_ip)
    }

    pub fn seeders_count(&self) -> usize {
        self.seeders.len()
    }

    pub fn leeches_count(&self) -> usize {
        self.leeches.len()
    }
}
