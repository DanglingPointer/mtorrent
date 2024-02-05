use core::fmt;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

#[derive(Clone)]
#[cfg_attr(test, derive(PartialEq, Eq, Debug))]
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

impl fmt::Display for DownloadState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "am_interested={:<5} peer_choking={:<5} bytes_recv={}",
            self.am_interested, self.peer_choking, self.bytes_received
        )?;
        Ok(())
    }
}

#[derive(Clone)]
#[cfg_attr(test, derive(PartialEq, Eq, Debug))]
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

impl fmt::Display for UploadState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "peer_interested={:<5} am_choking={:<5} bytes_sent={}",
            self.peer_interested, self.am_choking, self.bytes_sent
        )?;
        Ok(())
    }
}

#[derive(Default)]
pub struct PeerStates {
    peers: HashMap<SocketAddr, (DownloadState, UploadState)>,
    seeders: HashSet<SocketAddr>,
    leeches: HashSet<SocketAddr>,
    previously_uploaded_bytes: usize,
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
        if let Some((_download, upload)) = self.peers.get(remote_ip) {
            self.previously_uploaded_bytes += upload.bytes_sent;
            self.peers.remove(remote_ip);
            self.seeders.remove(remote_ip);
            self.leeches.remove(remote_ip);
        }
    }

    pub fn get(&self, peer_ip: &SocketAddr) -> Option<(&DownloadState, &UploadState)> {
        self.peers.get(peer_ip).map(|(download, upload)| (download, upload))
    }

    pub fn seeders_count(&self) -> usize {
        self.seeders.len()
    }

    pub fn leeches_count(&self) -> usize {
        self.leeches.len()
    }

    pub fn all(&self) -> impl Iterator<Item = (&SocketAddr, &DownloadState, &UploadState)> {
        self.peers.iter().map(|(addr, (download, upload))| (addr, download, upload))
    }

    pub fn uploaded_bytes(&self) -> usize {
        self.previously_uploaded_bytes
            + self.peers.values().map(|(_, upload)| upload.bytes_sent).sum::<usize>()
    }
}

impl fmt::Display for PeerStates {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Connected peers ({}):", self.peers.len())?;
        for (ip, (download, upload)) in &self.peers {
            write!(f, "\n[{:<21}]: {}\n{:<24} {}", ip, download, " ", upload)?;
        }
        Ok(())
    }
}
