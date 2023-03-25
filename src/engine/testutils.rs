use super::{listeners, Context, State};
use crate::data::{BlockAccountant, PieceInfo, PieceTracker};
use crate::pwp::{DownloaderMessage, UploaderMessage};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::{collections::VecDeque, iter, rc::Rc};

pub struct FakeDownloadMonitor {
    pub am_interested: Cell<bool>,
    pub peer_choking: Cell<bool>,
    pub bytes_received: Cell<usize>,
    pub submitted_msgs: RefCell<VecDeque<DownloaderMessage>>,
}

impl Default for FakeDownloadMonitor {
    fn default() -> Self {
        Self {
            am_interested: Cell::new(false),
            peer_choking: Cell::new(true),
            bytes_received: Cell::new(0),
            submitted_msgs: Default::default(),
        }
    }
}

impl listeners::DownloadChannelMonitor for FakeDownloadMonitor {
    fn am_interested(&self) -> bool {
        self.am_interested.get()
    }

    fn peer_choking(&self) -> bool {
        self.peer_choking.get()
    }

    fn remote_availability(&self) -> &BlockAccountant {
        todo!()
    }

    fn bytes_received(&self) -> usize {
        self.bytes_received.get()
    }

    fn submit_outbound(&self, msg: DownloaderMessage) {
        self.submitted_msgs.borrow_mut().push_back(msg);
    }
}

pub struct FakeUploadMonitor {
    pub peer_interested: Cell<bool>,
    pub am_choking: Cell<bool>,
    pub bytes_sent: Cell<usize>,
    pub submitted_msgs: RefCell<VecDeque<UploaderMessage>>,
}

impl Default for FakeUploadMonitor {
    fn default() -> Self {
        Self {
            peer_interested: Cell::new(false),
            am_choking: Cell::new(true),
            bytes_sent: Cell::new(0),
            submitted_msgs: Default::default(),
        }
    }
}

impl listeners::UploadChannelMonitor for FakeUploadMonitor {
    fn peer_interested(&self) -> bool {
        self.peer_interested.get()
    }

    fn am_choking(&self) -> bool {
        self.am_choking.get()
    }

    fn bytes_sent(&self) -> usize {
        self.bytes_sent.get()
    }

    fn submit_outbound(&self, msg: UploaderMessage) {
        self.submitted_msgs.borrow_mut().push_back(msg)
    }
}

#[derive(Default)]
pub struct FakeMonitorOwner {
    pub monitors: BTreeMap<SocketAddr, (FakeDownloadMonitor, FakeUploadMonitor)>,
    next_port: u16,
}

impl FakeMonitorOwner {
    pub fn add_peer(&mut self) -> (SocketAddr, &FakeDownloadMonitor, &FakeUploadMonitor) {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, self.next_port));
        self.next_port += 1;
        assert!(self
            .monitors
            .insert(addr, (FakeDownloadMonitor::default(), FakeUploadMonitor::default()))
            .is_none());
        let (dm, um) = self.monitors.get(&addr).unwrap();
        (addr, dm, um)
    }
}

impl listeners::MonitorOwner for FakeMonitorOwner {
    fn download_monitor(
        &self,
        remote_ip: &SocketAddr,
    ) -> Option<&dyn listeners::DownloadChannelMonitor> {
        let (download, _upload) = self.monitors.get(remote_ip)?;
        Some(download)
    }

    fn upload_monitor(
        &self,
        remote_ip: &SocketAddr,
    ) -> Option<&dyn listeners::UploadChannelMonitor> {
        let (_download, upload) = self.monitors.get(remote_ip)?;
        Some(upload)
    }

    fn all_download_monitors(
        &self,
    ) -> Box<dyn Iterator<Item = (&SocketAddr, &dyn listeners::DownloadChannelMonitor)> + '_> {
        Box::new(self.monitors.iter().map(|(addr, (download, _upload))| {
            (addr, download as &dyn listeners::DownloadChannelMonitor)
        }))
    }

    fn all_upload_monitors(
        &self,
    ) -> Box<dyn Iterator<Item = (&SocketAddr, &dyn listeners::UploadChannelMonitor)> + '_> {
        Box::new(self.monitors.iter().map(|(addr, (_download, upload))| {
            (addr, upload as &dyn listeners::UploadChannelMonitor)
        }))
    }

    fn all_monitors(
        &self,
    ) -> Box<
        dyn Iterator<
                Item = (
                    &dyn listeners::DownloadChannelMonitor,
                    &dyn listeners::UploadChannelMonitor,
                ),
            > + '_,
    > {
        fn to_dyn_ref_pair(
            val: &(FakeDownloadMonitor, FakeUploadMonitor),
        ) -> (&dyn listeners::DownloadChannelMonitor, &dyn listeners::UploadChannelMonitor)
        {
            let (download, upload) = val;
            (download, upload)
        }
        Box::new(self.monitors.values().map(to_dyn_ref_pair))
    }
}

pub struct Fixture {
    pub block_accountant: BlockAccountant,
    pub piece_tracker: PieceTracker,
    pub monitor_owner: FakeMonitorOwner,
    pub state: State,
}

impl Fixture {
    pub fn new(piece_count: usize, piece_len: usize) -> Self {
        let piece_info = Rc::new(PieceInfo::new(
            iter::repeat_with(|| [0u8; 20].as_slice()).take(piece_count),
            piece_len,
        ));

        let block_accountant = BlockAccountant::new(piece_info);
        let piece_tracker = PieceTracker::new(piece_count);
        let monitor_owner = FakeMonitorOwner::default();
        let state = State::default();

        Self {
            block_accountant,
            piece_tracker,
            monitor_owner,
            state,
        }
    }

    pub fn ctx(&mut self) -> Context {
        Context {
            local_availability: &self.block_accountant,
            piece_tracker: &self.piece_tracker,
            monitor_owner: &self.monitor_owner,
            state: &mut self.state,
        }
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_peers_stored_in_the_order_of_creation() {
        let mut monitors = FakeMonitorOwner::default();

        let mut ips = Vec::new();
        for _ in 0..50 {
            let (ip, _dm, _um) = monitors.add_peer();
            ips.push(ip);
        }

        for (i, (ip, (_, _))) in monitors.monitors.iter().enumerate() {
            assert_eq!(&ips[i], ip);
        }
    }
}
