use super::handler;
use crate::data::{BlockAccountant, PieceInfo};
use crate::engine;
use crate::pwp::{BlockInfo, DownloaderMessage, UploaderMessage};
use crate::utils::fifo;
use core::fmt;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::rc::Rc;

pub(super) struct PeerManager {
    pieces: Rc<PieceInfo>,
    channel_states: HashMap<SocketAddr, (DownloadChannelState, UploadChannelState)>,
}

impl PeerManager {
    pub(super) fn new(pieces: Rc<PieceInfo>) -> Self {
        Self {
            pieces,
            channel_states: HashMap::new(),
        }
    }

    pub(super) fn add_peer(&mut self, remote_ip: &SocketAddr) {
        let download = DownloadChannelState {
            am_interested: false,
            peer_choking: true,
            availability: BlockAccountant::new(self.pieces.clone()),
            bytes_received: 0,
            pending_tx_msgs: Default::default(),
            requested_blocks: HashSet::new(),
        };
        let upload = UploadChannelState {
            peer_interested: false,
            am_choking: true,
            bytes_sent: 0,
            pending_tx_msgs: Default::default(),
        };
        self.channel_states.insert(*remote_ip, (download, upload));
    }

    pub(super) fn remove_peer(&mut self, remote_ip: &SocketAddr) {
        self.channel_states.remove(remote_ip);
    }
}

impl fmt::Display for PeerManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Connected peers ({}):", self.channel_states.len())?;
        for (ip, (download, upload)) in &self.channel_states {
            write!(f, "\n[{:<21}]: {}\n{:<24} {}", ip, download, " ", upload)?;
        }
        Ok(())
    }
}

impl handler::HandlerOwner for PeerManager {
    fn download_handler(
        &mut self,
        remote_ip: &SocketAddr,
    ) -> Option<&mut dyn super::handler::DownloadChannelHandler> {
        let (download, _upload) = self.channel_states.get_mut(remote_ip)?;
        Some(download)
    }

    fn upload_handler(
        &mut self,
        remote_ip: &SocketAddr,
    ) -> Option<&mut dyn super::handler::UploadChannelHandler> {
        let (_download, upload) = self.channel_states.get_mut(remote_ip)?;
        Some(upload)
    }
}

impl engine::MonitorOwner for PeerManager {
    fn download_monitor(
        &self,
        remote_ip: &SocketAddr,
    ) -> Option<&dyn engine::DownloadChannelMonitor> {
        let (download, _upload) = self.channel_states.get(remote_ip)?;
        Some(download)
    }

    fn upload_monitor(&self, remote_ip: &SocketAddr) -> Option<&dyn engine::UploadChannelMonitor> {
        let (_download, upload) = self.channel_states.get(remote_ip)?;
        Some(upload)
    }

    fn all_download_monitors(
        &self,
    ) -> Box<dyn Iterator<Item = (&SocketAddr, &dyn engine::DownloadChannelMonitor)> + '_> {
        Box::new(self.channel_states.iter().map(|(addr, (download, _upload))| {
            (addr, download as &dyn engine::DownloadChannelMonitor)
        }))
    }

    fn all_upload_monitors(
        &self,
    ) -> Box<dyn Iterator<Item = (&SocketAddr, &dyn engine::UploadChannelMonitor)> + '_> {
        Box::new(
            self.channel_states.iter().map(|(addr, (_download, upload))| {
                (addr, upload as &dyn engine::UploadChannelMonitor)
            }),
        )
    }

    fn all_monitors(
        &self,
    ) -> Box<
        dyn Iterator<
                Item = (&dyn engine::DownloadChannelMonitor, &dyn engine::UploadChannelMonitor),
            > + '_,
    > {
        fn to_dyn_ref_pair(
            val: &(DownloadChannelState, UploadChannelState),
        ) -> (&dyn engine::DownloadChannelMonitor, &dyn engine::UploadChannelMonitor) {
            let (download, upload) = val;
            (download, upload)
        }

        Box::new(self.channel_states.values().map(to_dyn_ref_pair))
    }
}

struct DownloadChannelState {
    am_interested: bool,
    peer_choking: bool,
    availability: BlockAccountant,
    bytes_received: usize,
    pending_tx_msgs: fifo::Queue<DownloaderMessage>,
    requested_blocks: HashSet<BlockInfo>,
}

impl fmt::Display for DownloadChannelState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "am_interested={:<5} peer_choking={:<5} bytes_recv={}",
            self.am_interested, self.peer_choking, self.bytes_received
        )?;
        write!(f, " {}", self.availability)?;

        let pending_msgs_count = self.pending_tx_msgs.len();
        if pending_msgs_count > 0 {
            write!(f, " pending_down_msgs={pending_msgs_count}")?;
        }
        let outstanding_requests = self.requested_blocks.len();
        if outstanding_requests > 0 {
            write!(f, " outstanding_reqs={outstanding_requests}")?;
        }
        Ok(())
    }
}

impl handler::DownloadChannelHandler for DownloadChannelState {
    fn update_state(&mut self, inbound: &UploaderMessage) {
        match inbound {
            UploaderMessage::Unchoke => self.peer_choking = false,
            UploaderMessage::Choke => {
                self.peer_choking = true;
                self.requested_blocks.clear();
            }
            UploaderMessage::Bitfield(bitfield) => {
                let _ = self.availability.submit_bitfield(bitfield);
            }
            UploaderMessage::Have { piece_index } => {
                let _ = self.availability.submit_piece(*piece_index);
            }
            UploaderMessage::Block(block_info, data) => {
                let _ = self.availability.submit_block(block_info);
                self.requested_blocks.remove(block_info);
                self.bytes_received += data.len();
            }
        }
    }

    fn next_outbound(&mut self) -> Option<DownloaderMessage> {
        let msg = self.pending_tx_msgs.pop()?;
        match &msg {
            DownloaderMessage::Request(block) => {
                self.requested_blocks.insert(block.clone());
            }
            DownloaderMessage::Cancel(block) => {
                self.requested_blocks.remove(block);
            }
            DownloaderMessage::NotInterested => {
                self.am_interested = false;
                self.requested_blocks.clear();
            }
            DownloaderMessage::Interested => self.am_interested = true,
        }
        Some(msg)
    }
}

impl engine::DownloadChannelMonitor for DownloadChannelState {
    fn am_interested(&self) -> bool {
        if self.am_interested {
            !self.pending_tx_msgs.contains(&DownloaderMessage::NotInterested)
        } else {
            self.pending_tx_msgs.contains(&DownloaderMessage::Interested)
        }
    }

    fn peer_choking(&self) -> bool {
        self.peer_choking
    }

    fn remote_availability(&self) -> &BlockAccountant {
        &self.availability
    }

    fn bytes_received(&self) -> usize {
        self.bytes_received
    }

    fn submit_outbound(&self, msg: DownloaderMessage) {
        if self.pending_tx_msgs.contains(&msg) {
            return;
        }
        let should_enqueue = match &msg {
            DownloaderMessage::Interested => {
                let contained_notinterested =
                    self.pending_tx_msgs.remove_all(&DownloaderMessage::NotInterested);
                !contained_notinterested
            }
            DownloaderMessage::NotInterested => {
                let contained_interested =
                    self.pending_tx_msgs.remove_all(&DownloaderMessage::Interested);
                !contained_interested
            }
            DownloaderMessage::Request(requested_block) => {
                let contained_cancel = self
                    .pending_tx_msgs
                    .remove_all(&DownloaderMessage::Cancel(requested_block.clone()));
                let already_requested = self.requested_blocks.contains(requested_block);
                !contained_cancel && !already_requested
            }
            DownloaderMessage::Cancel(canceled_block) => {
                let contained_request = self
                    .pending_tx_msgs
                    .remove_all(&DownloaderMessage::Request(canceled_block.clone()));
                !contained_request
            }
        };
        if should_enqueue {
            self.pending_tx_msgs.push(msg);
        }
    }
}

struct UploadChannelState {
    peer_interested: bool,
    am_choking: bool,
    bytes_sent: usize,
    pending_tx_msgs: fifo::Queue<UploaderMessage>,
}

impl fmt::Display for UploadChannelState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "peer_interested={:<5} am_choking={:<5} bytes_sent={}",
            self.peer_interested, self.am_choking, self.bytes_sent
        )?;
        let pending_msgs_count = self.pending_tx_msgs.len();
        if pending_msgs_count > 0 {
            write!(f, " pending_up_msgs={}", pending_msgs_count)?;
        }
        Ok(())
    }
}

impl handler::UploadChannelHandler for UploadChannelState {
    fn update_state(&mut self, inbound: &DownloaderMessage) {
        match inbound {
            DownloaderMessage::NotInterested => self.peer_interested = false,
            DownloaderMessage::Interested => self.peer_interested = true,
            DownloaderMessage::Cancel(block) => {
                self.pending_tx_msgs
                    .remove_if(|m| matches!(m, UploaderMessage::Block(info, _) if info == block));
            }
            DownloaderMessage::Request(_block) => (),
        }
    }

    fn next_outbound(&mut self) -> Option<UploaderMessage> {
        let msg = self.pending_tx_msgs.pop()?;
        match &msg {
            UploaderMessage::Unchoke => self.am_choking = false,
            UploaderMessage::Choke => self.am_choking = true,
            UploaderMessage::Block(_block_info, data) => self.bytes_sent += data.len(),
            UploaderMessage::Have { piece_index: _ } => (),
            UploaderMessage::Bitfield(_bitfield) => (),
        }
        Some(msg)
    }
}

impl engine::UploadChannelMonitor for UploadChannelState {
    fn peer_interested(&self) -> bool {
        self.peer_interested
    }

    fn am_choking(&self) -> bool {
        if self.am_choking {
            !self.pending_tx_msgs.contains(&UploaderMessage::Unchoke)
        } else {
            self.pending_tx_msgs.contains(&UploaderMessage::Choke)
        }
    }

    fn bytes_sent(&self) -> usize {
        self.bytes_sent
    }

    fn submit_outbound(&self, msg: UploaderMessage) {
        if self.pending_tx_msgs.contains(&msg) {
            return;
        }
        let should_enqueue = match &msg {
            UploaderMessage::Choke => {
                let contained_unchoke = self.pending_tx_msgs.remove_all(&UploaderMessage::Unchoke);
                !contained_unchoke
            }
            UploaderMessage::Unchoke => {
                let contained_choke = self.pending_tx_msgs.remove_all(&UploaderMessage::Choke);
                !contained_choke
            }
            _ => true,
        };
        if should_enqueue {
            self.pending_tx_msgs.push(msg);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::handler::HandlerOwner;
    use super::*;
    use crate::engine::MonitorOwner;
    use std::iter;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[test]
    fn test_manager_adds_and_removes_peers() {
        let ip = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666));
        let mut mgr = PeerManager::new(Rc::new(PieceInfo::new(iter::empty(), 3)));

        assert_eq!(0, mgr.all_monitors().count());
        assert_eq!(0, mgr.all_upload_monitors().count());
        assert_eq!(0, mgr.all_download_monitors().count());

        mgr.add_peer(&ip);
        assert_eq!(1, mgr.all_monitors().count());
        assert_eq!(1, mgr.all_upload_monitors().count());
        assert_eq!(1, mgr.all_download_monitors().count());
        assert!(mgr.upload_handler(&ip).is_some());
        assert!(mgr.download_handler(&ip).is_some());
        assert!(mgr.upload_monitor(&ip).is_some());
        assert!(mgr.download_monitor(&ip).is_some());

        mgr.remove_peer(&ip);
        assert_eq!(0, mgr.all_monitors().count());
        assert_eq!(0, mgr.all_upload_monitors().count());
        assert_eq!(0, mgr.all_download_monitors().count());
        assert!(mgr.upload_handler(&ip).is_none());
        assert!(mgr.download_handler(&ip).is_none());
        assert!(mgr.upload_monitor(&ip).is_none());
        assert!(mgr.download_monitor(&ip).is_none());
    }

    fn block() -> BlockInfo {
        BlockInfo {
            piece_index: 123,
            in_piece_offset: 16384,
            block_length: 1024,
        }
    }

    struct Fixture {
        ip: SocketAddr,
        mgr: PeerManager,
    }

    impl Fixture {
        fn new() -> Self {
            let ip = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666));
            let mut mgr = PeerManager::new(Rc::new(PieceInfo::new(iter::empty(), 3)));
            mgr.add_peer(&ip);
            Self { ip, mgr }
        }
        fn dm(&self) -> &dyn engine::DownloadChannelMonitor {
            self.mgr.download_monitor(&self.ip).unwrap()
        }
        fn um(&self) -> &dyn engine::UploadChannelMonitor {
            self.mgr.upload_monitor(&self.ip).unwrap()
        }
        fn dh(&mut self) -> &mut dyn handler::DownloadChannelHandler {
            self.mgr.download_handler(&self.ip).unwrap()
        }
        fn uh(&mut self) -> &mut dyn handler::UploadChannelHandler {
            self.mgr.upload_handler(&self.ip).unwrap()
        }
    }

    #[test]
    fn test_peer_has_correct_initial_state() {
        let ip = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666));
        let mut mgr = PeerManager::new(Rc::new(PieceInfo::new(iter::empty(), 3)));
        mgr.add_peer(&ip);

        let down_mon = mgr.download_monitor(&ip).unwrap();
        assert!(!down_mon.am_interested());
        assert!(down_mon.peer_choking());
        assert_eq!(0, down_mon.bytes_received());

        let up_mon = mgr.upload_monitor(&ip).unwrap();
        assert!(!up_mon.peer_interested());
        assert!(up_mon.am_choking());
        assert_eq!(0, up_mon.bytes_sent());

        let down_hnd = mgr.download_handler(&ip).unwrap();
        assert!(down_hnd.next_outbound().is_none());

        let up_hnd = mgr.upload_handler(&ip).unwrap();
        assert!(up_hnd.next_outbound().is_none());
    }

    #[test]
    fn test_peer_updates_state_on_upload_channel_update() {
        let mut f = Fixture::new();

        f.uh().update_state(&DownloaderMessage::Interested);
        assert!(f.um().peer_interested());

        f.uh().update_state(&DownloaderMessage::NotInterested);
        assert!(!f.um().peer_interested());

        f.um().submit_outbound(UploaderMessage::Unchoke);
        assert!(!f.um().am_choking());
        assert!(matches!(f.uh().next_outbound().unwrap(), UploaderMessage::Unchoke));
        assert!(f.uh().next_outbound().is_none());

        f.um().submit_outbound(UploaderMessage::Choke);
        assert!(f.um().am_choking());
        assert!(matches!(f.uh().next_outbound().unwrap(), UploaderMessage::Choke));
        assert!(f.uh().next_outbound().is_none());

        let block = block();
        f.um().submit_outbound(UploaderMessage::Block(block.clone(), vec![0u8; 1024]));
        assert_eq!(0, f.um().bytes_sent());

        let msg = f.uh().next_outbound().unwrap();
        assert!(matches!(
            msg,
            UploaderMessage::Block(info, data) if info == block && data == vec![0u8; 1024]));
        assert!(f.uh().next_outbound().is_none());
        assert_eq!(1024, f.um().bytes_sent());

        f.um().submit_outbound(UploaderMessage::Block(block.clone(), vec![0u8; 1024]));
        assert_eq!(1024, f.um().bytes_sent());

        let msg = f.uh().next_outbound().unwrap();
        assert!(matches!(
            msg,
            UploaderMessage::Block(info, data) if info == block && data == vec![0u8; 1024]));
        assert!(f.uh().next_outbound().is_none());
        assert_eq!(1024 * 2, f.um().bytes_sent());
    }

    #[test]
    fn test_peer_updates_state_on_download_channel_update() {
        let mut f = Fixture::new();

        f.dh().update_state(&UploaderMessage::Unchoke);
        assert!(!f.dm().peer_choking());

        f.dh().update_state(&UploaderMessage::Choke);
        assert!(f.dm().peer_choking());

        f.dm().submit_outbound(DownloaderMessage::Interested);
        assert!(f.dm().am_interested());
        assert!(matches!(f.dh().next_outbound().unwrap(), DownloaderMessage::Interested));
        assert!(f.dh().next_outbound().is_none());

        f.dm().submit_outbound(DownloaderMessage::NotInterested);
        assert!(!f.dm().am_interested());
        assert!(matches!(f.dh().next_outbound().unwrap(), DownloaderMessage::NotInterested));
        assert!(f.dh().next_outbound().is_none());

        let block = block();
        f.dh().update_state(&UploaderMessage::Block(block.clone(), vec![0u8; 1024]));
        assert_eq!(1024, f.dm().bytes_received());

        f.dh().update_state(&UploaderMessage::Block(block, vec![0u8; 1024]));
        assert_eq!(1024 * 2, f.dm().bytes_received());
    }

    #[test]
    fn test_duplicated_outbound_upload_messages_are_dropped() {
        let mut f = Fixture::new();

        f.um().submit_outbound(UploaderMessage::Unchoke);
        f.um().submit_outbound(UploaderMessage::Unchoke);
        assert!(matches!(f.uh().next_outbound().unwrap(), UploaderMessage::Unchoke));
        assert!(f.uh().next_outbound().is_none());

        let block = block();
        f.um().submit_outbound(UploaderMessage::Block(block.clone(), vec![0u8; 1024]));
        f.um().submit_outbound(UploaderMessage::Block(block.clone(), vec![0u8; 1024]));
        assert!(matches!(
            f.uh().next_outbound().unwrap(),
            UploaderMessage::Block(info, data) if info == block && data == vec![0u8; 1024]));
        assert!(f.uh().next_outbound().is_none());

        f.um().submit_outbound(UploaderMessage::Have { piece_index: 124 });
        f.um().submit_outbound(UploaderMessage::Have { piece_index: 123 });
        f.um().submit_outbound(UploaderMessage::Have { piece_index: 124 });
        assert!(matches!(
            f.uh().next_outbound().unwrap(),
            UploaderMessage::Have { piece_index: 124 }
        ));
        assert!(matches!(
            f.uh().next_outbound().unwrap(),
            UploaderMessage::Have { piece_index: 123 }
        ));
        assert!(f.uh().next_outbound().is_none());
    }

    #[test]
    fn test_duplicated_outbound_download_messages_are_dropped() {
        let mut f = Fixture::new();

        f.dm().submit_outbound(DownloaderMessage::Interested);
        f.dm().submit_outbound(DownloaderMessage::Interested);
        assert!(matches!(f.dh().next_outbound().unwrap(), DownloaderMessage::Interested));
        assert!(f.dh().next_outbound().is_none());

        let block = block();
        f.dm().submit_outbound(DownloaderMessage::Request(block.clone()));
        f.dm().submit_outbound(DownloaderMessage::Request(block.clone()));
        assert!(matches!(
            f.dh().next_outbound().unwrap(),
            DownloaderMessage::Request(info) if info == block));
        assert!(f.dh().next_outbound().is_none());

        let different_block = BlockInfo {
            piece_index: block.piece_index + 1,
            ..block
        };
        f.dm().submit_outbound(DownloaderMessage::Cancel(block.clone()));
        f.dm().submit_outbound(DownloaderMessage::Cancel(different_block.clone()));
        f.dm().submit_outbound(DownloaderMessage::Cancel(block.clone()));
        assert!(matches!(
            f.dh().next_outbound().unwrap(),
            DownloaderMessage::Cancel(info) if info == block));
        assert!(matches!(
            f.dh().next_outbound().unwrap(),
            DownloaderMessage::Cancel(info) if info == different_block));
        assert!(f.dh().next_outbound().is_none());
    }

    #[test]
    fn test_mutually_negating_outbound_upload_messages_are_dropped() {
        let mut f = Fixture::new();

        f.um().submit_outbound(UploaderMessage::Unchoke);
        f.um().submit_outbound(UploaderMessage::Choke);
        assert!(f.uh().next_outbound().is_none());

        f.um().submit_outbound(UploaderMessage::Choke);
        f.um().submit_outbound(UploaderMessage::Unchoke);
        assert!(f.uh().next_outbound().is_none());
    }

    #[test]
    fn test_mutually_negating_outbound_download_messages_are_dropped() {
        let mut f = Fixture::new();

        f.dm().submit_outbound(DownloaderMessage::Interested);
        f.dm().submit_outbound(DownloaderMessage::NotInterested);
        assert!(f.dh().next_outbound().is_none());

        f.dm().submit_outbound(DownloaderMessage::NotInterested);
        f.dm().submit_outbound(DownloaderMessage::Interested);
        assert!(f.dh().next_outbound().is_none());

        let block = block();
        f.dm().submit_outbound(DownloaderMessage::Request(block.clone()));
        f.dm().submit_outbound(DownloaderMessage::Cancel(block.clone()));
        assert!(f.dh().next_outbound().is_none());

        f.dm().submit_outbound(DownloaderMessage::Cancel(block.clone()));
        f.dm().submit_outbound(DownloaderMessage::Request(block));
        assert!(f.dh().next_outbound().is_none());
    }

    #[test]
    fn test_pending_outbound_blocks_are_dropped_when_canceled_by_peer() {
        let mut f = Fixture::new();

        // given
        let block = block();
        let different_block = BlockInfo {
            piece_index: block.piece_index + 1,
            ..block
        };
        f.um().submit_outbound(UploaderMessage::Block(block.clone(), Vec::new()));
        f.um()
            .submit_outbound(UploaderMessage::Block(different_block.clone(), Vec::new()));

        // when
        f.uh().update_state(&DownloaderMessage::Cancel(block));

        // then
        assert!(matches!(
            f.uh().next_outbound().unwrap(),
            UploaderMessage::Block(info, _) if info == different_block));
        assert!(f.uh().next_outbound().is_none());
    }

    #[test]
    fn test_pending_outbound_request_is_not_repeated_until_fulfilled_by_peer() {
        let mut f = Fixture::new();

        // given
        let block = block();
        f.dm().submit_outbound(DownloaderMessage::Request(block.clone()));
        let _ = f.dh().next_outbound();

        // when
        f.dm().submit_outbound(DownloaderMessage::Request(block.clone()));

        // then
        assert!(f.dh().next_outbound().is_none());

        // when
        f.dh().update_state(&UploaderMessage::Block(block.clone(), Vec::new()));
        f.dm().submit_outbound(DownloaderMessage::Request(block.clone()));

        // then
        assert!(matches!(
            f.dh().next_outbound(),
            Some(DownloaderMessage::Request(info)) if info == block));
        assert!(f.dh().next_outbound().is_none());
    }

    #[test]
    fn test_pending_outbound_request_can_be_repeated_if_canceled() {
        let mut f = Fixture::new();

        // given
        let block = block();
        f.dm().submit_outbound(DownloaderMessage::Request(block.clone()));
        let _ = f.dh().next_outbound();
        f.dm().submit_outbound(DownloaderMessage::Cancel(block.clone()));
        let _ = f.dh().next_outbound();

        // when
        f.dm().submit_outbound(DownloaderMessage::Request(block.clone()));

        // then
        assert!(matches!(
            f.dh().next_outbound(),
            Some(DownloaderMessage::Request(info)) if info == block));
        assert!(f.dh().next_outbound().is_none());
    }
}
