#![allow(dead_code)]
use crate::data::{BlockAccountant, PieceInfo};
use crate::pwp::{BlockInfo, DownloaderMessage, UploaderMessage};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::rc::Rc;

pub trait DownloadChannelHandler {
    fn update_state(&mut self, inbound: &UploaderMessage);
    fn next_outbound(&mut self) -> Option<DownloaderMessage>;
}

pub trait UploadChannelHandler {
    fn update_state(&mut self, inbound: &DownloaderMessage);
    fn next_outbound(&mut self) -> Option<UploaderMessage>;
}

pub trait DownloadChannelMonitor {
    fn am_interested(&self) -> bool;
    fn peer_choking(&self) -> bool;
    fn remote_availability(&self) -> &BlockAccountant;
    fn bytes_received(&self) -> usize;
    fn submit_outbound(&mut self, msg: DownloaderMessage);
}

pub trait UploadChannelMonitor {
    fn peer_interested(&self) -> bool;
    fn am_choking(&self) -> bool;
    fn bytes_sent(&self) -> usize;
    fn submit_outbound(&mut self, msg: UploaderMessage);
}

pub struct PeerManager {
    pieces: Rc<PieceInfo>,
    channel_states: HashMap<SocketAddr, (DownloadChannelState, UploadChannelState)>,
}

pub struct HandlerOwner<'m> {
    inner: &'m mut PeerManager,
}

pub struct MonitorOwner<'m> {
    inner: &'m mut PeerManager,
}

impl PeerManager {
    pub fn new(pieces: Rc<PieceInfo>) -> Self {
        Self {
            pieces,
            channel_states: HashMap::new(),
        }
    }

    pub fn add_peer(&mut self, remote_ip: &SocketAddr) {
        let download = DownloadChannelState {
            am_interested: false,
            peer_choking: true,
            availability: BlockAccountant::new(self.pieces.clone()),
            bytes_received: 0,
            pending_tx_msgs: VecDeque::new(),
            requested_blocks: HashSet::new(),
        };
        let upload = UploadChannelState {
            peer_interested: false,
            am_choking: true,
            bytes_sent: 0,
            pending_tx_msgs: VecDeque::new(),
        };
        self.channel_states.insert(*remote_ip, (download, upload));
    }

    pub fn remove_peer(&mut self, remote_ip: &SocketAddr) {
        self.channel_states.remove(remote_ip);
    }

    pub fn as_handler_owner(&mut self) -> HandlerOwner {
        HandlerOwner { inner: self }
    }

    pub fn as_monitor_owner(&mut self) -> MonitorOwner {
        MonitorOwner { inner: self }
    }
}

impl<'m> HandlerOwner<'m> {
    pub fn download_handler(
        &mut self,
        remote_ip: &SocketAddr,
    ) -> Option<&mut impl DownloadChannelHandler> {
        let (download, _upload) = self.inner.channel_states.get_mut(remote_ip)?;
        Some(download)
    }

    pub fn upload_handler(
        &mut self,
        remote_ip: &SocketAddr,
    ) -> Option<&mut impl UploadChannelHandler> {
        let (_download, upload) = self.inner.channel_states.get_mut(remote_ip)?;
        Some(upload)
    }
}

impl<'m> MonitorOwner<'m> {
    pub fn download_monitor(
        &mut self,
        remote_ip: &SocketAddr,
    ) -> Option<&mut impl DownloadChannelMonitor> {
        let (download, _upload) = self.inner.channel_states.get_mut(remote_ip)?;
        Some(download)
    }

    pub fn upload_monitor(
        &mut self,
        remote_ip: &SocketAddr,
    ) -> Option<&mut impl UploadChannelMonitor> {
        let (_download, upload) = self.inner.channel_states.get_mut(remote_ip)?;
        Some(upload)
    }

    pub fn all_download_monitors(
        &mut self,
    ) -> impl Iterator<Item = &mut impl DownloadChannelMonitor> {
        self.inner.channel_states.values_mut().map(|(download, _upload)| download)
    }

    pub fn all_upload_monitors(&mut self) -> impl Iterator<Item = &mut impl UploadChannelMonitor> {
        self.inner.channel_states.values_mut().map(|(_download, upload)| upload)
    }

    pub fn all_monitors(
        &mut self,
    ) -> impl Iterator<Item = (&mut impl DownloadChannelMonitor, &mut impl UploadChannelMonitor)>
    {
        self.inner
            .channel_states
            .values_mut()
            .map(|(download, upload)| (download, upload))
    }
}

struct DownloadChannelState {
    am_interested: bool,
    peer_choking: bool,
    availability: BlockAccountant,
    bytes_received: usize,
    pending_tx_msgs: VecDeque<DownloaderMessage>,
    requested_blocks: HashSet<BlockInfo>,
}

impl DownloadChannelHandler for DownloadChannelState {
    fn update_state(&mut self, inbound: &UploaderMessage) {
        match inbound {
            UploaderMessage::Unchoke => self.peer_choking = false,
            UploaderMessage::Choke => self.peer_choking = true,
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
        let msg = self.pending_tx_msgs.pop_front()?;
        match &msg {
            DownloaderMessage::Request(block) => {
                self.requested_blocks.insert(block.clone());
            }
            DownloaderMessage::Cancel(block) => {
                self.requested_blocks.remove(block);
            }
            DownloaderMessage::NotInterested => self.am_interested = false,
            DownloaderMessage::Interested => self.am_interested = true,
        }
        Some(msg)
    }
}

impl DownloadChannelMonitor for DownloadChannelState {
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

    fn submit_outbound(&mut self, msg: DownloaderMessage) {
        if self.pending_tx_msgs.contains(&msg) {
            return;
        }
        let should_enqueue = match &msg {
            DownloaderMessage::Interested => {
                let contained_notinterested = remove_if(&mut self.pending_tx_msgs, |m| {
                    matches!(m, DownloaderMessage::NotInterested)
                });
                !contained_notinterested
            }
            DownloaderMessage::NotInterested => {
                let contained_interested = remove_if(&mut self.pending_tx_msgs, |m| {
                    matches!(m, DownloaderMessage::Interested)
                });
                !contained_interested
            }
            DownloaderMessage::Request(requested_block) => {
                let contained_cancel = remove_if(
                    &mut self.pending_tx_msgs,
                    |m| matches!(m, DownloaderMessage::Cancel(canceled_block) if canceled_block == requested_block),
                );
                let already_requested = self.requested_blocks.contains(requested_block);
                !contained_cancel && !already_requested
            }
            DownloaderMessage::Cancel(canceled_block) => {
                let contained_request = remove_if(
                    &mut self.pending_tx_msgs,
                    |m| matches!(m, DownloaderMessage::Request(requested_block) if requested_block == canceled_block),
                );
                !contained_request
            }
        };
        if should_enqueue {
            self.pending_tx_msgs.push_back(msg);
        }
    }
}

struct UploadChannelState {
    peer_interested: bool,
    am_choking: bool,
    bytes_sent: usize,
    pending_tx_msgs: VecDeque<UploaderMessage>,
}

impl UploadChannelHandler for UploadChannelState {
    fn update_state(&mut self, inbound: &DownloaderMessage) {
        match inbound {
            DownloaderMessage::NotInterested => self.peer_interested = false,
            DownloaderMessage::Interested => self.peer_interested = true,
            DownloaderMessage::Cancel(block) => {
                remove_if(
                    &mut self.pending_tx_msgs,
                    |m| matches!(m, UploaderMessage::Block(info, _) if info == block),
                );
            }
            DownloaderMessage::Request(_block) => (),
        }
    }

    fn next_outbound(&mut self) -> Option<UploaderMessage> {
        let msg = self.pending_tx_msgs.pop_front()?;
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

impl UploadChannelMonitor for UploadChannelState {
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

    fn submit_outbound(&mut self, msg: UploaderMessage) {
        if self.pending_tx_msgs.contains(&msg) {
            return;
        }
        let should_enqueue = match &msg {
            UploaderMessage::Choke => {
                let contained_unchoke =
                    remove_if(&mut self.pending_tx_msgs, |m| matches!(m, UploaderMessage::Unchoke));
                !contained_unchoke
            }
            UploaderMessage::Unchoke => {
                let contained_choke =
                    remove_if(&mut self.pending_tx_msgs, |m| matches!(m, UploaderMessage::Choke));
                !contained_choke
            }
            _ => true,
        };
        if should_enqueue {
            self.pending_tx_msgs.push_back(msg);
        }
    }
}

fn remove_if<E, F>(src: &mut VecDeque<E>, pred: F) -> bool
where
    F: Fn(&E) -> bool,
{
    let initial_len = src.len();
    src.retain(|e| !pred(e));
    src.len() != initial_len
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[test]
    fn test_manager_adds_and_removes_peers() {
        let ip = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666));
        let mut mgr = PeerManager::new(Rc::new(PieceInfo::new(iter::empty(), 3)));

        let mut mo = mgr.as_monitor_owner();
        assert_eq!(0, mo.all_monitors().count());
        assert_eq!(0, mo.all_upload_monitors().count());
        assert_eq!(0, mo.all_download_monitors().count());

        mgr.add_peer(&ip);

        let mut mo = mgr.as_monitor_owner();
        assert_eq!(1, mo.all_monitors().count());
        assert_eq!(1, mo.all_upload_monitors().count());
        assert_eq!(1, mo.all_download_monitors().count());
        assert!(mo.upload_monitor(&ip).is_some());
        assert!(mo.download_monitor(&ip).is_some());

        let mut ho = mgr.as_handler_owner();
        assert!(ho.upload_handler(&ip).is_some());
        assert!(ho.download_handler(&ip).is_some());

        mgr.remove_peer(&ip);

        let mut mo = mgr.as_monitor_owner();
        assert_eq!(0, mo.all_monitors().count());
        assert_eq!(0, mo.all_upload_monitors().count());
        assert_eq!(0, mo.all_download_monitors().count());
        assert!(mo.upload_monitor(&ip).is_none());
        assert!(mo.download_monitor(&ip).is_none());

        let mut ho = mgr.as_handler_owner();
        assert!(ho.upload_handler(&ip).is_none());
        assert!(ho.download_handler(&ip).is_none());
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
    }

    macro_rules! um {
        ($fix:ident, $method:ident($( $args:expr ),*)) => {{
            let ip = $fix.ip;
            let mut monitors = $fix.mgr.as_monitor_owner();
            let monitor = monitors.upload_monitor(&ip).unwrap();
            monitor.$method($($args),*)
        }};
    }
    macro_rules! dm {
        ($fix:ident, $method:ident($( $args:expr ),*)) => {{
            let ip = $fix.ip;
            let mut monitors = $fix.mgr.as_monitor_owner();
            let monitor = monitors.download_monitor(&ip).unwrap();
            monitor.$method($($args),*)
        }};
    }
    macro_rules! uh {
        ($fix:ident, $method:ident($( $args:expr ),*)) => {{
            let ip = $fix.ip;
            let mut handlers = $fix.mgr.as_handler_owner();
            let handler = handlers.upload_handler(&ip).unwrap();
            handler.$method($($args),*)
        }};
    }
    macro_rules! dh {
        ($fix:ident, $method:ident($( $args:expr ),*)) => {{
            let ip = $fix.ip;
            let mut handlers = $fix.mgr.as_handler_owner();
            let handler = handlers.download_handler(&ip).unwrap();
            handler.$method($($args),*)
        }};
    }

    #[test]
    fn test_peer_has_correct_initial_state() {
        let ip = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666));
        let mut mgr = PeerManager::new(Rc::new(PieceInfo::new(iter::empty(), 3)));
        mgr.add_peer(&ip);

        let mut monitors = mgr.as_monitor_owner();

        let down_mon = monitors.download_monitor(&ip).unwrap();
        assert!(!down_mon.am_interested());
        assert!(down_mon.peer_choking());
        assert_eq!(0, down_mon.bytes_received());

        let up_mon = monitors.upload_monitor(&ip).unwrap();
        assert!(!up_mon.peer_interested());
        assert!(up_mon.am_choking());
        assert_eq!(0, up_mon.bytes_sent());

        let mut handlers = mgr.as_handler_owner();

        let down_hnd = handlers.download_handler(&ip).unwrap();
        assert!(down_hnd.next_outbound().is_none());

        let up_hnd = handlers.upload_handler(&ip).unwrap();
        assert!(up_hnd.next_outbound().is_none());
    }

    #[test]
    fn test_peer_updates_state_on_upload_channel_update() {
        let mut f = Fixture::new();

        uh!(f, update_state(&DownloaderMessage::Interested));
        assert!(um!(f, peer_interested()));

        uh!(f, update_state(&DownloaderMessage::NotInterested));
        assert!(!um!(f, peer_interested()));

        um!(f, submit_outbound(UploaderMessage::Unchoke));
        assert!(!um!(f, am_choking()));
        assert!(matches!(uh!(f, next_outbound()).unwrap(), UploaderMessage::Unchoke));
        assert!(uh!(f, next_outbound()).is_none());

        um!(f, submit_outbound(UploaderMessage::Choke));
        assert!(um!(f, am_choking()));
        assert!(matches!(uh!(f, next_outbound()).unwrap(), UploaderMessage::Choke));
        assert!(uh!(f, next_outbound()).is_none());

        let block = block();
        um!(f, submit_outbound(UploaderMessage::Block(block.clone(), vec![0u8; 1024])));
        assert_eq!(0, um!(f, bytes_sent()));

        let msg = uh!(f, next_outbound()).unwrap();
        assert!(matches!(
            msg,
            UploaderMessage::Block(info, data) if info == block && data == vec![0u8; 1024]));
        assert!(uh!(f, next_outbound()).is_none());
        assert_eq!(1024, um!(f, bytes_sent()));

        um!(f, submit_outbound(UploaderMessage::Block(block.clone(), vec![0u8; 1024])));
        assert_eq!(1024, um!(f, bytes_sent()));

        let msg = uh!(f, next_outbound()).unwrap();
        assert!(matches!(
            msg,
            UploaderMessage::Block(info, data) if info == block && data == vec![0u8; 1024]));
        assert!(uh!(f, next_outbound()).is_none());
        assert_eq!(1024 * 2, um!(f, bytes_sent()));
    }

    #[test]
    fn test_peer_updates_state_on_download_channel_update() {
        let mut f = Fixture::new();

        dh!(f, update_state(&UploaderMessage::Unchoke));
        assert!(!dm!(f, peer_choking()));

        dh!(f, update_state(&UploaderMessage::Choke));
        assert!(dm!(f, peer_choking()));

        dm!(f, submit_outbound(DownloaderMessage::Interested));
        assert!(dm!(f, am_interested()));
        assert!(matches!(dh!(f, next_outbound()).unwrap(), DownloaderMessage::Interested));
        assert!(dh!(f, next_outbound()).is_none());

        dm!(f, submit_outbound(DownloaderMessage::NotInterested));
        assert!(!dm!(f, am_interested()));
        assert!(matches!(dh!(f, next_outbound()).unwrap(), DownloaderMessage::NotInterested));
        assert!(dh!(f, next_outbound()).is_none());

        let block = block();
        dh!(f, update_state(&UploaderMessage::Block(block.clone(), vec![0u8; 1024])));
        assert_eq!(1024, dm!(f, bytes_received()));

        dh!(f, update_state(&UploaderMessage::Block(block, vec![0u8; 1024])));
        assert_eq!(1024 * 2, dm!(f, bytes_received()));
    }

    #[test]
    fn test_duplicated_outbound_upload_messages_are_dropped() {
        let mut f = Fixture::new();

        um!(f, submit_outbound(UploaderMessage::Unchoke));
        um!(f, submit_outbound(UploaderMessage::Unchoke));
        assert!(matches!(uh!(f, next_outbound()).unwrap(), UploaderMessage::Unchoke));
        assert!(uh!(f, next_outbound()).is_none());

        let block = block();
        um!(f, submit_outbound(UploaderMessage::Block(block.clone(), vec![0u8; 1024])));
        um!(f, submit_outbound(UploaderMessage::Block(block.clone(), vec![0u8; 1024])));
        assert!(matches!(
            uh!(f, next_outbound()).unwrap(),
            UploaderMessage::Block(info, data) if info == block && data == vec![0u8; 1024]));
        assert!(uh!(f, next_outbound()).is_none());

        um!(f, submit_outbound(UploaderMessage::Have { piece_index: 124 }));
        um!(f, submit_outbound(UploaderMessage::Have { piece_index: 123 }));
        um!(f, submit_outbound(UploaderMessage::Have { piece_index: 124 }));
        assert!(matches!(
            uh!(f, next_outbound()).unwrap(),
            UploaderMessage::Have { piece_index: 124 }
        ));
        assert!(matches!(
            uh!(f, next_outbound()).unwrap(),
            UploaderMessage::Have { piece_index: 123 }
        ));
        assert!(uh!(f, next_outbound()).is_none());
    }

    #[test]
    fn test_duplicated_outbound_download_messages_are_dropped() {
        let mut f = Fixture::new();

        dm!(f, submit_outbound(DownloaderMessage::Interested));
        dm!(f, submit_outbound(DownloaderMessage::Interested));
        assert!(matches!(dh!(f, next_outbound()).unwrap(), DownloaderMessage::Interested));
        assert!(dh!(f, next_outbound()).is_none());

        let block = block();
        dm!(f, submit_outbound(DownloaderMessage::Request(block.clone())));
        dm!(f, submit_outbound(DownloaderMessage::Request(block.clone())));
        assert!(matches!(
            dh!(f, next_outbound()).unwrap(),
            DownloaderMessage::Request(info) if info == block));
        assert!(dh!(f, next_outbound()).is_none());

        let different_block = BlockInfo {
            piece_index: block.piece_index + 1,
            ..block
        };
        dm!(f, submit_outbound(DownloaderMessage::Cancel(block.clone())));
        dm!(f, submit_outbound(DownloaderMessage::Cancel(different_block.clone())));
        dm!(f, submit_outbound(DownloaderMessage::Cancel(block.clone())));
        assert!(matches!(
            dh!(f, next_outbound()).unwrap(),
            DownloaderMessage::Cancel(info) if info == block));
        assert!(matches!(
            dh!(f, next_outbound()).unwrap(),
            DownloaderMessage::Cancel(info) if info == different_block));
        assert!(dh!(f, next_outbound()).is_none());
    }

    #[test]
    fn test_mutually_negating_outbound_upload_messages_are_dropped() {
        let mut f = Fixture::new();

        um!(f, submit_outbound(UploaderMessage::Unchoke));
        um!(f, submit_outbound(UploaderMessage::Choke));
        assert!(uh!(f, next_outbound()).is_none());

        um!(f, submit_outbound(UploaderMessage::Choke));
        um!(f, submit_outbound(UploaderMessage::Unchoke));
        assert!(uh!(f, next_outbound()).is_none());
    }

    #[test]
    fn test_mutually_negating_outbound_download_messages_are_dropped() {
        let mut f = Fixture::new();

        dm!(f, submit_outbound(DownloaderMessage::Interested));
        dm!(f, submit_outbound(DownloaderMessage::NotInterested));
        assert!(dh!(f, next_outbound()).is_none());

        dm!(f, submit_outbound(DownloaderMessage::NotInterested));
        dm!(f, submit_outbound(DownloaderMessage::Interested));
        assert!(dh!(f, next_outbound()).is_none());

        let block = block();
        dm!(f, submit_outbound(DownloaderMessage::Request(block.clone())));
        dm!(f, submit_outbound(DownloaderMessage::Cancel(block.clone())));
        assert!(dh!(f, next_outbound()).is_none());

        dm!(f, submit_outbound(DownloaderMessage::Cancel(block.clone())));
        dm!(f, submit_outbound(DownloaderMessage::Request(block)));
        assert!(dh!(f, next_outbound()).is_none());
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
        um!(f, submit_outbound(UploaderMessage::Block(block.clone(), Vec::new())));
        um!(f, submit_outbound(UploaderMessage::Block(different_block.clone(), Vec::new())));

        // when
        uh!(f, update_state(&DownloaderMessage::Cancel(block)));

        // then
        assert!(matches!(
            uh!(f, next_outbound()).unwrap(),
            UploaderMessage::Block(info, _) if info == different_block));
        assert!(uh!(f, next_outbound()).is_none());
    }

    #[test]
    fn test_pending_outbound_request_is_not_repeated_until_fulfilled_by_peer() {
        let mut f = Fixture::new();

        // given
        let block = block();
        dm!(f, submit_outbound(DownloaderMessage::Request(block.clone())));
        let _ = dh!(f, next_outbound());

        // when
        dm!(f, submit_outbound(DownloaderMessage::Request(block.clone())));

        // then
        assert!(dh!(f, next_outbound()).is_none());

        // when
        dh!(f, update_state(&UploaderMessage::Block(block.clone(), Vec::new())));
        dm!(f, submit_outbound(DownloaderMessage::Request(block.clone())));

        // then
        assert!(matches!(
            dh!(f, next_outbound()),
            Some(DownloaderMessage::Request(info)) if info == block));
        assert!(dh!(f, next_outbound()).is_none());
    }

    #[test]
    fn test_pending_outbound_request_can_be_repeated_if_canceled() {
        let mut f = Fixture::new();

        // given
        let block = block();
        dm!(f, submit_outbound(DownloaderMessage::Request(block.clone())));
        let _ = dh!(f, next_outbound());
        dm!(f, submit_outbound(DownloaderMessage::Cancel(block.clone())));
        let _ = dh!(f, next_outbound());

        // when
        dm!(f, submit_outbound(DownloaderMessage::Request(block.clone())));

        // then
        assert!(matches!(
            dh!(f, next_outbound()),
            Some(DownloaderMessage::Request(info)) if info == block));
        assert!(dh!(f, next_outbound()).is_none());
    }
}
