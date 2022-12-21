#![allow(dead_code)]
use crate::ctrl::channel_monitors::{DownloadChannelStateUpdate, UploadChannelStateUpdate};
use crate::data::{BlockAccountant, PieceInfo};
use crate::pwp::{BlockInfo, DownloaderMessage, UploaderMessage};
use std::collections::HashSet;
use std::rc::Rc;
use std::{cmp, mem};

pub struct PeerStatus {
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    am_choking: bool,
    accountant: BlockAccountant,
    bytes_sent: usize,
    bytes_received: usize,

    pending_tx_upload: Vec<UploaderMessage>,
    pending_tx_download: Vec<DownloaderMessage>,
    requested_blocks: HashSet<BlockInfo>,
}

impl PeerStatus {
    pub fn new(pieces: Rc<PieceInfo>) -> Self {
        Self {
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            am_choking: true,
            accountant: BlockAccountant::new(pieces),
            bytes_sent: 0,
            bytes_received: 0,
            pending_tx_upload: Vec::new(),
            pending_tx_download: Vec::new(),
            requested_blocks: HashSet::new(),
        }
    }

    pub fn update_upload_state(
        &mut self,
        peer_interested: bool,
        am_choking: bool,
        state: &UploadChannelStateUpdate,
    ) {
        self.peer_interested = peer_interested;
        self.am_choking = am_choking;
        self.bytes_sent += state.bytes_uploaded;

        for block in &state.received_cancels {
            remove_if(
                &mut self.pending_tx_upload,
                |msg| matches!(msg, UploaderMessage::Block(info, _) if info == block),
            );
        }
    }

    pub fn update_download_state(
        &mut self,
        am_interested: bool,
        peer_choking: bool,
        state: &DownloadChannelStateUpdate,
    ) {
        self.am_interested = am_interested;
        self.peer_choking = peer_choking;
        self.bytes_received += state.bytes_downloaded;

        for bitfield in &state.received_bitfields {
            self.accountant.submit_bitfield(bitfield);
        }
        for piece_index in &state.received_haves {
            self.accountant.submit_piece(*piece_index);
        }
        for (block_info, _) in &state.received_blocks {
            let _ = self.accountant.submit_block(block_info);
            self.requested_blocks.remove(block_info);
        }
    }

    pub fn am_interested(&self) -> bool {
        self.am_interested
    }

    pub fn peer_choking(&self) -> bool {
        self.peer_choking
    }

    pub fn am_choking(&self) -> bool {
        self.am_choking
    }

    pub fn peer_interested(&self) -> bool {
        self.peer_interested
    }

    pub fn enqueue_uploader_msg(&mut self, msg: UploaderMessage) {
        if self.pending_tx_upload.contains(&msg) {
            return;
        }
        let should_enqueue = match &msg {
            UploaderMessage::Choke => {
                let contained_unchoke = remove_if(&mut self.pending_tx_upload, |m| {
                    matches!(m, UploaderMessage::Unchoke)
                });
                !contained_unchoke
            }
            UploaderMessage::Unchoke => {
                let contained_choke =
                    remove_if(&mut self.pending_tx_upload, |m| matches!(m, UploaderMessage::Choke));
                !contained_choke
            }
            _ => true,
        };
        if should_enqueue {
            self.pending_tx_upload.push(msg);
        }
    }

    pub fn enqueue_downloader_msg(&mut self, msg: DownloaderMessage) {
        if self.pending_tx_download.contains(&msg) {
            return;
        }
        let should_enqueue = match &msg {
            DownloaderMessage::Interested => {
                let contained_notinterested = remove_if(&mut self.pending_tx_download, |m| {
                    matches!(m, DownloaderMessage::NotInterested)
                });
                !contained_notinterested
            }
            DownloaderMessage::NotInterested => {
                let contained_interested = remove_if(&mut self.pending_tx_download, |m| {
                    matches!(m, DownloaderMessage::Interested)
                });
                !contained_interested
            }
            DownloaderMessage::Request(requested_block) => {
                let contained_cancel = remove_if(
                    &mut self.pending_tx_download,
                    |m| matches!(m, DownloaderMessage::Cancel(canceled_block) if canceled_block == requested_block),
                );
                let already_requested = self.requested_blocks.contains(requested_block);
                !contained_cancel && !already_requested
            }
            DownloaderMessage::Cancel(canceled_block) => {
                let contained_request = remove_if(
                    &mut self.pending_tx_download,
                    |m| matches!(m, DownloaderMessage::Request(requested_block) if requested_block == canceled_block),
                );
                !contained_request
            }
        };
        if should_enqueue {
            self.pending_tx_download.push(msg);
        }
    }

    pub fn take_pending_uploader_msgs(&mut self) -> impl Iterator<Item = UploaderMessage> {
        const MAX_UPLOAD_SIZE: usize = MAX_BLOCK_SIZE * 10;
        let mut total_upload_size = 0usize;
        let first_postponed = self.pending_tx_upload.iter().enumerate().find_map(|(index, msg)| {
            if let UploaderMessage::Block(info, _) = msg {
                total_upload_size += info.block_length;
            }
            if total_upload_size > MAX_UPLOAD_SIZE {
                Some(index)
            } else {
                None
            }
        });
        if let Some(from) = first_postponed {
            let postponed_tx_upload = self.pending_tx_upload.split_off(from);
            mem::replace(&mut self.pending_tx_upload, postponed_tx_upload).into_iter()
        } else {
            mem::take(&mut self.pending_tx_upload).into_iter()
        }
    }

    pub fn take_pending_downloader_msgs(&mut self) -> impl Iterator<Item = DownloaderMessage> {
        for msg in &self.pending_tx_download {
            match msg {
                DownloaderMessage::Request(block) => {
                    self.requested_blocks.insert(block.clone());
                }
                DownloaderMessage::Cancel(block) => {
                    self.requested_blocks.remove(block);
                }
                _ => (),
            }
        }
        mem::take(&mut self.pending_tx_download).into_iter()
    }
}

fn remove_if<E, F>(src: &mut Vec<E>, pred: F) -> bool
where
    F: Fn(&E) -> bool,
{
    let initial_len = src.len();
    src.retain(|e| !pred(e));
    src.len() != initial_len
}

const MAX_BLOCK_SIZE: usize = 16384;

pub fn divide_piece_into_blocks(
    piece_index: usize,
    piece_len: usize,
) -> impl Iterator<Item = BlockInfo> {
    (0..piece_len)
        .into_iter()
        .step_by(MAX_BLOCK_SIZE)
        .map(move |in_piece_offset| BlockInfo {
            piece_index,
            in_piece_offset,
            block_length: cmp::min(MAX_BLOCK_SIZE, piece_len - in_piece_offset),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter;

    #[test]
    fn test_divide_piece_into_multiple_blocks() {
        let mut blocks_it = divide_piece_into_blocks(42, 2 * MAX_BLOCK_SIZE + 100);
        assert_eq!(
            BlockInfo {
                piece_index: 42,
                in_piece_offset: 0,
                block_length: MAX_BLOCK_SIZE,
            },
            blocks_it.next().unwrap()
        );
        assert_eq!(
            BlockInfo {
                piece_index: 42,
                in_piece_offset: MAX_BLOCK_SIZE,
                block_length: MAX_BLOCK_SIZE,
            },
            blocks_it.next().unwrap()
        );
        assert_eq!(
            BlockInfo {
                piece_index: 42,
                in_piece_offset: 2 * MAX_BLOCK_SIZE,
                block_length: 100,
            },
            blocks_it.next().unwrap()
        );
        assert!(blocks_it.next().is_none());
    }

    #[test]
    fn test_piece_fits_in_single_block() {
        let mut blocks_it = divide_piece_into_blocks(42, MAX_BLOCK_SIZE - 100);
        assert_eq!(
            BlockInfo {
                piece_index: 42,
                in_piece_offset: 0,
                block_length: MAX_BLOCK_SIZE - 100,
            },
            blocks_it.next().unwrap()
        );
        assert!(blocks_it.next().is_none());
    }

    #[test]
    fn test_peer_status_has_correct_initial_state() {
        let mut ps = PeerStatus::new(Rc::new(PieceInfo::new(iter::empty(), 3)));

        assert!(!ps.am_interested());
        assert!(!ps.peer_interested());
        assert!(ps.am_choking());
        assert!(ps.peer_choking());
        assert!(ps.take_pending_uploader_msgs().next().is_none());
        assert!(ps.take_pending_downloader_msgs().next().is_none());
        assert_eq!(0, ps.bytes_sent);
        assert_eq!(0, ps.bytes_received);
    }

    #[test]
    fn test_peer_status_updates_state_on_upload_channel_update() {
        let mut ps = PeerStatus::new(Rc::new(PieceInfo::new(iter::empty(), 3)));

        let upload_update = UploadChannelStateUpdate {
            bytes_uploaded: 1024,
            ..Default::default()
        };
        let peer_interested = true;
        let am_choking = false;
        ps.update_upload_state(peer_interested, am_choking, &upload_update);

        assert_eq!(peer_interested, ps.peer_interested());
        assert_eq!(am_choking, ps.am_choking());
        assert_eq!(upload_update.bytes_uploaded, ps.bytes_sent);

        let am_choking = true;
        ps.update_upload_state(peer_interested, am_choking, &upload_update);
        assert_eq!(peer_interested, ps.peer_interested());
        assert_eq!(am_choking, ps.am_choking());
        assert_eq!(upload_update.bytes_uploaded * 2, ps.bytes_sent);
    }

    #[test]
    fn test_peer_status_updates_state_on_download_channel_update() {
        let mut ps = PeerStatus::new(Rc::new(PieceInfo::new(iter::empty(), 3)));

        let download_update = DownloadChannelStateUpdate {
            bytes_downloaded: 1024,
            ..Default::default()
        };

        let am_interested = true;
        let peer_choking = false;
        ps.update_download_state(am_interested, peer_choking, &download_update);

        assert_eq!(am_interested, ps.am_interested());
        assert_eq!(peer_choking, ps.peer_choking());
        assert_eq!(download_update.bytes_downloaded, ps.bytes_received);

        let am_interested = false;
        ps.update_download_state(am_interested, peer_choking, &download_update);

        assert_eq!(am_interested, ps.am_interested());
        assert_eq!(peer_choking, ps.peer_choking());
        assert_eq!(download_update.bytes_downloaded * 2, ps.bytes_received);
    }

    #[test]
    fn test_peer_status_filters_mutually_negating_upload_messages() {
        let mut ps = PeerStatus::new(Rc::new(PieceInfo::new(iter::empty(), 3)));

        ps.enqueue_uploader_msg(UploaderMessage::Unchoke);
        let mut msgs = ps.take_pending_uploader_msgs();
        assert!(matches!(msgs.next(), Some(UploaderMessage::Unchoke)));
        assert!(ps.take_pending_uploader_msgs().next().is_none());

        ps.enqueue_uploader_msg(UploaderMessage::Choke);
        let mut msgs = ps.take_pending_uploader_msgs();
        assert!(matches!(msgs.next(), Some(UploaderMessage::Choke)));
        assert!(ps.take_pending_uploader_msgs().next().is_none());

        ps.enqueue_uploader_msg(UploaderMessage::Unchoke);
        ps.enqueue_uploader_msg(UploaderMessage::Choke);
        assert!(ps.take_pending_uploader_msgs().next().is_none());
    }

    #[test]
    fn test_peer_status_filters_mutually_negating_download_messages() {
        let mut ps = PeerStatus::new(Rc::new(PieceInfo::new(iter::empty(), 3)));

        ps.enqueue_downloader_msg(DownloaderMessage::Interested);
        let mut msgs = ps.take_pending_downloader_msgs();
        assert!(matches!(msgs.next(), Some(DownloaderMessage::Interested)));
        assert!(msgs.next().is_none());

        ps.enqueue_downloader_msg(DownloaderMessage::NotInterested);
        let mut msgs = ps.take_pending_downloader_msgs();
        assert!(matches!(msgs.next(), Some(DownloaderMessage::NotInterested)));
        assert!(msgs.next().is_none());

        ps.enqueue_downloader_msg(DownloaderMessage::Interested);
        ps.enqueue_downloader_msg(DownloaderMessage::NotInterested);
        assert!(ps.take_pending_downloader_msgs().next().is_none());

        let block = BlockInfo {
            piece_index: 123,
            in_piece_offset: 16384,
            block_length: 1024,
        };

        ps.enqueue_downloader_msg(DownloaderMessage::Request(block.clone()));
        let mut msgs = ps.take_pending_downloader_msgs();
        assert!(matches!(msgs.next(), Some(DownloaderMessage::Request(info)) if info == block));
        assert!(msgs.next().is_none());

        ps.enqueue_downloader_msg(DownloaderMessage::Cancel(block.clone()));
        let mut msgs = ps.take_pending_downloader_msgs();
        assert!(matches!(msgs.next(), Some(DownloaderMessage::Cancel(info)) if info == block));
        assert!(msgs.next().is_none());

        ps.enqueue_downloader_msg(DownloaderMessage::Request(block.clone()));
        ps.enqueue_downloader_msg(DownloaderMessage::Cancel(block.clone()));
        assert!(ps.take_pending_downloader_msgs().next().is_none());

        let another_block = BlockInfo {
            piece_index: 100,
            in_piece_offset: 0,
            block_length: 16384,
        };
        ps.enqueue_downloader_msg(DownloaderMessage::Request(block.clone()));
        ps.enqueue_downloader_msg(DownloaderMessage::Cancel(another_block.clone()));
        let mut msgs = ps.take_pending_downloader_msgs();
        assert!(matches!(msgs.next(), Some(DownloaderMessage::Request(info)) if info == block));
        assert!(
            matches!(msgs.next(), Some(DownloaderMessage::Cancel(info)) if info == another_block)
        );
        assert!(msgs.next().is_none());
    }

    #[test]
    fn test_peer_status_doesnt_filter_different_messages_of_same_type() {
        let mut ps = PeerStatus::new(Rc::new(PieceInfo::new(iter::empty(), 3)));

        ps.enqueue_uploader_msg(UploaderMessage::Have { piece_index: 120 });
        ps.enqueue_uploader_msg(UploaderMessage::Have { piece_index: 121 });
        let mut msgs = ps.take_pending_uploader_msgs();
        assert!(matches!(msgs.next(), Some(UploaderMessage::Have { piece_index: 120 })));
        assert!(matches!(msgs.next(), Some(UploaderMessage::Have { piece_index: 121 })));
        assert!(msgs.next().is_none());

        let block = BlockInfo {
            piece_index: 123,
            in_piece_offset: 16384,
            block_length: 1024,
        };
        let another_block = BlockInfo {
            piece_index: 100,
            in_piece_offset: 0,
            block_length: 16384,
        };
        ps.enqueue_downloader_msg(DownloaderMessage::Request(block.clone()));
        ps.enqueue_downloader_msg(DownloaderMessage::Request(another_block.clone()));
        let mut msgs = ps.take_pending_downloader_msgs();
        assert!(matches!(msgs.next(), Some(DownloaderMessage::Request(info)) if info == block));
        assert!(
            matches!(msgs.next(), Some(DownloaderMessage::Request(info)) if info == another_block)
        );
        assert!(msgs.next().is_none());
    }

    #[test]
    fn test_peer_status_filters_duplicate_messages() {
        let mut ps = PeerStatus::new(Rc::new(PieceInfo::new(iter::empty(), 3)));

        ps.enqueue_uploader_msg(UploaderMessage::Have { piece_index: 124 });
        ps.enqueue_uploader_msg(UploaderMessage::Have { piece_index: 124 });
        let mut msgs = ps.take_pending_uploader_msgs();
        assert!(matches!(msgs.next(), Some(UploaderMessage::Have { piece_index: 124 })));
        assert!(msgs.next().is_none());

        let block = BlockInfo {
            piece_index: 123,
            in_piece_offset: 16384,
            block_length: 1024,
        };
        ps.enqueue_downloader_msg(DownloaderMessage::Request(block.clone()));
        ps.enqueue_downloader_msg(DownloaderMessage::Request(block.clone()));
        let mut msgs = ps.take_pending_downloader_msgs();
        assert!(matches!(msgs.next(), Some(DownloaderMessage::Request(info)) if info == block));
        assert!(msgs.next().is_none());
    }

    #[test]
    fn test_peer_status_limits_upload_to_at_most_10_max_block_size() {
        let mut ps = PeerStatus::new(Rc::new(PieceInfo::new(iter::empty(), 3)));

        // given
        ps.enqueue_uploader_msg(UploaderMessage::Unchoke);
        let all_blocks: Vec<_> = divide_piece_into_blocks(100, MAX_BLOCK_SIZE * 12).collect();
        for block in &all_blocks {
            ps.enqueue_uploader_msg(UploaderMessage::Block(block.clone(), Vec::new()));
        }

        // when
        let mut msgs = ps.take_pending_uploader_msgs();

        // then
        assert!(matches!(msgs.next(), Some(UploaderMessage::Unchoke)));
        for block in &all_blocks[0..10] {
            assert!(matches!(msgs.next(), Some(UploaderMessage::Block(info, _)) if &info == block));
        }
        assert!(msgs.next().is_none());

        // when
        let mut msgs = ps.take_pending_uploader_msgs();

        // then
        for block in &all_blocks[10..12] {
            assert!(matches!(msgs.next(), Some(UploaderMessage::Block(info, _)) if &info == block));
        }
        assert!(msgs.next().is_none());
    }

    #[test]
    fn test_peer_status_limits_upload_and_drops_canceled_blocks() {
        let mut ps = PeerStatus::new(Rc::new(PieceInfo::new(iter::empty(), 3)));

        // given
        ps.enqueue_uploader_msg(UploaderMessage::Unchoke);
        let all_blocks: Vec<_> = divide_piece_into_blocks(100, MAX_BLOCK_SIZE * 12).collect();
        for block in &all_blocks {
            ps.enqueue_uploader_msg(UploaderMessage::Block(block.clone(), Vec::new()));
        }
        drop(ps.take_pending_uploader_msgs());

        // when
        let upload_update = UploadChannelStateUpdate {
            received_cancels: vec![all_blocks[10].clone()],
            ..Default::default()
        };
        ps.update_upload_state(true, false, &upload_update);
        let mut msgs = ps.take_pending_uploader_msgs();

        // then
        for block in &all_blocks[11..12] {
            assert!(matches!(msgs.next(), Some(UploaderMessage::Block(info, _)) if &info == block));
        }
        assert!(msgs.next().is_none());
    }

    fn test_peer_status_filters_requests_for_requested_but_not_yet_received_blocks() {
        let mut ps = PeerStatus::new(Rc::new(PieceInfo::new(iter::empty(), 3)));

        // given
        let block = BlockInfo {
            piece_index: 123,
            in_piece_offset: 16384,
            block_length: 1024,
        };
        ps.enqueue_downloader_msg(DownloaderMessage::Request(block.clone()));
        drop(ps.take_pending_downloader_msgs());

        // when
        ps.enqueue_downloader_msg(DownloaderMessage::Request(block.clone()));

        // then
        let mut msgs = ps.take_pending_downloader_msgs();
        assert!(msgs.next().is_none());

        // when
        let download_update = DownloadChannelStateUpdate {
            received_blocks: vec![(block.clone(), Vec::new())],
            ..Default::default()
        };
        ps.update_download_state(true, false, &download_update);
        ps.enqueue_downloader_msg(DownloaderMessage::Request(block.clone()));

        // then
        let mut msgs = ps.take_pending_downloader_msgs();
        assert!(matches!(msgs.next(), Some(DownloaderMessage::Request(info)) if info == block));
        assert!(msgs.next().is_none());
    }

    #[test]
    fn test_peer_status_doesnt_filter_duplicate_request_if_initial_one_was_canceled() {
        let mut ps = PeerStatus::new(Rc::new(PieceInfo::new(iter::empty(), 3)));

        // given
        let block = BlockInfo {
            piece_index: 123,
            in_piece_offset: 16384,
            block_length: 1024,
        };
        ps.enqueue_downloader_msg(DownloaderMessage::Request(block.clone()));
        drop(ps.take_pending_downloader_msgs());

        ps.enqueue_downloader_msg(DownloaderMessage::Cancel(block.clone()));
        let mut msgs = ps.take_pending_downloader_msgs();
        assert!(matches!(msgs.next(), Some(DownloaderMessage::Cancel(info)) if info == block));
        assert!(msgs.next().is_none());

        // when
        ps.enqueue_downloader_msg(DownloaderMessage::Request(block.clone()));

        // then
        let mut msgs = ps.take_pending_downloader_msgs();
        assert!(matches!(msgs.next(), Some(DownloaderMessage::Request(info)) if info == block));
        assert!(msgs.next().is_none());
    }
}
