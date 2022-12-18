use crate::data::{BlockAccountant, PieceInfo};
use crate::pwp::{
    BlockInfo, ChannelError, DownloadChannel, DownloaderMessage, UploadChannel, UploaderMessage,
};
use log::{error, info};
use std::cell::Cell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

pub struct DownloadChannelMonitor {
    channel: DownloadChannel,
    interested: bool,
    choked: bool,
    availability: BlockAccountant,
    received_blocks: Cell<Vec<(BlockInfo, Vec<u8>)>>,
}

impl DownloadChannelMonitor {
    pub fn new(channel: DownloadChannel, pieces: Rc<PieceInfo>) -> Self {
        Self {
            channel,
            interested: false,
            choked: true,
            availability: BlockAccountant::new(pieces),
            received_blocks: Cell::new(Vec::new()),
        }
    }

    pub fn remote_ip(&self) -> &SocketAddr {
        self.channel.remote_ip()
    }

    pub async fn send_outgoing<I>(&mut self, msgs: I) -> Result<(), ChannelError>
    where
        I: Iterator<Item = DownloaderMessage>,
    {
        for msg in msgs {
            let interested = matches!(msg, DownloaderMessage::Interested);
            let not_interested = matches!(msg, DownloaderMessage::NotInterested);
            self.channel.send_message(msg).await?;
            if interested {
                self.interested = true;
            } else if not_interested {
                self.interested = false;
            }
        }
        Ok(())
    }

    pub async fn receive_incoming(&mut self, timeout: Duration) -> Result<(), ChannelError> {
        // returns error on timeout!
        let msg = self.channel.receive_message_timed(timeout).await?;
        info!("{} Message received: {}", self.remote_ip(), &msg);
        match msg {
            UploaderMessage::Choke => self.choked = true,
            UploaderMessage::Unchoke => self.choked = false,
            UploaderMessage::Have { piece_index } => {
                if !self.availability.submit_piece(piece_index) {
                    error!("{} invalid Have received", self.remote_ip());
                }
            }
            UploaderMessage::Bitfield(bitfield) => {
                if !self.availability.submit_bitfield(&bitfield) {
                    error!("{} invalid Bitfield received", self.remote_ip());
                }
            }
            UploaderMessage::Block(info, data) => self.received_blocks.get_mut().push((info, data)),
        }
        Ok(())
    }

    pub fn remote_availability(&self) -> &BlockAccountant {
        &self.availability
    }

    pub fn am_interested(&self) -> bool {
        self.interested
    }

    pub fn peer_choking(&self) -> bool {
        self.choked
    }

    pub fn received_blocks(&self) -> impl Iterator<Item = (BlockInfo, Vec<u8>)> {
        self.received_blocks.take().into_iter()
    }
}

pub struct UploadChannelMonitor {
    channel: UploadChannel,
    choking: bool,
    interest: bool,
    received_requests: Cell<Vec<BlockInfo>>,
    received_cancels: Cell<Vec<BlockInfo>>,
}

impl UploadChannelMonitor {
    pub fn new(channel: UploadChannel) -> Self {
        Self {
            channel,
            choking: true,
            interest: false,
            received_requests: Cell::new(Vec::new()),
            received_cancels: Cell::new(Vec::new()),
        }
    }

    pub fn remote_ip(&self) -> &SocketAddr {
        self.channel.remote_ip()
    }

    pub async fn send_outgoing<I>(&mut self, msgs: I) -> Result<(), ChannelError>
    where
        I: Iterator<Item = UploaderMessage>,
    {
        for msg in msgs {
            let choking = matches!(msg, UploaderMessage::Choke);
            let unchoking = matches!(msg, UploaderMessage::Unchoke);
            self.channel.send_message(msg).await?;
            if choking {
                self.choking = true;
            } else if unchoking {
                self.choking = false;
            }
        }
        Ok(())
    }

    pub async fn receive_incoming(&mut self, timeout: Duration) -> Result<(), ChannelError> {
        // returns error on timeout!
        let msg = self.channel.receive_message_timed(timeout).await?;
        info!("{} Message received: {}", self.remote_ip(), &msg);
        match msg {
            DownloaderMessage::Interested => self.interest = true,
            DownloaderMessage::NotInterested => self.interest = false,
            DownloaderMessage::Request(info) => self.received_requests.get_mut().push(info),
            DownloaderMessage::Cancel(info) => self.received_cancels.get_mut().push(info),
        }
        Ok(())
    }

    pub fn am_choking(&self) -> bool {
        self.choking
    }

    pub fn peer_interested(&self) -> bool {
        self.interest
    }

    pub fn received_requests(&self) -> impl Iterator<Item = BlockInfo> {
        self.received_requests.take().into_iter()
    }

    pub fn received_cancellations(&mut self) -> impl Iterator<Item = BlockInfo> {
        self.received_cancels.take().into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::mpsc;
    use futures::join;
    use futures::prelude::*;
    use std::iter::{once, repeat_with};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn create_download_monitor(
        remote_ip: SocketAddr,
        pieces: Rc<PieceInfo>,
    ) -> (
        DownloadChannelMonitor,
        mpsc::Receiver<Option<DownloaderMessage>>,
        mpsc::Sender<UploaderMessage>,
    ) {
        let (tx_msg_sender, tx_msg_receiver) = mpsc::channel::<Option<DownloaderMessage>>(0);
        let (rx_msg_sender, rx_msg_receiver) = mpsc::channel::<UploaderMessage>(0);

        let channel = DownloadChannel::new(rx_msg_receiver, tx_msg_sender, remote_ip);
        let monitor = DownloadChannelMonitor::new(channel, pieces);
        (monitor, tx_msg_receiver, rx_msg_sender)
    }

    fn create_upload_monitor(
        remote_ip: SocketAddr,
    ) -> (
        UploadChannelMonitor,
        mpsc::Receiver<Option<UploaderMessage>>,
        mpsc::Sender<DownloaderMessage>,
    ) {
        let (tx_msg_sender, tx_msg_receiver) = mpsc::channel::<Option<UploaderMessage>>(0);
        let (rx_msg_sender, rx_msg_receiver) = mpsc::channel::<DownloaderMessage>(0);

        let channel = UploadChannel::new(rx_msg_receiver, tx_msg_sender, remote_ip);
        let monitor = UploadChannelMonitor::new(channel);
        (monitor, tx_msg_receiver, rx_msg_sender)
    }

    #[test]
    fn test_download_monitor_updates_state_correctly() {
        let remote_ip = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666));
        let piece_length = 128;
        let pieces = Rc::new(PieceInfo::new(vec![[0u8; 20].as_slice()].into_iter(), piece_length));
        let (mut monitor, mut local_msgs, mut remote_msgs) =
            create_download_monitor(remote_ip, pieces);
        assert!(!monitor.am_interested());
        assert!(monitor.peer_choking());
        assert_eq!(remote_ip, *monitor.remote_ip());
        assert!(monitor.received_blocks.get_mut().is_empty());

        let block = BlockInfo {
            piece_index: 0,
            in_piece_offset: 8,
            block_length: 8,
        };

        let runner_fut = async move {
            remote_msgs.send(UploaderMessage::Unchoke).await.unwrap();
            remote_msgs.send(UploaderMessage::Choke).await.unwrap();
            local_msgs.next().await.unwrap();
            local_msgs.next().await.unwrap();
            local_msgs.next().await.unwrap();
            local_msgs.next().await.unwrap();
            remote_msgs.send(UploaderMessage::Have { piece_index: 0 }).await.unwrap();
            remote_msgs.send(UploaderMessage::Block(block, vec![42u8; 20])).await.unwrap();
            local_msgs.next().await.unwrap();
            local_msgs.next().await.unwrap();
            local_msgs.next().await.unwrap();
            local_msgs.next().await.unwrap();
        };

        let test_fut = async move {
            monitor.receive_incoming(Duration::from_secs(1)).await.unwrap();
            assert!(!monitor.peer_choking());

            monitor.receive_incoming(Duration::from_secs(1)).await.unwrap();
            assert!(monitor.peer_choking());

            monitor.send_outgoing(once(DownloaderMessage::Interested)).await.unwrap();
            assert!(monitor.am_interested());

            monitor.send_outgoing(once(DownloaderMessage::NotInterested)).await.unwrap();
            assert!(!monitor.am_interested());

            monitor.receive_incoming(Duration::from_secs(1)).await.unwrap();
            assert!(monitor.availability.has_exact_block_at(0, piece_length));

            monitor.receive_incoming(Duration::from_secs(1)).await.unwrap();
            let mut received_blocks: Vec<_> = monitor.received_blocks().collect();
            assert_eq!(1, received_blocks.len());

            let (info, data) = received_blocks.pop().unwrap();
            assert_eq!(block, info);
            assert_eq!(vec![42u8; 20], data);

            let block_info = BlockInfo {
                piece_index: 0,
                in_piece_offset: 0,
                block_length: 0,
            };
            monitor
                .send_outgoing(repeat_with(|| DownloaderMessage::Request(block_info)).take(2))
                .await
                .unwrap();

            let err = monitor.send_outgoing(once(DownloaderMessage::Interested)).await.unwrap_err();
            assert!(matches!(err, ChannelError::ConnectionClosed));
            assert!(!monitor.am_interested());

            let err = monitor.receive_incoming(Duration::from_secs(1)).await.unwrap_err();
            assert!(matches!(err, ChannelError::ConnectionClosed));
        };

        async_io::block_on(async move { join!(runner_fut, test_fut) });
    }

    #[test]
    fn test_upload_monitor_updates_state_correctly() {
        let remote_ip = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666));
        let (mut monitor, mut local_msgs, mut remote_msgs) = create_upload_monitor(remote_ip);
        assert!(monitor.am_choking());
        assert!(!monitor.peer_interested());
        assert_eq!(remote_ip, *monitor.remote_ip());
        assert!(monitor.received_requests.get_mut().is_empty());
        assert!(monitor.received_cancels.get_mut().is_empty());

        let block = BlockInfo {
            piece_index: 0,
            in_piece_offset: 8,
            block_length: 8,
        };

        let runner_fut = async move {
            remote_msgs.send(DownloaderMessage::Interested).await.unwrap();
            remote_msgs.send(DownloaderMessage::NotInterested).await.unwrap();
            local_msgs.next().await.unwrap();
            local_msgs.next().await.unwrap();
            local_msgs.next().await.unwrap();
            local_msgs.next().await.unwrap();
            remote_msgs.send(DownloaderMessage::Request(block)).await.unwrap();
            remote_msgs.send(DownloaderMessage::Cancel(block)).await.unwrap();
            local_msgs.next().await.unwrap();
            local_msgs.next().await.unwrap();
            local_msgs.next().await.unwrap();
            local_msgs.next().await.unwrap();
        };

        let test_fut = async move {
            monitor.receive_incoming(Duration::from_secs(1)).await.unwrap();
            assert!(monitor.peer_interested());

            monitor.receive_incoming(Duration::from_secs(1)).await.unwrap();
            assert!(!monitor.peer_interested());

            monitor.send_outgoing(once(UploaderMessage::Unchoke)).await.unwrap();
            assert!(!monitor.am_choking());

            monitor.send_outgoing(once(UploaderMessage::Choke)).await.unwrap();
            assert!(monitor.am_choking());

            monitor.receive_incoming(Duration::from_secs(1)).await.unwrap();
            assert_eq!(vec![block], monitor.received_requests().collect::<Vec<_>>());

            monitor.receive_incoming(Duration::from_secs(1)).await.unwrap();
            assert_eq!(vec![block], monitor.received_cancellations().collect::<Vec<_>>());

            monitor
                .send_outgoing(repeat_with(|| UploaderMessage::Have { piece_index: 0 }).take(2))
                .await
                .unwrap();

            let err = monitor.send_outgoing(once(UploaderMessage::Unchoke)).await.unwrap_err();
            assert!(matches!(err, ChannelError::ConnectionClosed));
            assert!(monitor.am_choking());

            let err = monitor.receive_incoming(Duration::from_secs(1)).await.unwrap_err();
            assert!(matches!(err, ChannelError::ConnectionClosed));
        };

        async_io::block_on(async move { join!(runner_fut, test_fut) });
    }
}
