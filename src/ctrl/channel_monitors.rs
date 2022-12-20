use crate::pwp::{
    Bitfield, BlockInfo, ChannelError, DownloadChannel, DownloaderMessage, UploadChannel,
    UploaderMessage,
};
use log::info;
use std::cell::Cell;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Default, Eq, PartialEq, Debug)]
pub struct DownloadChannelStateUpdate {
    pub received_blocks: Vec<(BlockInfo, Vec<u8>)>,
    pub received_haves: Vec<usize>,
    pub received_bitfields: Vec<Bitfield>,
    pub bytes_downloaded: usize,
}

pub struct DownloadChannelMonitor {
    channel: DownloadChannel,
    am_interested: bool,
    peer_choking: bool,
    state_update: Cell<DownloadChannelStateUpdate>,
}

impl DownloadChannelMonitor {
    pub fn new(channel: DownloadChannel) -> Self {
        Self {
            channel,
            am_interested: false,
            peer_choking: true,
            state_update: Cell::new(Default::default()),
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
                self.am_interested = true;
            } else if not_interested {
                self.am_interested = false;
            }
        }
        Ok(())
    }

    pub async fn receive_incoming(&mut self, timeout: Duration) -> Result<(), ChannelError> {
        // returns error on timeout!
        let msg = self.channel.receive_message_timed(timeout).await?;
        info!("{} Message received: {}", self.remote_ip(), &msg);
        match msg {
            UploaderMessage::Choke => self.peer_choking = true,
            UploaderMessage::Unchoke => self.peer_choking = false,
            UploaderMessage::Have { piece_index } => {
                self.state_update.get_mut().received_haves.push(piece_index)
            }
            UploaderMessage::Bitfield(bitfield) => {
                self.state_update.get_mut().received_bitfields.push(bitfield);
            }
            UploaderMessage::Block(info, data) => {
                let mut state = self.state_update.get_mut();
                state.bytes_downloaded += data.len();
                state.received_blocks.push((info, data));
            }
        }
        Ok(())
    }

    pub fn am_interested(&self) -> bool {
        self.am_interested
    }

    pub fn peer_choking(&self) -> bool {
        self.peer_choking
    }

    pub fn last_update(&self) -> DownloadChannelStateUpdate {
        self.state_update.take()
    }
}

#[derive(Default, Eq, PartialEq, Debug)]
pub struct UploadChannelStateUpdate {
    pub received_requests: Vec<BlockInfo>,
    pub received_cancels: Vec<BlockInfo>,
    pub bytes_uploaded: usize,
}

pub struct UploadChannelMonitor {
    channel: UploadChannel,
    am_choking: bool,
    peer_interested: bool,
    state_update: Cell<UploadChannelStateUpdate>,
}

impl UploadChannelMonitor {
    pub fn new(channel: UploadChannel) -> Self {
        Self {
            channel,
            am_choking: true,
            peer_interested: false,
            state_update: Default::default(),
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
                self.am_choking = true;
            } else if unchoking {
                self.am_choking = false;
            }
        }
        Ok(())
    }

    pub async fn receive_incoming(&mut self, timeout: Duration) -> Result<(), ChannelError> {
        // returns error on timeout!
        let msg = self.channel.receive_message_timed(timeout).await?;
        info!("{} Message received: {}", self.remote_ip(), &msg);
        match msg {
            DownloaderMessage::Interested => self.peer_interested = true,
            DownloaderMessage::NotInterested => self.peer_interested = false,
            DownloaderMessage::Request(info) => {
                self.state_update.get_mut().received_requests.push(info)
            }
            DownloaderMessage::Cancel(info) => {
                self.state_update.get_mut().received_cancels.push(info)
            }
        }
        Ok(())
    }

    pub fn am_choking(&self) -> bool {
        self.am_choking
    }

    pub fn peer_interested(&self) -> bool {
        self.peer_interested
    }

    pub fn last_update(&self) -> UploadChannelStateUpdate {
        self.state_update.take()
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
    ) -> (
        DownloadChannelMonitor,
        mpsc::Receiver<Option<DownloaderMessage>>,
        mpsc::Sender<UploaderMessage>,
    ) {
        let (tx_msg_sender, tx_msg_receiver) = mpsc::channel::<Option<DownloaderMessage>>(0);
        let (rx_msg_sender, rx_msg_receiver) = mpsc::channel::<UploaderMessage>(0);

        let channel = DownloadChannel::new(rx_msg_receiver, tx_msg_sender, remote_ip);
        let monitor = DownloadChannelMonitor::new(channel);
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
        let (mut monitor, mut local_msgs, mut remote_msgs) = create_download_monitor(remote_ip);
        assert!(!monitor.am_interested());
        assert!(monitor.peer_choking());
        assert_eq!(remote_ip, *monitor.remote_ip());
        assert_eq!(DownloadChannelStateUpdate::default(), monitor.last_update());

        let block = BlockInfo {
            piece_index: 0,
            in_piece_offset: 8,
            block_length: 8,
        };
        let block_copy = block.clone();

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
            assert_eq!(vec![0usize], monitor.last_update().received_haves);

            monitor.receive_incoming(Duration::from_secs(1)).await.unwrap();
            let mut last_update = monitor.last_update();
            assert_eq!(1, last_update.received_blocks.len());

            let (info, data) = last_update.received_blocks.pop().unwrap();
            assert_eq!(block_copy, info);
            assert_eq!(vec![42u8; 20], data);

            fn new_request() -> DownloaderMessage {
                DownloaderMessage::Request(BlockInfo {
                    piece_index: 0,
                    in_piece_offset: 0,
                    block_length: 0,
                })
            }

            monitor.send_outgoing(repeat_with(new_request).take(2)).await.unwrap();

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
        let last_update = monitor.last_update();
        assert!(last_update.received_requests.is_empty());
        assert!(last_update.received_cancels.is_empty());

        let block = BlockInfo {
            piece_index: 0,
            in_piece_offset: 8,
            block_length: 8,
        };
        let block_copy = block.clone();

        let runner_fut = async move {
            remote_msgs.send(DownloaderMessage::Interested).await.unwrap();
            remote_msgs.send(DownloaderMessage::NotInterested).await.unwrap();
            local_msgs.next().await.unwrap();
            local_msgs.next().await.unwrap();
            local_msgs.next().await.unwrap();
            local_msgs.next().await.unwrap();
            remote_msgs.send(DownloaderMessage::Request(block.clone())).await.unwrap();
            remote_msgs.send(DownloaderMessage::Cancel(block.clone())).await.unwrap();
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
            assert_eq!(vec![block_copy.clone()], monitor.last_update().received_requests);

            monitor.receive_incoming(Duration::from_secs(1)).await.unwrap();
            assert_eq!(vec![block_copy.clone()], monitor.last_update().received_cancels);

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
