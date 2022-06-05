use crate::peers::channels::{Channel, PeerChannel};
use crate::peers::{
    ChannelError, DownloadChannel, DownloaderMessage, UploadChannel, UploaderMessage,
};
use std::time::Duration;

pub struct ChannelMonitor<C: PeerChannel> {
    channel: C,
    pending_msg: Option<C::RxMessage>,
    pending_err: Option<ChannelError>,
    open_locally: bool,
}

impl<Rx, Tx> From<Channel<Rx, Tx>> for ChannelMonitor<Channel<Rx, Tx>> {
    fn from(channel: Channel<Rx, Tx>) -> Self {
        ChannelMonitor {
            channel,
            pending_msg: None,
            pending_err: None,
            open_locally: false,
        }
    }
}

impl<Rx, Tx> ChannelMonitor<Channel<Rx, Tx>> {
    pub fn channel(&self) -> &Channel<Rx, Tx> {
        &self.channel
    }
    pub fn take_pending_message(&mut self) -> Option<Rx> {
        self.pending_msg.take()
    }
    pub fn take_pending_error(&mut self) -> Option<ChannelError> {
        self.pending_err.take()
    }
    pub async fn handle_incoming(&mut self, timeout: Duration) {
        match self.channel.receive_message_timed(timeout).await {
            Ok(msg) => {
                assert!(self.pending_msg.is_none());
                self.pending_msg = Some(msg);
            }
            Err(e) => {
                assert!(self.pending_err.is_none());
                self.pending_err = Some(e);
            }
        }
    }
    async fn send_outgoing(&mut self, msg: Tx) -> bool {
        match self.channel.send_message(msg).await {
            Ok(_) => true,
            Err(e) => {
                assert!(self.pending_err.is_none());
                self.pending_err = Some(e);
                false
            }
        }
    }
}

impl ChannelMonitor<DownloadChannel> {
    pub fn interested(&self) -> bool {
        self.open_locally
    }
    pub async fn handle_outgoing(&mut self, msg: <DownloadChannel as PeerChannel>::TxMessage) {
        let interested = matches!(msg, DownloaderMessage::Interested);
        let not_interested = matches!(msg, DownloaderMessage::NotInterested);
        if self.send_outgoing(msg).await {
            if interested {
                self.open_locally = true;
            } else if not_interested {
                self.open_locally = false;
            }
        }
    }
}

impl ChannelMonitor<UploadChannel> {
    pub fn choking(&self) -> bool {
        !self.open_locally
    }
    pub async fn handle_outgoing(&mut self, msg: <UploadChannel as PeerChannel>::TxMessage) {
        let is_choke = matches!(msg, UploaderMessage::Choke);
        let is_unchoke = matches!(msg, UploaderMessage::Unchoke);
        if self.send_outgoing(msg).await {
            if is_choke {
                self.open_locally = false;
            } else if is_unchoke {
                self.open_locally = true;
            }
        }
    }
}

pub type DownloadMonitor = ChannelMonitor<DownloadChannel>;
pub type UploadMonitor = ChannelMonitor<UploadChannel>;

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::mpsc;
    use futures::join;
    use futures::prelude::*;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    #[test]
    fn test_downloader_pending_error() {
        let (tx_msg_sender, tx_msg_receiver) = mpsc::channel::<Option<DownloaderMessage>>(0);
        let (rx_msg_sender, rx_msg_receiver) = mpsc::channel::<UploaderMessage>(0);

        let channel = DownloadChannel::new(
            rx_msg_receiver,
            tx_msg_sender,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
        );
        let mut monitor = DownloadMonitor::from(channel);

        drop(rx_msg_sender);
        drop(tx_msg_receiver);

        assert!(!monitor.interested());

        async_io::block_on(async move {
            monitor.handle_outgoing(DownloaderMessage::Interested).await;
            assert!(!monitor.interested());
            assert!(matches!(monitor.take_pending_error(), Some(ChannelError::ConnectionClosed)));
            assert!(monitor.take_pending_error().is_none());

            monitor.handle_incoming(Duration::from_secs(1)).await;
            assert!(matches!(monitor.take_pending_error(), Some(ChannelError::ConnectionClosed)));
        });
    }

    #[test]
    fn test_uploader_pending_error() {
        let (tx_msg_sender, tx_msg_receiver) = mpsc::channel::<Option<UploaderMessage>>(0);
        let (rx_msg_sender, rx_msg_receiver) = mpsc::channel::<DownloaderMessage>(0);

        let channel = UploadChannel::new(
            rx_msg_receiver,
            tx_msg_sender,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
        );
        let mut monitor = UploadMonitor::from(channel);

        drop(rx_msg_sender);
        drop(tx_msg_receiver);

        assert!(monitor.choking());

        async_io::block_on(async move {
            monitor.handle_outgoing(UploaderMessage::Unchoke).await;
            assert!(monitor.choking());
            assert!(matches!(monitor.take_pending_error(), Some(ChannelError::ConnectionClosed)));
            assert!(monitor.take_pending_error().is_none());

            monitor.handle_incoming(Duration::from_secs(1)).await;
            assert!(matches!(monitor.take_pending_error(), Some(ChannelError::ConnectionClosed)));
        });
    }

    #[test]
    fn test_downloader_exchanges_messages_and_updates_state() {
        let (tx_msg_sender, mut tx_msg_receiver) = mpsc::channel::<Option<DownloaderMessage>>(0);
        let (mut rx_msg_sender, rx_msg_receiver) = mpsc::channel::<UploaderMessage>(0);

        let channel = DownloadChannel::new(
            rx_msg_receiver,
            tx_msg_sender,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
        );
        let mut monitor = DownloadMonitor::from(channel);

        assert!(monitor.take_pending_error().is_none());
        assert!(monitor.take_pending_message().is_none());
        assert!(!monitor.interested());

        let runner_fut = async move {
            rx_msg_sender.send(UploaderMessage::Unchoke).await.unwrap();
            while tx_msg_receiver.next().await.is_some() {}
        };

        let test_fut = async move {
            monitor.handle_incoming(Duration::from_secs(1)).await;
            assert!(monitor.take_pending_error().is_none());
            assert!(matches!(monitor.take_pending_message(), Some(UploaderMessage::Unchoke)));

            monitor.handle_outgoing(DownloaderMessage::Interested).await;
            assert!(monitor.take_pending_error().is_none());
            assert!(monitor.take_pending_message().is_none());
            assert!(monitor.interested());

            monitor.handle_outgoing(DownloaderMessage::NotInterested).await;
            assert!(monitor.take_pending_error().is_none());
            assert!(monitor.take_pending_message().is_none());
            assert!(!monitor.interested());
        };

        async_io::block_on(async move { join!(runner_fut, test_fut) });
    }

    #[test]
    fn test_uploader_exchanges_messages_and_updates_state() {
        let (tx_msg_sender, mut tx_msg_receiver) = mpsc::channel::<Option<UploaderMessage>>(0);
        let (mut rx_msg_sender, rx_msg_receiver) = mpsc::channel::<DownloaderMessage>(0);

        let channel = UploadChannel::new(
            rx_msg_receiver,
            tx_msg_sender,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6666)),
        );
        let mut monitor = UploadMonitor::from(channel);

        assert!(monitor.take_pending_error().is_none());
        assert!(monitor.take_pending_message().is_none());
        assert!(monitor.choking());

        let runner_fut = async move {
            rx_msg_sender.send(DownloaderMessage::Interested).await.unwrap();
            while tx_msg_receiver.next().await.is_some() {}
        };

        let test_fut = async move {
            monitor.handle_incoming(Duration::from_secs(1)).await;
            assert!(monitor.take_pending_error().is_none());
            assert!(matches!(monitor.take_pending_message(), Some(DownloaderMessage::Interested)));

            monitor.handle_outgoing(UploaderMessage::Unchoke).await;
            assert!(monitor.take_pending_error().is_none());
            assert!(monitor.take_pending_message().is_none());
            assert!(!monitor.choking());

            monitor.handle_outgoing(UploaderMessage::Choke).await;
            assert!(monitor.take_pending_error().is_none());
            assert!(monitor.take_pending_message().is_none());
            assert!(monitor.choking());
        };

        async_io::block_on(async move { join!(runner_fut, test_fut) });
    }
}
