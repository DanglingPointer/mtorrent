use super::message::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnyMessage {
    Uploader(UploaderMessage),
    Downloader(DownloaderMessage),
    Extended((ExtendedMessage, u8)),
}
impl From<UploaderMessage> for AnyMessage {
    fn from(msg: UploaderMessage) -> Self {
        Self::Uploader(msg)
    }
}
impl From<DownloaderMessage> for AnyMessage {
    fn from(msg: DownloaderMessage) -> Self {
        Self::Downloader(msg)
    }
}
impl From<ExtendedMessage> for AnyMessage {
    fn from(msg: ExtendedMessage) -> Self {
        let id = match msg {
            ExtendedMessage::Handshake(_) => 0,
            ExtendedMessage::PeerExchange(_) => Extension::PeerExchange.local_id(),
            _ => Extension::Metadata.local_id(),
        };
        Self::Extended((msg, id))
    }
}

impl AnyMessage {
    pub fn write_to_buffer(self, buffer: &mut Vec<u8>) {
        match self {
            Self::Uploader(msg) => PeerMessage::from(msg).encode(buffer).unwrap(),
            Self::Downloader(msg) => PeerMessage::from(msg).encode(buffer).unwrap(),
            Self::Extended(msg) => PeerMessage::from(msg).encode(buffer).unwrap(),
        }
    }
}

/// ```no_run
/// # use mtorrent_core::msgs;
/// # use mtorrent_core::pwp::*;
/// msgs![DownloaderMessage::Interested, DownloaderMessage::NotInterested];
/// ```
#[macro_export]
macro_rules! msgs {
    ($($arg:expr),+ $(,)? ) => {{
        let mut buffer = Vec::new();
        $($crate::pwp::testutils::AnyMessage::write_to_buffer($arg.into(), &mut buffer);)+
        buffer
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_msgs() {
        let msgs = msgs![
            DownloaderMessage::Interested,
            UploaderMessage::Have { piece_index: 42 },
            ExtendedMessage::MetadataReject { piece: 41 }
        ];

        let expected = {
            let extended_content = b"d8:msg_typei2e5:piecei41ee";
            #[rustfmt::skip]
            let mut tmp = vec![
                0, 0, 0, 1,  // len Interested
                2,           // id Interested
                0, 0, 0, 5,  // len Have
                4,           // id Have
                0, 0, 0, 42, // piece_index
                0, 0, 0, (extended_content.len() + 2) as u8, // len MetadataReject
                20, // id Extended
                1,  // (local) id Metadata
            ];
            tmp.extend_from_slice(extended_content);
            tmp
        };
        assert_eq!(msgs, expected);
    }
}
