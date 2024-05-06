use super::message::*;
use futures::FutureExt;
use std::io;
use tokio::io::BufWriter;

pub(crate) enum AnyMessage {
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
    pub(crate) fn write_to_buffer(self, buffer: &mut BufWriter<io::Cursor<Vec<u8>>>) {
        fn write(msg: impl Into<PeerMessage>, buffer: &mut BufWriter<io::Cursor<Vec<u8>>>) {
            msg.into().write_to(buffer).now_or_never().unwrap().unwrap();
        }
        match self {
            Self::Uploader(msg) => write(msg, buffer),
            Self::Downloader(msg) => write(msg, buffer),
            Self::Extended(msg) => write(msg, buffer),
        }
    }
}

#[macro_export]
macro_rules! msgs {
    ($($arg:expr),+ $(,)? ) => {{
        let mut buffer = tokio::io::BufWriter::new(std::io::Cursor::<Vec<u8>>::default());
        $($crate::pwp::testutils::AnyMessage::write_to_buffer($arg.into(), &mut buffer);)+
        buffer.into_inner().into_inner()
    }};
}

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
