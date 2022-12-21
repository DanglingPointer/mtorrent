use bitvec::prelude::*;
use futures::prelude::*;
use std::{fmt, io};

pub type Bitfield = BitVec<u8, Msb0>;

#[derive(Debug)]
pub(super) enum PeerMessage {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have {
        piece_index: u32,
    },
    Bitfield {
        bitfield: Bitfield,
    },
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
    Port {
        listen_port: u16,
    },
}

impl PeerMessage {
    pub(super) async fn read_from<S: futures::AsyncReadExt + Unpin>(
        src: &mut S,
    ) -> io::Result<PeerMessage> {
        use PeerMessage::*;

        let msg_len = read_u32_from(src).await? as usize;

        if msg_len == 0 {
            return Ok(KeepAlive);
        }

        let id = {
            let mut id_byte = [0u8; 1];
            src.read_exact(&mut id_byte).await?;
            id_byte[0]
        };

        match id {
            ID_CHOKE => Ok(Choke),
            ID_UNCHOKE => Ok(Unchoke),
            ID_INTERESTED => Ok(Interested),
            ID_NOT_INTERESTED => Ok(NotInterested),
            ID_HAVE => Ok(Have {
                piece_index: read_u32_from(src).await?,
            }),
            ID_BITFIELD => {
                let mut bitfield_bytes = vec![0u8; msg_len - 1];
                src.read_exact(&mut bitfield_bytes).await?;
                Ok(Bitfield {
                    bitfield: BitVec::from_vec(bitfield_bytes),
                })
            }
            ID_REQUEST => {
                let index = read_u32_from(src).await?;
                let begin = read_u32_from(src).await?;
                let length = read_u32_from(src).await?;
                Ok(Request {
                    index,
                    begin,
                    length,
                })
            }
            ID_PIECE => {
                let index = read_u32_from(src).await?;
                let begin = read_u32_from(src).await?;
                let mut block = vec![0u8; msg_len - 9];
                src.read_exact(&mut block).await?;
                Ok(Piece {
                    index,
                    begin,
                    block,
                })
            }
            ID_CANCEL => {
                let index = read_u32_from(src).await?;
                let begin = read_u32_from(src).await?;
                let length = read_u32_from(src).await?;
                Ok(Cancel {
                    index,
                    begin,
                    length,
                })
            }
            ID_PORT => {
                let mut port_bytes = [0u8; 2];
                src.read_exact(&mut port_bytes).await?;
                Ok(Port {
                    listen_port: u16::from_be_bytes(port_bytes),
                })
            }
            _ => Err(io::Error::new(io::ErrorKind::Other, format!("Unknown message type: {}", id))),
        }
    }

    pub(super) async fn write_to<S: futures::AsyncWriteExt + Unpin>(
        &self,
        dest: &mut futures::io::BufWriter<S>,
    ) -> io::Result<()> {
        use PeerMessage::*;

        let length = (self.get_length() as u32).to_be_bytes();
        dest.write_all(&length).await?;

        match self {
            KeepAlive => (),
            Choke => {
                dest.write_all(&[ID_CHOKE]).await?;
            }
            Unchoke => {
                dest.write_all(&[ID_UNCHOKE]).await?;
            }
            Interested => {
                dest.write_all(&[ID_INTERESTED]).await?;
            }
            NotInterested => {
                dest.write_all(&[ID_NOT_INTERESTED]).await?;
            }
            Have { piece_index } => {
                dest.write_all(&[ID_HAVE]).await?;
                dest.write_all(&piece_index.to_be_bytes()).await?;
            }
            Bitfield { bitfield } => {
                dest.write_all(&[ID_BITFIELD]).await?;
                dest.write_all(bitfield.as_raw_slice()).await?;
            }
            Request {
                index,
                begin,
                length,
            } => {
                dest.write_all(&[ID_REQUEST]).await?;
                dest.write_all(&index.to_be_bytes()).await?;
                dest.write_all(&begin.to_be_bytes()).await?;
                dest.write_all(&length.to_be_bytes()).await?;
            }
            Piece {
                index,
                begin,
                block,
            } => {
                dest.write_all(&[ID_PIECE]).await?;
                dest.write_all(&index.to_be_bytes()).await?;
                dest.write_all(&begin.to_be_bytes()).await?;
                dest.write_all(block).await?;
            }
            Cancel {
                index,
                begin,
                length,
            } => {
                dest.write_all(&[ID_CANCEL]).await?;
                dest.write_all(&index.to_be_bytes()).await?;
                dest.write_all(&begin.to_be_bytes()).await?;
                dest.write_all(&length.to_be_bytes()).await?;
            }
            Port { listen_port } => {
                dest.write_all(&[ID_PORT]).await?;
                dest.write_all(&listen_port.to_be_bytes()).await?;
            }
        };
        dest.flush().await?;
        Ok(())
    }

    fn get_length(&self) -> usize {
        use PeerMessage::*;

        match self {
            KeepAlive => 0,
            Choke | Unchoke | Interested | NotInterested => 1,
            Have { .. } => 5,
            Bitfield { bitfield } => 1 + bitfield.as_raw_slice().len(),
            Request { .. } | Cancel { .. } => 13,
            Piece { block, .. } => 9 + block.len(),
            Port { .. } => 3,
        }
    }
}

const ID_CHOKE: u8 = 0;
const ID_UNCHOKE: u8 = 1;
const ID_INTERESTED: u8 = 2;
const ID_NOT_INTERESTED: u8 = 3;
const ID_HAVE: u8 = 4;
const ID_BITFIELD: u8 = 5;
const ID_REQUEST: u8 = 6;
const ID_PIECE: u8 = 7;
const ID_CANCEL: u8 = 8;
const ID_PORT: u8 = 9;

async fn read_u32_from<S: futures::AsyncReadExt + Unpin>(src: &mut S) -> io::Result<u32> {
    let mut bytes = [0u8; 4];
    src.read_exact(&mut bytes).await?;
    Ok(u32::from_be_bytes(bytes))
}

// ------

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct BlockInfo {
    pub piece_index: usize,
    pub in_piece_offset: usize,
    pub block_length: usize,
}

#[derive(Eq, PartialEq)]
pub enum UploaderMessage {
    Choke,
    Unchoke,
    Have {
        piece_index: usize,
    },
    Bitfield(Bitfield),
    Block(BlockInfo, Vec<u8>),
}

#[derive(Eq, PartialEq)]
pub enum DownloaderMessage {
    Interested,
    NotInterested,
    Request(BlockInfo),
    Cancel(BlockInfo),
}

impl fmt::Display for BlockInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ind={} off={} len={}", self.piece_index, self.in_piece_offset, self.block_length)
    }
}

impl From<UploaderMessage> for PeerMessage {
    fn from(msg: UploaderMessage) -> Self {
        match msg {
            UploaderMessage::Choke => PeerMessage::Choke,
            UploaderMessage::Unchoke => PeerMessage::Unchoke,
            UploaderMessage::Have { piece_index } => PeerMessage::Have {
                piece_index: piece_index as u32,
            },
            UploaderMessage::Bitfield(bitfield) => PeerMessage::Bitfield { bitfield },
            UploaderMessage::Block(info, data) => PeerMessage::Piece {
                index: info.piece_index as u32,
                begin: info.in_piece_offset as u32,
                block: data,
            },
        }
    }
}

impl TryFrom<PeerMessage> for UploaderMessage {
    type Error = PeerMessage;

    fn try_from(msg: PeerMessage) -> Result<Self, Self::Error> {
        match msg {
            PeerMessage::Choke => Ok(UploaderMessage::Choke),
            PeerMessage::Unchoke => Ok(UploaderMessage::Unchoke),
            PeerMessage::Have { piece_index } => Ok(UploaderMessage::Have {
                piece_index: piece_index as usize,
            }),
            PeerMessage::Bitfield { bitfield } => Ok(UploaderMessage::Bitfield(bitfield)),
            PeerMessage::Piece {
                index,
                begin,
                block,
            } => Ok(UploaderMessage::Block(
                BlockInfo {
                    piece_index: index as usize,
                    in_piece_offset: begin as usize,
                    block_length: block.len(),
                },
                block,
            )),
            _ => Err(msg),
        }
    }
}

impl fmt::Display for UploaderMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UploaderMessage::Choke => {
                write!(f, "Choke")
            }
            UploaderMessage::Unchoke => {
                write!(f, "Unchoke")
            }
            UploaderMessage::Have { piece_index } => {
                write!(f, "Have[ind={}]", piece_index)
            }
            UploaderMessage::Bitfield(bitvec) => {
                write!(f, "Bitfield[len={}]", bitvec.len())
            }
            UploaderMessage::Block(info, _) => {
                write!(f, "Block[{info}]")
            }
        }
    }
}

impl From<DownloaderMessage> for PeerMessage {
    fn from(msg: DownloaderMessage) -> Self {
        match msg {
            DownloaderMessage::Interested => PeerMessage::Interested,
            DownloaderMessage::NotInterested => PeerMessage::NotInterested,
            DownloaderMessage::Request(info) => PeerMessage::Request {
                index: info.piece_index as u32,
                begin: info.in_piece_offset as u32,
                length: info.block_length as u32,
            },
            DownloaderMessage::Cancel(info) => PeerMessage::Cancel {
                index: info.piece_index as u32,
                begin: info.in_piece_offset as u32,
                length: info.block_length as u32,
            },
        }
    }
}

impl TryFrom<PeerMessage> for DownloaderMessage {
    type Error = PeerMessage;

    fn try_from(msg: PeerMessage) -> Result<Self, Self::Error> {
        match msg {
            PeerMessage::Interested => Ok(DownloaderMessage::Interested),
            PeerMessage::NotInterested => Ok(DownloaderMessage::NotInterested),
            PeerMessage::Request {
                index,
                begin,
                length,
            } => Ok(DownloaderMessage::Request(BlockInfo {
                piece_index: index as usize,
                in_piece_offset: begin as usize,
                block_length: length as usize,
            })),
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => Ok(DownloaderMessage::Cancel(BlockInfo {
                piece_index: index as usize,
                in_piece_offset: begin as usize,
                block_length: length as usize,
            })),
            _ => Err(msg),
        }
    }
}

impl fmt::Display for DownloaderMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DownloaderMessage::Interested => {
                write!(f, "Interested")
            }
            DownloaderMessage::NotInterested => {
                write!(f, "NotInterested")
            }
            DownloaderMessage::Request(info) => {
                write!(f, "Request[{info}]")
            }
            DownloaderMessage::Cancel(info) => {
                write!(f, "Cancel[{info}]")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitfield_is_parsed_correctly() {
        let bits = b"\x82";
        let msg = UploaderMessage::Bitfield(BitVec::from_slice(bits));

        let bitfield = if let UploaderMessage::Bitfield(bitfield) = msg {
            bitfield
        } else {
            panic!()
        };

        assert!(bitfield[0]);
        assert!(!bitfield[1]);
        assert!(!bitfield[2]);
        assert!(!bitfield[3]);

        assert!(!bitfield[4]);
        assert!(!bitfield[5]);
        assert!(bitfield[6]);
        assert!(!bitfield[7]);
    }
}
