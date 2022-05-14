use bitvec::prelude::*;
use futures::prelude::*;
use std::io;

#[derive(Debug)]
pub enum PeerMessage {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have {
        piece_index: u32,
    },
    Bitfield {
        bitfield: BitVec<u8, Msb0>,
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
    pub async fn read_from<S: futures::AsyncReadExt + Unpin>(
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
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Unknown message type: {}", id),
            )),
        }
    }

    pub async fn write_to<S: futures::AsyncWriteExt + Unpin>(
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
