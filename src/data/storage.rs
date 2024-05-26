use crate::data::Error;
use crate::{pwp, warn_stopwatch};
use sha1_smol::Sha1;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::{cmp, error, fs, io};
use tokio::sync::{mpsc, oneshot};

pub type StorageServer = GenericStorageServer<fs::File>;

pub struct GenericStorageServer<F: RandomAccessReadWrite> {
    channel: mpsc::UnboundedReceiver<Command>,
    storage: GenericStorage<F>,
}

#[derive(Clone)]
pub struct StorageClient {
    channel: mpsc::UnboundedSender<Command>,
}

pub fn new_async_storage(
    parent_dir: impl AsRef<Path>,
    length_path_it: impl Iterator<Item = (usize, PathBuf)>,
) -> Result<(StorageClient, StorageServer), Error> {
    let storage = Storage::new(parent_dir, length_path_it)?;
    Ok(async_generic_storage(storage))
}

fn async_generic_storage<F: RandomAccessReadWrite>(
    storage: GenericStorage<F>,
) -> (StorageClient, GenericStorageServer<F>) {
    let (tx, rx) = mpsc::unbounded_channel::<Command>();
    (
        StorageClient { channel: tx },
        GenericStorageServer {
            channel: rx,
            storage,
        },
    )
}

#[cfg(test)]
pub(crate) fn new_mock_storage(total_size: usize) -> StorageClient {
    let (tx, mut rx) = mpsc::unbounded_channel::<Command>();
    tokio::task::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::WriteBlock {
                    global_offset,
                    data,
                    callback,
                } => {
                    if let Some(cb) = callback {
                        cb.send(if global_offset + data.len() < total_size {
                            Ok(())
                        } else {
                            Err(Error::InvalidLocation)
                        })
                        .unwrap();
                    }
                }
                Command::ReadBlock {
                    global_offset,
                    length,
                    callback,
                } => {
                    callback
                        .send(if global_offset + length < total_size {
                            Ok(vec![0; length])
                        } else {
                            Err(Error::InvalidLocation)
                        })
                        .unwrap();
                }
                Command::VerifyBlock {
                    global_offset,
                    length,
                    expected_sha1: _,
                    callback,
                } => {
                    callback
                        .send(if global_offset + length < total_size {
                            Ok(true)
                        } else {
                            Err(Error::InvalidLocation)
                        })
                        .unwrap();
                }
            }
        }
    });
    StorageClient { channel: tx }
}

impl<F: RandomAccessReadWrite> GenericStorageServer<F> {
    pub async fn run(mut self) {
        while let Some(cmd) = self.channel.recv().await {
            self.handle_cmd(cmd);
        }
    }

    pub fn run_blocking(mut self) {
        while let Some(cmd) = self.channel.blocking_recv() {
            self.handle_cmd(cmd);
        }
    }

    fn handle_cmd(&self, cmd: Command) {
        match cmd {
            Command::WriteBlock {
                global_offset,
                data,
                callback,
            } => {
                let result = self.storage.write_block(global_offset, data);
                if let Some(callback) = callback {
                    let _ = callback.send(result);
                }
            }
            Command::ReadBlock {
                global_offset,
                length,
                callback,
            } => {
                let result = self.storage.read_block(global_offset, length);
                let _ = callback.send(result);
            }
            Command::VerifyBlock {
                global_offset,
                length,
                expected_sha1,
                callback,
            } => {
                let end = global_offset + length;
                let mut buffer = [0u8; pwp::MAX_BLOCK_SIZE];
                let mut sha1 = Sha1::new();
                let result = (global_offset..end)
                    .step_by(buffer.len())
                    .try_for_each(|offset| {
                        let bytes_to_read = cmp::min(buffer.len(), end - offset);
                        let dest = &mut buffer[..bytes_to_read];
                        self.storage.read_block_at(offset, dest).map(|_| sha1.update(dest))
                    })
                    .map(|_| {
                        let computed_sha1: [u8; 20] = sha1.digest().bytes();
                        computed_sha1 == expected_sha1
                    });
                let _ = callback.send(result);
            }
        }
    }
}

impl StorageClient {
    pub async fn write_block(&self, global_offset: usize, data: Vec<u8>) -> Result<(), Error> {
        let (result_sender, result_receiver) = oneshot::channel::<WriteResult>();
        self.channel
            .send(Command::WriteBlock {
                global_offset,
                data,
                callback: Some(result_sender),
            })
            .map_err(Self::to_io_error_other)?;
        result_receiver.await.map_err(Self::to_io_error_other)?
    }

    pub fn start_write_block(&self, global_offset: usize, data: Vec<u8>) -> Result<(), Error> {
        self.channel
            .send(Command::WriteBlock {
                global_offset,
                data,
                callback: None,
            })
            .map_err(Self::to_io_error_other)?;
        Ok(())
    }

    pub async fn read_block(&self, global_offset: usize, length: usize) -> Result<Vec<u8>, Error> {
        let (result_sender, result_receiver) = oneshot::channel::<ReadResult>();
        self.channel
            .send(Command::ReadBlock {
                global_offset,
                length,
                callback: result_sender,
            })
            .map_err(Self::to_io_error_other)?;
        result_receiver.await.map_err(Self::to_io_error_other)?
    }

    pub async fn verify_block(
        &self,
        global_offset: usize,
        length: usize,
        expected_sha1: &[u8; 20],
    ) -> Result<bool, Error> {
        let _sw = warn_stopwatch!("Verification of {length} bytes");
        let (result_sender, result_receiver) = oneshot::channel::<VerifyResult>();
        self.channel
            .send(Command::VerifyBlock {
                global_offset,
                length,
                expected_sha1: *expected_sha1,
                callback: result_sender,
            })
            .map_err(Self::to_io_error_other)?;
        result_receiver.await.map_err(Self::to_io_error_other)?
    }

    fn to_io_error_other<E: error::Error + Send + Sync + 'static>(e: E) -> io::Error {
        io::Error::new(io::ErrorKind::Other, Box::new(e))
    }
}

type WriteResult = Result<(), Error>;
type ReadResult = Result<Vec<u8>, Error>;
type VerifyResult = Result<bool, Error>;

#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
enum Command {
    WriteBlock {
        global_offset: usize,
        data: Vec<u8>,
        callback: Option<oneshot::Sender<WriteResult>>,
    },
    ReadBlock {
        global_offset: usize,
        length: usize,
        callback: oneshot::Sender<ReadResult>,
    },
    VerifyBlock {
        global_offset: usize,
        length: usize,
        expected_sha1: [u8; 20],
        callback: oneshot::Sender<VerifyResult>,
    },
}

// ------

pub type Storage = GenericStorage<fs::File>;

pub struct GenericStorage<F: RandomAccessReadWrite> {
    files: BTreeMap<usize, F>,
}

impl Storage {
    pub fn new<I: Iterator<Item = (usize, PathBuf)>, P: AsRef<Path>>(
        parent_dir: P,
        length_path_it: I,
    ) -> Result<Self, Error> {
        let open_file = |(length, path): (usize, PathBuf)| -> io::Result<(usize, fs::File)> {
            let path = parent_dir.as_ref().join(path);
            if let Some(prefix) = path.parent() {
                fs::create_dir_all(prefix)?;
            }
            let file = fs::OpenOptions::new()
                .write(true)
                .read(true)
                .create(true)
                .truncate(false)
                .open(path)?;
            file.set_len(length as u64)?;
            Ok((length, file))
        };

        Self::from_length_file_pairs(length_path_it.map(open_file))
    }
}

impl<F: RandomAccessReadWrite> GenericStorage<F> {
    fn from_length_file_pairs<I: Iterator<Item = io::Result<(usize, F)>>>(
        length_file_it: I,
    ) -> Result<Self, Error> {
        let mut filemap = BTreeMap::new();
        let mut offset = 0usize;

        for result in length_file_it {
            let (length, file) = result?;
            filemap.insert(offset, file);
            offset += length;
        }
        if let Some((_offset, file)) = filemap.last_key_value() {
            let fd_clone = file.try_clone()?;
            filemap.insert(offset, fd_clone);
        }
        Ok(Self { files: filemap })
    }

    pub fn write_block(&self, global_offset: usize, block: Vec<u8>) -> Result<(), Error> {
        self.write_block_at(global_offset, &block)?;
        Ok(())
    }

    pub fn read_block(&self, global_offset: usize, length: usize) -> Result<Vec<u8>, Error> {
        let mut dest = vec![0u8; length];
        self.read_block_at(global_offset, &mut dest)?;
        Ok(dest)
    }

    fn find_file_and_offset(&self, global_offset: usize) -> Result<(usize, &F, usize), Error> {
        let next_start_offset = {
            let (offset, _) =
                self.files.range(global_offset + 1..).next().ok_or(Error::InvalidLocation)?;
            *offset
        };
        let (start_offset, file) = {
            let (offset, file) =
                self.files.range(..=global_offset).last().ok_or(Error::InvalidLocation)?;
            (*offset, file)
        };
        Ok((start_offset, file, next_start_offset))
    }

    fn write_block_at(&self, global_offset: usize, block: &[u8]) -> Result<(), Error> {
        let (start_offset, file, next_start_offset) = self.find_file_and_offset(global_offset)?;
        let local_offset = global_offset - start_offset;

        let available_space = next_start_offset - global_offset;
        if block.len() <= available_space {
            file.write_all_at_offset(block, local_offset as u64)?;
            Ok(())
        } else {
            let (left, right) = block.split_at(available_space);
            file.write_all_at_offset(left, local_offset as u64)?;
            self.write_block_at(next_start_offset, right)
        }
    }

    fn read_block_at(&self, global_offset: usize, dest: &mut [u8]) -> Result<(), Error> {
        let (start_offset, file, next_start_offset) = self.find_file_and_offset(global_offset)?;
        let local_offset = global_offset - start_offset;

        let available_space = next_start_offset - global_offset;
        if dest.len() <= available_space {
            file.read_all_at_offset(dest, local_offset as u64)?;
            Ok(())
        } else {
            let (left, right) = dest.split_at_mut(available_space);
            file.read_all_at_offset(left, local_offset as u64)?;
            self.read_block_at(next_start_offset, right)
        }
    }
}

pub trait RandomAccessReadWrite {
    fn read_at_offset(&self, dest: &mut [u8], offset: u64) -> io::Result<usize>;
    fn write_at_offset(&self, src: &[u8], offset: u64) -> io::Result<usize>;
    fn try_clone(&self) -> io::Result<Self>
    where
        Self: Sized;

    fn read_all_at_offset(&self, mut dest: &mut [u8], mut offset: u64) -> io::Result<()> {
        while !dest.is_empty() {
            let bytes_read = self.read_at_offset(dest, offset)?;
            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            dest = &mut dest[bytes_read..];
            offset += bytes_read as u64;
        }
        Ok(())
    }
    fn write_all_at_offset(&self, mut src: &[u8], mut offset: u64) -> io::Result<()> {
        while !src.is_empty() {
            let bytes_written = self.write_at_offset(src, offset)?;
            if bytes_written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            src = &src[bytes_written..];
            offset += bytes_written as u64;
        }
        Ok(())
    }
}

#[cfg(target_family = "unix")]
impl RandomAccessReadWrite for fs::File {
    fn read_at_offset(&self, dest: &mut [u8], offset: u64) -> io::Result<usize> {
        use std::os::unix::prelude::*;
        self.read_at(dest, offset)
    }

    fn write_at_offset(&self, src: &[u8], offset: u64) -> io::Result<usize> {
        use std::os::unix::prelude::*;
        self.write_at(src, offset)
    }

    fn try_clone(&self) -> io::Result<Self>
    where
        Self: Sized,
    {
        self.try_clone()
    }
}

#[cfg(target_family = "windows")]
impl RandomAccessReadWrite for fs::File {
    fn read_at_offset(&self, dest: &mut [u8], offset: u64) -> io::Result<usize> {
        use std::os::windows::prelude::*;
        self.seek_read(dest, offset)
    }

    fn write_at_offset(&self, src: &[u8], offset: u64) -> io::Result<usize> {
        use std::os::windows::prelude::*;
        self.seek_write(src, offset)
    }

    fn try_clone(&self) -> io::Result<Self>
    where
        Self: Sized,
    {
        self.try_clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Read, Seek, SeekFrom, Write};
    use std::iter;
    use tokio::task;

    type FakeFile = std::cell::RefCell<Cursor<Vec<u8>>>;

    fn fake_length_file_pair(content: Vec<u8>) -> io::Result<(usize, FakeFile)> {
        Ok((content.len(), std::cell::RefCell::new(Cursor::new(content))))
    }

    impl RandomAccessReadWrite for FakeFile {
        fn read_at_offset(&self, dest: &mut [u8], offset: u64) -> io::Result<usize> {
            self.borrow_mut().seek(SeekFrom::Start(offset))?;
            self.borrow_mut().read(dest)
        }

        fn write_at_offset(&self, src: &[u8], offset: u64) -> io::Result<usize> {
            self.borrow_mut().seek(SeekFrom::Start(offset))?;
            self.borrow_mut().write(src)
        }

        fn try_clone(&self) -> io::Result<Self>
        where
            Self: Sized,
        {
            Ok(self.clone())
        }
    }

    #[test]
    fn test_write_piece_within_one_file() {
        let s = GenericStorage::from_length_file_pairs(
            iter::repeat_with(|| fake_length_file_pair(vec![0u8; 10])).take(3),
        )
        .unwrap();

        s.write_block(16, vec![1u8, 2u8, 3u8, 4u8]).unwrap();

        assert_eq!(&vec![0u8; 10], s.files.get(&0).unwrap().borrow().get_ref());
        assert_eq!(
            &vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 1u8, 2u8, 3u8, 4u8],
            s.files.get(&10).unwrap().borrow().get_ref()
        );
        assert_eq!(&vec![0u8; 10], s.files.get(&20).unwrap().borrow().get_ref());
    }

    #[test]
    fn test_write_piece_on_file_boundary() {
        let s = GenericStorage::from_length_file_pairs(
            iter::repeat_with(|| fake_length_file_pair(vec![0u8; 10])).take(3),
        )
        .unwrap();

        s.write_block(17, vec![1u8, 2u8, 3u8, 4u8, 5u8]).unwrap();

        assert_eq!(&vec![0u8; 10], s.files.get(&0).unwrap().borrow().get_ref());
        assert_eq!(
            &vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 1u8, 2u8, 3u8],
            s.files.get(&10).unwrap().borrow().get_ref()
        );
        assert_eq!(
            &vec![4u8, 5u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8],
            s.files.get(&20).unwrap().borrow().get_ref()
        );
    }

    #[test]
    fn test_read_piece_within_one_file() {
        let s = GenericStorage::from_length_file_pairs(
            iter::repeat_with(|| fake_length_file_pair((0u8..10u8).collect())).take(3),
        )
        .unwrap();

        let dest = s.read_block(12, 3).unwrap();
        assert_eq!(vec![2u8, 3u8, 4u8], dest);

        assert_eq!(&(0u8..10u8).collect::<Vec<u8>>(), s.files.get(&0).unwrap().borrow().get_ref());
        assert_eq!(&(0u8..10u8).collect::<Vec<u8>>(), s.files.get(&10).unwrap().borrow().get_ref());
        assert_eq!(&(0u8..10u8).collect::<Vec<u8>>(), s.files.get(&20).unwrap().borrow().get_ref());
    }

    #[test]
    fn test_read_piece_on_file_boundary() {
        let s = GenericStorage::from_length_file_pairs(
            iter::repeat_with(|| fake_length_file_pair((1u8..=10u8).collect())).take(2),
        )
        .unwrap();

        let dest = s.read_block(8, 3).unwrap();
        assert_eq!(vec![9u8, 10u8, 1u8], dest);

        assert_eq!(&(1u8..=10u8).collect::<Vec<u8>>(), s.files.get(&0).unwrap().borrow().get_ref());
        assert_eq!(
            &(1u8..=10u8).collect::<Vec<u8>>(),
            s.files.get(&10).unwrap().borrow().get_ref()
        );
    }

    #[test]
    fn test_past_the_end_read_fails() {
        let s = GenericStorage::from_length_file_pairs(iter::once(fake_length_file_pair(
            (1u8..=10u8).collect(),
        )))
        .unwrap();
        assert!(matches!(s.read_block(5, 10), Err(Error::InvalidLocation)));
        assert!(matches!(s.read_block(11, 5), Err(Error::InvalidLocation)));
    }

    #[tokio::test]
    async fn test_async_detached_write_then_read_on_file_boundary() {
        task::LocalSet::new()
            .run_until(async {
                let s = GenericStorage::from_length_file_pairs(
                    iter::repeat_with(|| fake_length_file_pair(vec![0u8; 10])).take(2),
                )
                .unwrap();

                let (client, server) = async_generic_storage(s);

                task::spawn_local(async move {
                    server.run().await;
                });

                // given
                let initial_data = client.read_block(8, 3).await.unwrap();
                assert_eq!(vec![0u8, 0u8, 0u8], initial_data);

                // when
                client.start_write_block(8, vec![9u8, 10u8, 1u8]).unwrap();

                // then
                let final_data = client.read_block(8, 3).await.unwrap();
                assert_eq!(vec![9u8, 10u8, 1u8], final_data);
            })
            .await;
    }

    #[tokio::test]
    async fn test_async_verify_block() {
        let sha1_0_10 =
            b"\x49\x41\x79\x71\x4a\x6c\xd6\x27\x23\x9d\xfe\xde\xdf\x2d\xe9\xef\x99\x4c\xaf\x03";
        let sha1_10_20 =
            b"\xdd\xd1\x27\x8d\x28\xaf\x87\xc7\x58\x84\xf5\x5b\x71\xfb\xb4\xa1\x23\x1a\xf2\xe5";
        task::LocalSet::new()
            .run_until(async {
                let s = GenericStorage::from_length_file_pairs(iter::once(fake_length_file_pair(
                    (0u8..20u8).collect(),
                )))
                .unwrap();

                let (client, server) = async_generic_storage(s);

                task::spawn_local(async move {
                    server.run().await;
                });

                let verify_success = client.verify_block(0, 10, sha1_0_10).await.unwrap();
                assert!(verify_success);

                let verify_success = client.verify_block(0, 10, sha1_10_20).await.unwrap();
                assert!(!verify_success);

                let verify_success = client.verify_block(10, 10, sha1_10_20).await.unwrap();
                assert!(verify_success);

                let verify_success = client.verify_block(10, 10, sha1_0_10).await.unwrap();
                assert!(!verify_success);
            })
            .await;
    }
}
