use crate::storage::Error;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::{fs, io};

pub struct FileKeeper {
    files: BTreeMap<usize, fs::File>,
}

impl FileKeeper {
    const PLACEHOLDER_FILE: &'static str = "ignore_me.txt";

    pub fn new<I: Iterator<Item = (usize, PathBuf)>, P: AsRef<Path>>(
        parent_dir: P,
        length_path_it: I,
    ) -> Result<Self, Error> {
        let mut filemap = BTreeMap::new();
        let mut offset = 0usize;

        for (length, path) in length_path_it {
            let path = parent_dir.as_ref().join(path);
            if let Some(prefix) = path.parent() {
                fs::create_dir_all(prefix)?;
            }
            let file = fs::OpenOptions::new().write(true).read(true).create_new(true).open(path)?;
            file.set_len(length as u64)?;
            filemap.insert(offset, file);
            offset += length;
        }
        filemap.insert(offset, fs::File::create(FileKeeper::PLACEHOLDER_FILE)?);

        Ok(FileKeeper { files: filemap })
    }

    pub fn write_block(&self, global_offset: usize, block: Vec<u8>) -> Result<(), Error> {
        write_block_to(&self.files, global_offset, &block)?;
        Ok(())
    }

    pub fn read_block(&self, global_offset: usize, length: usize) -> Result<Vec<u8>, Error> {
        let mut dest = vec![0u8; length];
        read_block_from(&self.files, global_offset, &mut dest)?;
        Ok(dest)
    }
}

impl Drop for FileKeeper {
    fn drop(&mut self) {
        let _ = fs::remove_file(FileKeeper::PLACEHOLDER_FILE);
    }
}

trait RandomAccessReadWrite {
    fn read_at_offset(&self, dest: &mut [u8], offset: u64) -> io::Result<usize>;
    fn write_at_offset(&self, src: &[u8], offset: u64) -> io::Result<usize>;

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
}

fn find_file_and_offset<F>(
    all_files: &BTreeMap<usize, F>,
    global_offset: usize,
) -> Result<(usize, &F, usize), Error> {
    let next_start_offset = {
        let (offset, _) =
            all_files.range(global_offset + 1..).next().ok_or(Error::InvalidLocation)?;
        *offset
    };
    let (start_offset, file) = {
        let (offset, file) =
            all_files.range(..=global_offset).last().ok_or(Error::InvalidLocation)?;
        (*offset, file)
    };
    Ok((start_offset, file, next_start_offset))
}

fn write_block_to<F: RandomAccessReadWrite>(
    dest: &BTreeMap<usize, F>,
    global_offset: usize,
    block: &[u8],
) -> Result<(), Error> {
    let (start_offset, file, next_start_offset) = find_file_and_offset(dest, global_offset)?;
    let local_offset = global_offset - start_offset;

    let available_space = next_start_offset - global_offset;
    if block.len() <= available_space {
        file.write_all_at_offset(block, local_offset as u64)?;
        Ok(())
    } else {
        let (left, right) = block.split_at(available_space);
        file.write_all_at_offset(left, local_offset as u64)?;
        write_block_to(dest, next_start_offset, right)
    }
}

fn read_block_from<F: RandomAccessReadWrite>(
    src: &BTreeMap<usize, F>,
    global_offset: usize,
    dest: &mut [u8],
) -> Result<(), Error> {
    let (start_offset, file, next_start_offset) = find_file_and_offset(src, global_offset)?;
    let local_offset = global_offset - start_offset;

    let available_space = next_start_offset - global_offset;
    if dest.len() <= available_space {
        file.read_all_at_offset(dest, local_offset as u64)?;
        Ok(())
    } else {
        let (left, right) = dest.split_at_mut(available_space);
        file.read_all_at_offset(left, local_offset as u64)?;
        read_block_from(src, next_start_offset, right)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Read, Seek, SeekFrom, Write};

    type FakeFile = std::cell::RefCell<Cursor<Vec<u8>>>;

    fn fake_file_from(content: Vec<u8>) -> FakeFile {
        std::cell::RefCell::new(Cursor::new(content))
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
    }

    #[test]
    fn test_write_piece_within_one_file() {
        let mut map = BTreeMap::<usize, FakeFile>::new();
        map.insert(0, fake_file_from(vec![0u8; 10]));
        map.insert(10, fake_file_from(vec![0u8; 10]));
        map.insert(20, fake_file_from(vec![0u8; 10]));
        map.insert(30, fake_file_from(Vec::<u8>::new()));

        write_block_to(&map, 16, vec![1u8, 2u8, 3u8, 4u8].as_ref()).unwrap();

        assert_eq!(&vec![0u8; 10], map.get(&0).unwrap().borrow().get_ref());
        assert_eq!(
            &vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 1u8, 2u8, 3u8, 4u8],
            map.get(&10).unwrap().borrow().get_ref()
        );
        assert_eq!(&vec![0u8; 10], map.get(&20).unwrap().borrow().get_ref());
    }

    #[test]
    fn test_write_piece_on_file_boundary() {
        let mut map = BTreeMap::<usize, FakeFile>::new();
        map.insert(0, fake_file_from(vec![0u8; 10]));
        map.insert(10, fake_file_from(vec![0u8; 10]));
        map.insert(20, fake_file_from(vec![0u8; 10]));
        map.insert(30, fake_file_from(Vec::<u8>::new()));

        write_block_to(&map, 17, vec![1u8, 2u8, 3u8, 4u8, 5u8].as_ref()).unwrap();

        assert_eq!(&vec![0u8; 10], map.get(&0).unwrap().borrow().get_ref());
        assert_eq!(
            &vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 1u8, 2u8, 3u8],
            map.get(&10).unwrap().borrow().get_ref()
        );
        assert_eq!(
            &vec![4u8, 5u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8],
            map.get(&20).unwrap().borrow().get_ref()
        );
    }

    #[test]
    fn test_read_piece_within_one_file() {
        let mut map = BTreeMap::<usize, FakeFile>::new();
        map.insert(0, fake_file_from((0u8..10u8).collect()));
        map.insert(10, fake_file_from((0u8..10u8).collect()));
        map.insert(20, fake_file_from((0u8..10u8).collect()));
        map.insert(30, fake_file_from(Vec::<u8>::new()));

        let mut dest = vec![0u8; 3];
        read_block_from(&map, 12, &mut dest).unwrap();

        assert_eq!(vec![2u8, 3u8, 4u8], dest);

        assert_eq!(&(0u8..10u8).collect::<Vec<u8>>(), map.get(&0).unwrap().borrow().get_ref());
        assert_eq!(&(0u8..10u8).collect::<Vec<u8>>(), map.get(&10).unwrap().borrow().get_ref());
        assert_eq!(&(0u8..10u8).collect::<Vec<u8>>(), map.get(&20).unwrap().borrow().get_ref());
    }

    #[test]
    fn test_read_piece_on_file_boundary() {
        let mut map = BTreeMap::<usize, FakeFile>::new();
        map.insert(0, fake_file_from((1u8..=10u8).collect()));
        map.insert(10, fake_file_from((1u8..=10u8).collect()));
        map.insert(20, fake_file_from(Vec::<u8>::new()));

        let mut dest = vec![0u8; 3];
        read_block_from(&map, 8, &mut dest).unwrap();

        assert_eq!(vec![9u8, 10u8, 1u8], dest);

        assert_eq!(&(1u8..=10u8).collect::<Vec<u8>>(), map.get(&0).unwrap().borrow().get_ref());
        assert_eq!(&(1u8..=10u8).collect::<Vec<u8>>(), map.get(&10).unwrap().borrow().get_ref());
    }
}
