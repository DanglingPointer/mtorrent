use crate::storage::Error;
use std::collections::BTreeMap;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::{fs, io};

pub struct FileKeeper {
    files: BTreeMap<usize, fs::File>,
}

impl FileKeeper {
    const PLACEHOLDER_FILE: &'static str = "ignore_me.txt";

    pub fn new<I: Iterator<Item = (usize, PathBuf)>>(length_path_it: I) -> Result<Self, Error> {
        let mut filemap = BTreeMap::new();
        let mut offset = 0usize;

        for (length, path) in length_path_it {
            let file = fs::OpenOptions::new()
                .write(true)
                .read(true)
                .create_new(true)
                .open(path)?;
            file.set_len(length as u64)?;
            filemap.insert(offset, file);
            offset += length;
        }
        filemap.insert(offset, fs::File::create(FileKeeper::PLACEHOLDER_FILE)?);

        Ok(FileKeeper { files: filemap })
    }

    pub fn write_block(&mut self, global_offset: usize, block: Vec<u8>) -> Result<(), Error> {
        write_block_to(&mut self.files, global_offset, &block)?;
        Ok(())
    }

    pub fn read_block(&mut self, global_offset: usize, length: usize) -> Result<Vec<u8>, Error> {
        let mut dest = vec![0u8; length];
        read_block_from(&mut self.files, global_offset, &mut dest)?;
        Ok(dest)
    }
}

impl Drop for FileKeeper {
    fn drop(&mut self) {
        let _ = fs::remove_file(FileKeeper::PLACEHOLDER_FILE);
    }
}

fn find_file_and_offset<F>(
    all_files: &mut BTreeMap<usize, F>,
    global_offset: usize,
) -> Result<(usize, &mut F, usize), Error> {
    let next_start_offset = {
        let (offset, _) = all_files
            .range_mut(global_offset + 1..)
            .next()
            .ok_or(Error::InvalidLocation)?;
        *offset
    };
    let (start_offset, file) = {
        let (offset, file) = all_files
            .range_mut(..=global_offset)
            .last()
            .ok_or(Error::InvalidLocation)?;
        (*offset, file)
    };
    Ok((start_offset, file, next_start_offset))
}

fn write_block_to<F: io::Seek + io::Write>(
    dest: &mut BTreeMap<usize, F>,
    global_offset: usize,
    block: &[u8],
) -> Result<(), Error> {
    let (start_offset, file, next_start_offset) = find_file_and_offset(dest, global_offset)?;
    file.seek(SeekFrom::Start((global_offset - start_offset) as u64))?;

    let available_space = next_start_offset - global_offset;
    if block.len() <= available_space {
        file.write_all(block)?;
        file.flush()?;
        Ok(())
    } else {
        let (left, right) = block.split_at(available_space);
        file.write_all(left)?;
        file.flush()?;
        write_block_to(dest, next_start_offset, right)
    }
}

fn read_block_from<F: io::Seek + io::Read>(
    src: &mut BTreeMap<usize, F>,
    global_offset: usize,
    dest: &mut [u8],
) -> Result<(), Error> {
    let (start_offset, file, next_start_offset) = find_file_and_offset(src, global_offset)?;
    file.seek(SeekFrom::Start((global_offset - start_offset) as u64))?;

    let available_space = next_start_offset - global_offset;
    if dest.len() <= available_space {
        file.read_exact(dest)?;
        Ok(())
    } else {
        let (left, right) = dest.split_at_mut(available_space);
        file.read_exact(left)?;
        read_block_from(src, next_start_offset, right)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_write_piece_within_one_file() {
        let mut map = BTreeMap::<usize, Cursor<Vec<u8>>>::new();
        map.insert(0, Cursor::new(vec![0u8; 10]));
        map.insert(10, Cursor::new(vec![0u8; 10]));
        map.insert(20, Cursor::new(vec![0u8; 10]));
        map.insert(30, Cursor::new(Vec::<u8>::new()));

        write_block_to(&mut map, 16, vec![1u8, 2u8, 3u8, 4u8].as_ref()).unwrap();

        assert_eq!(&vec![0u8; 10], map.get(&0).unwrap().get_ref());
        assert_eq!(
            &vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 1u8, 2u8, 3u8, 4u8],
            map.get(&10).unwrap().get_ref()
        );
        assert_eq!(&vec![0u8; 10], map.get(&20).unwrap().get_ref());
    }

    #[test]
    fn test_write_piece_on_file_boundary() {
        let mut map = BTreeMap::<usize, Cursor<Vec<u8>>>::new();
        map.insert(0, Cursor::new(vec![0u8; 10]));
        map.insert(10, Cursor::new(vec![0u8; 10]));
        map.insert(20, Cursor::new(vec![0u8; 10]));
        map.insert(30, Cursor::new(Vec::<u8>::new()));

        write_block_to(&mut map, 17, vec![1u8, 2u8, 3u8, 4u8, 5u8].as_ref()).unwrap();

        assert_eq!(&vec![0u8; 10], map.get(&0).unwrap().get_ref());
        assert_eq!(
            &vec![0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 1u8, 2u8, 3u8],
            map.get(&10).unwrap().get_ref()
        );
        assert_eq!(
            &vec![4u8, 5u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8],
            map.get(&20).unwrap().get_ref()
        );
    }

    #[test]
    fn test_read_piece_within_one_file() {
        let mut map = BTreeMap::<usize, Cursor<Vec<u8>>>::new();
        map.insert(0, Cursor::new((0u8..10u8).collect()));
        map.insert(10, Cursor::new((0u8..10u8).collect()));
        map.insert(20, Cursor::new((0u8..10u8).collect()));
        map.insert(30, Cursor::new(Vec::<u8>::new()));

        let mut dest = vec![0u8; 3];
        read_block_from(&mut map, 12, &mut dest).unwrap();

        assert_eq!(vec![2u8, 3u8, 4u8], dest);

        assert_eq!(
            &(0u8..10u8).collect::<Vec<u8>>(),
            map.get(&0).unwrap().get_ref()
        );
        assert_eq!(
            &(0u8..10u8).collect::<Vec<u8>>(),
            map.get(&10).unwrap().get_ref()
        );
        assert_eq!(
            &(0u8..10u8).collect::<Vec<u8>>(),
            map.get(&20).unwrap().get_ref()
        );
    }

    #[test]
    fn test_read_piece_on_file_boundary() {
        let mut map = BTreeMap::<usize, Cursor<Vec<u8>>>::new();
        map.insert(0, Cursor::new((1u8..=10u8).collect()));
        map.insert(10, Cursor::new((1u8..=10u8).collect()));
        map.insert(20, Cursor::new(Vec::<u8>::new()));

        let mut dest = vec![0u8; 3];
        read_block_from(&mut map, 8, &mut dest).unwrap();

        assert_eq!(vec![9u8, 10u8, 1u8], dest);

        assert_eq!(
            &(1u8..=10u8).collect::<Vec<u8>>(),
            map.get(&0).unwrap().get_ref()
        );
        assert_eq!(
            &(1u8..=10u8).collect::<Vec<u8>>(),
            map.get(&10).unwrap().get_ref()
        )
    }
}
