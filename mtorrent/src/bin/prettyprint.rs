use mtorrent_utils::benc;
use std::{env, fs, io};

fn main() -> io::Result<()> {
    let metainfo_path = env::args()
        .nth(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no input file specified"))?;

    let metainfo_content =
        fs::read(metainfo_path).inspect_err(|_| eprintln!("Couldn't read input file"))?;

    let entity = benc::Element::from_bytes(&metainfo_content)
        .inspect_err(|_| eprintln!("Invalid bencode"))?;

    println!("{entity}");
    Ok(())
}
