use mtorrent::utils::{benc, meta};
use sha1_smol::Sha1;
use std::{env, fs, str};

fn print_entity(entity: &benc::Element) {
    match entity {
        benc::Element::ByteString(data) => match str::from_utf8(data) {
            Ok(text) => print!(" {} ", text),
            Err(_) => print!(" <{} bytes> ", data.len()),
        },
        benc::Element::Integer(number) => print!(" {} ", number),
        benc::Element::List(elements) => {
            print!("[");
            for e in elements {
                print_entity(e);
                print!(",");
            }
            print!("]");
        }
        benc::Element::Dictionary(map) => {
            println!();
            print!("{{");
            for (key, val) in map {
                print!("<");
                print_entity(key);
                print!(":");
                print_entity(val);
                print!(">");
            }
            print!("}}");
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    println!("{:?}", args);

    let source = if args.len() >= 2 {
        args[1].to_owned()
    } else {
        "tests/example.torrent".to_string()
    };

    let dest = if args.len() >= 3 {
        args[2].to_owned()
    } else {
        "tests/zeroed.torrent".to_string()
    };

    let source_content = fs::read(source).unwrap();

    let entity = benc::Element::from_bytes(&source_content).unwrap();
    let metainfo = meta::Metainfo::try_from(entity).unwrap();

    let piece_count = metainfo.pieces().unwrap().count();
    let piece_length = metainfo.piece_length().unwrap();
    let total_length = metainfo
        .length()
        .or_else(|| metainfo.files().map(|it| it.map(|(len, _path)| len).sum()))
        .unwrap();
    let last_piece_length = total_length % piece_length;

    let zeroed_piece = vec![0u8; piece_length];
    let zeroed_last_piece = vec![0u8; last_piece_length];
    let piece_sha1: [u8; 20] = Sha1::from(zeroed_piece).digest().bytes();
    let last_piece_sha1: [u8; 20] = Sha1::from(zeroed_last_piece).digest().bytes();

    let mut entity = benc::Element::from_bytes(&source_content).unwrap();
    if let benc::Element::Dictionary(root_dict) = &mut entity {
        let info_key: benc::Element = benc::Element::from("info");
        let info = root_dict.get_mut(&info_key).unwrap();
        if let benc::Element::Dictionary(info_dict) = info {
            let pieces_key: benc::Element = benc::Element::from("pieces");
            let pieces = info_dict.get_mut(&pieces_key).unwrap();
            if let benc::Element::ByteString(pieces_data) = pieces {
                for hash in pieces_data.chunks_exact_mut(20).take(piece_count - 1) {
                    hash.copy_from_slice(&piece_sha1);
                }
                for hash in pieces_data.chunks_exact_mut(20).skip(piece_count - 1) {
                    hash.copy_from_slice(&last_piece_sha1);
                }
            }
        }
    }

    print_entity(&entity);

    let dest_content = entity.to_bytes();
    fs::write(dest, dest_content).unwrap();
}
