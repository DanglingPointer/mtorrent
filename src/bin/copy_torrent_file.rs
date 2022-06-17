use mtorrent::benc;
use std::{env, fs};

fn print_entity(entity: &benc::Element) {
    match entity {
        benc::Element::ByteString(data) => {
            let text = String::from_utf8_lossy(data);
            print!(" {} ", text)
        }
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
        "/home/mikhailv/Movies/torrents/example.torrent".to_string()
    };

    let dest = if args.len() >= 3 {
        args[2].to_owned()
    } else {
        "/home/mikhailv/Movies/torrents/example_copy.torrent".to_string()
    };

    let source_content = fs::read(source).unwrap();
    let entity = benc::Element::from_bytes(&source_content).unwrap();

    print_entity(&entity);

    let dest_content = entity.to_bytes();
    fs::write(dest, dest_content).unwrap();
}
