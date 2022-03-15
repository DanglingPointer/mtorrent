use std::fs;

mod benc;

fn print_entity(entity: &benc::Element) {
    match entity {
        benc::Element::ByteString(data) => {
            let text = String::from_utf8_lossy(&data);
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
    let file_content = fs::read("/home/mikhailv/Movies/torrents/example.torrent").unwrap();
    let entity = benc::Element::from_bytes(&file_content).unwrap();
    print_entity(&entity);

    fs::write(
        "/home/mikhailv/Movies/torrents/example_copy.torrent",
        entity.to_bytes(),
    )
    .unwrap();
}
