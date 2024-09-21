mod msgs;
mod u160;

#[derive(Debug)]
pub enum Error {
    ParseError(&'static str),
}
