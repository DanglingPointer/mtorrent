use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use tokio::net::{TcpListener, TcpStream};

pub async fn run_pwp_listener(port: u16, mut callback: impl FnMut(TcpStream)) -> io::Result<()> {
    let bind_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port));
    let listener = TcpListener::bind(bind_addr).await?;
    loop {
        let (stream, _addr) = listener.accept().await?;
        callback(stream);
    }
}
