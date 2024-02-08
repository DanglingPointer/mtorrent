use std::io;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};

pub async fn run_pwp_listener(
    local_addr: SocketAddr,
    mut callback: impl FnMut(TcpStream),
) -> io::Result<()> {
    let listener = TcpListener::bind(local_addr).await?;
    log::info!("TCP listener started on {}", listener.local_addr()?);
    loop {
        let (stream, _addr) = listener.accept().await?;
        callback(stream);
    }
}
