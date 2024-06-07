use crate::utils::ip;
use std::io;
use std::net::SocketAddr;
use tokio::net::TcpStream;

pub async fn run_pwp_listener(
    local_addr: SocketAddr,
    mut callback: impl FnMut(TcpStream, SocketAddr),
) -> io::Result<()> {
    let socket = ip::bound_tcp_socket(local_addr)?;
    let listener = socket.listen(1024)?;
    log::info!("TCP listener started on {}", listener.local_addr()?);
    loop {
        let (stream, addr) = listener.accept().await?;
        callback(stream, addr);
    }
}
