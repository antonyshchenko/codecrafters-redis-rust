use std::io;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .expect("Failed to bind to port");

    loop {
        match listener.accept().await {
            Ok((mut stream, socket_addr)) => {
                println!("Client connected: {socket_addr}");
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(&mut stream).await {
                        eprintln!("Error while handling connection from {socket_addr}: {e}");
                    }
                });
            }
            Err(e) => {
                eprintln!("Failed to accept client connection {e}");
            }
        }
    }
}

async fn handle_connection(stream: &mut TcpStream) -> io::Result<()> {
    loop {
        let (reader, writer) = stream.split();
        let mut reader = BufReader::new(reader);
        let mut writer = BufWriter::new(writer);

        let data = reader.fill_buf().await?.to_vec();
        if data.is_empty() {
            return Ok(());
        }

        writer.write_all(b"+PONG\r\n").await?;
        writer.flush().await?;

        reader.consume(data.len());
    }
}
