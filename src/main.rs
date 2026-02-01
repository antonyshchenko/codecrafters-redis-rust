use std::io;
use std::io::{ErrorKind};
use std::process::exit;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap_or_else(|err| {
        eprintln!("Failed to bind to a port: {}", err);
        exit(1);
    });

    loop {
        match listener.accept().await {
            Ok((mut stream, socket_addr)) => {
                println!("Client connected: {}", socket_addr);
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(&mut stream).await {
                        eprintln!("Error while handling connection: {}", e);
                        let _ = stream.shutdown().await;
                    }
                });
            }
            Err(e) => {
                eprintln!("Failed to accept client connection {}", e);
            }
        }
    }
}

async fn handle_connection(stream: &mut TcpStream) -> io::Result<()> {
    loop {
        stream.readable().await?;

        let mut buf = Vec::new();

        match stream.read_buf(&mut buf).await {
            Ok(bytes_read) => {
                if bytes_read > 0 {
                    if let Err(error) = stream.write_all(b"+PONG\r\n").await {
                        eprintln!("Error while writing response: {}", error);
                    }
                } else {
                    return Ok(())
                }
            }
            Err(e) => {
                if e.kind() != ErrorKind::ConnectionReset {
                    eprintln!("Error while reading from TCP stream {}", e);
                }
                return Ok(())
            }
        }
    }
}
