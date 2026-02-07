use resp::RespCodec;
use std::io::{self};
use tokio::net::{TcpListener, TcpStream};

use crate::resp::{BulkString, RespCommand};

mod resp;

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
    let (reader, writer) = stream.split();
    let mut codec = RespCodec::new(reader, writer);

    loop {
        let command = match codec.read_command().await? {
            Some(cmd) => cmd,
            None => {
                return Ok(());
            }
        };

        println!("Got command {:?}", command);
        match command {
            RespCommand::Echo { message } => codec.write_bulk_str(message).await?,
            RespCommand::Ping { message } => {
                let response = message.unwrap_or_else(|| BulkString::from_str("PONG"));
                codec.write_bulk_str(response).await?
            }
        }
        codec.flush().await?;
    }
}
