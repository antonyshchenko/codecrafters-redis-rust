use crate::resp::{RespCodec, RespCommand};
use crate::store::Store;
use std::io::{self};
use tokio::net::{TcpListener, TcpStream};

mod resp;
mod store;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .expect("Failed to bind to port");

    let store = Store::new();

    loop {
        match listener.accept().await {
            Ok((mut stream, socket_addr)) => {
                println!("Client connected: {socket_addr}");
                let store = store.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(&mut stream, store).await {
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

async fn handle_connection(stream: &mut TcpStream, store: Store) -> io::Result<()> {
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
                match message {
                    Some(message) => codec.write_bulk_str(message).await?,
                    None => codec.write_simple_str("PONG").await?,
                };
            }
            RespCommand::Set { key, value } => {
                store.set(key, value);
                codec.write_simple_str("OK").await?;
            }
            RespCommand::Get { key } => {
                let value = store.get(&key);
                codec.write_bulk_str_opt(value).await?
            }
        }
        codec.flush().await?;
    }
}
