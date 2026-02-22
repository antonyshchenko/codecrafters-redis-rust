use crate::resp::{RespCodec, RespCommand, SetCmdCondition, SetCmdExpiry};
use crate::store::Store;
use std::io::{self};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{Instant, interval_at};

mod resp;
mod store;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .expect("Failed to bind to port");

    let store = Store::new();

    spawn_periodic_vacuum(store.clone());

    loop {
        match listener.accept().await {
            Ok((stream, socket_addr)) => {
                spawn_connection_handler(stream, socket_addr, store.clone());
            }
            Err(e) => {
                eprintln!("Failed to accept client connection {e}");
            }
        }
    }
}

fn spawn_periodic_vacuum(store: Store) -> JoinHandle<()> {
    let duration = Duration::from_secs(5);

    tokio::spawn(async move {
        let mut interval = interval_at(Instant::now() + duration, duration);
        loop {
            interval.tick().await;
            store.vacuum();
        }
    })
}

fn spawn_connection_handler(
    mut stream: TcpStream,
    socket_addr: SocketAddr,
    store: Store,
) -> JoinHandle<()> {
    println!("Client connected: {socket_addr}");
    tokio::spawn(async move {
        if let Err(e) = handle_connection(&mut stream, store).await {
            eprintln!("Error while handling connection from {socket_addr}: {e}");
        }
    })
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
            RespCommand::Set {
                key,
                value,
                condition,
                expiry,
            } => {
                let expires_at = expiry.map(|expiry| match expiry {
                    SetCmdExpiry::TimeToLive { millis } => {
                        Instant::now() + Duration::from_millis(millis)
                    }
                });

                match condition {
                    Some(SetCmdCondition::UnlessKeyExists) => {
                        store.set_unless_exists(key, value, expires_at);
                    }
                    Some(SetCmdCondition::IfKeyExists) => {
                        store.set_if_exists(key, value, expires_at);
                    }
                    None => {
                        store.set(key, value, expires_at);
                    }
                }
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
