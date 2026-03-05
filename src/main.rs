use crate::error::{Error, RespError};
use crate::store::Store;
use resp::cmd::{RespCommand, SetCmdCondition, SetCmdExpiry};
use resp::codec::RespCodec;
use resp::types::RespVal;
use std::io::{self};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{Instant, interval_at};

mod error;
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
        match codec.read_command().await {
            Ok(Some(command)) => {
                println!("Got command {:?}", command);
                match process_command(&store, command) {
                    Ok(resp_type) => codec.write_resp_val(resp_type).await?,
                    Err(error) => codec.write_err(error).await?,
                }

                codec.flush().await?;
            }
            Ok(None) => {
                return Ok(());
            }
            Err(Error::RespError(resp_error)) => {
                codec.write_err(resp_error).await?;
                codec.flush().await?
            }
            Err(Error::IoError(io_error)) => Err(io_error)?,
        };
    }
}

fn process_command(store: &Store, command: RespCommand) -> Result<RespVal, RespError> {
    match command {
        RespCommand::Echo { message } => Ok(RespVal::BulkString(message)),
        RespCommand::Ping { message } => match message {
            Some(message) => Ok(RespVal::BulkString(message)),
            None => Ok(RespVal::SimpleString(String::from("PONG"))),
        },
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
            Ok(RespVal::SimpleString(String::from("OK")))
        }
        RespCommand::Get { key } => store
            .get(&key)
            .map(|opt_val| opt_val.map_or(RespVal::Null, |val| RespVal::BulkString(val))),
        RespCommand::RPush { key, elements } => store
            .append_to_list(key, elements)
            .map(|list_size| RespVal::Integer(list_size.try_into().unwrap())),
    }
}
