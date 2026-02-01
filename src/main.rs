use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::process::exit;
use std::thread::{sleep};
use std::time::Duration;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap_or_else(|err| {
        eprintln!("Failed to bind to a port: {}", err);
        exit(1);
    });

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                handle_connection(&mut stream);
            }
            Err(e) => {
                eprintln!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: &TcpStream) {
    loop {
        let mut reader = BufReader::new(stream);
        let mut command = String::new();
        match reader.read_line(&mut command) {
            Ok(bytes_read) => {
                if bytes_read > 0 {
                    if let Err(error) = stream.write("+PONG\r\n".as_bytes()) {
                        eprintln!("Error while writing response: {}", error);
                        let _ = stream.shutdown(Shutdown::Both);
                    }
                } else {
                    sleep(Duration::from_millis(10))
                }
            }
            Err(e) => {
                if e.kind() != ErrorKind::ConnectionReset {
                    eprintln!("Error while reading from TCP stream {}", e);
                }
                return
            }
        }
    }
}
