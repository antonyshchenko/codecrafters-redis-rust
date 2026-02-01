use std::io::Write;
use std::net::{Shutdown, TcpListener};
use std::process::exit;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap_or_else(|err| {
        eprintln!("Failed to bind to a port: {}", err);
        exit(1);
    });

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                if let Err(error) = stream.write("+PONG\r\n".as_bytes()) {
                    eprintln!("Error while writing response: {}", error);
                    stream
                        .shutdown(Shutdown::Both)
                        .expect("Failed to shutdown TCP stream");
                }
            }
            Err(e) => {
                eprintln!("error: {}", e);
            }
        }
    }
}
