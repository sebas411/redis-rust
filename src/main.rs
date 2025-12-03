use std::{env, io::{Read, Write}, net::{TcpListener, TcpStream}, thread};
use anyhow::Result;

fn handle_client(mut stream: TcpStream) -> Result<()> {
    println!("Incoming connection from: {}", stream.peer_addr()?);
    let mut buffer = [0u8; 1024];

    loop {
        let bytes_read = stream.read(&mut buffer)?;
        if bytes_read == 0 {
            println!("Client disconnected: {}", stream.peer_addr()?);
            return Ok(())
        }
        let _line = String::from_utf8(buffer[..bytes_read].to_vec())?;
        stream.write("+PONG\r\n".as_bytes())?;
    }
}

fn main() -> Result<()> {
    let mut port = "6379".to_string();
    if let Ok(var_port) = env::var("REDIS_PORT") {
        port = var_port;
    }
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port))?;
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || {
                    if let Err(e) = handle_client(stream) {
                        eprintln!("Error handling client: {}", e)
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
