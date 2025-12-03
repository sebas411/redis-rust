use std::{env, io::{Read, Write}, net::TcpListener};
use anyhow::Result;

fn main() -> Result<()> {
    let mut port = "6379".to_string();
    if let Ok(var_port) = env::var("REDIS_PORT") {
        port = var_port;
    }
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port))?;
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut buffer = [0u8; 1024];
                loop {
                    let bytes_read = stream.read(&mut buffer)?;
                    if bytes_read == 0 {
                        break;
                    }
                    let _line = String::from_utf8(buffer[..bytes_read].to_vec())?;
                    stream.write("+PONG\r\n".as_bytes())?;
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
