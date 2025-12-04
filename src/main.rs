use std::{env, net::{TcpListener, TcpStream}, thread};
use anyhow::Result;

use crate::modules::{parser::RedisParser, values::RedisValue};
mod modules;

fn handle_client(stream: TcpStream) -> Result<()> {
    println!("Incoming connection from: {}", stream.peer_addr()?);
    let mut parser = RedisParser::new(stream);

    loop {
        match parser.read_value() {
            Err(e) => {
                println!("{}", e);
                return Ok(())
            },
            Ok(value) => {
                if let RedisValue::Array(args) = value {
                    if args.len() == 0 {
                        continue;
                    }
                    let command = args[0].get_string()?.to_ascii_uppercase();
                    let response: &[u8] = match command.as_str() {
                        "PING" => &RedisValue::String("PONG".to_string()).as_simple_string()?,
                        "ECHO" => {
                            if args.len() != 2 {
                                &RedisValue::Error("Err wrong number of arguments for 'ECHO' command".to_string()).encode()
                            } else {
                                &args[1].encode()
                            }
                        },
                        c => &RedisValue::Error(format!("Err unknown command '{}'", c)).encode(),
                    };
                    parser.send(response)?;
                }
            },
        }
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
