use std::env;
use anyhow::Result;
use tokio::{net::{TcpListener, TcpStream}, signal, task::JoinSet};
use crate::modules::{parser::RedisParser, values::RedisValue};
mod modules;

async fn handle_client_async(stream: TcpStream) -> Result<()> {
    println!("Incoming connection from: {}", stream.peer_addr()?);
    let mut parser = RedisParser::new(stream);

    loop {
        match parser.read_value().await {
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
                    parser.send(response).await?;
                }
            },
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut port = "6379".to_string();
    if let Ok(var_port) = env::var("REDIS_PORT") {
        port = var_port;
    }
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;
    println!("Listening on 127.0.0.1:{}", port);

    let mut handles = JoinSet::new();
    let ctrl_c_signal = signal::ctrl_c();
    tokio::pin!(ctrl_c_signal);

    loop {
        tokio::select! {
            biased;
            _ = &mut ctrl_c_signal => {
                println!("\nCtrl+C received! Stopping listener and waiting for clients to finish...");
                break;
            },
            conn = listener.accept() => {
                match conn {
                    Ok((stream, addr)) => {
                        println!("Accepted connection from {}", addr);
                        handles.spawn(async move {
                            if let Err(e) = handle_client_async(stream).await {
                                eprintln!("Error handling client: {}", e);
                            }
                        });
                    },
                    Err(e) => {
                        eprintln!("error accepting connection: {}", e);
                    }
                }
            },
            _ = handles.join_next() => {}
        }
    }
    Ok(())
}
