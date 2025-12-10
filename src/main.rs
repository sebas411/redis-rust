use std::{collections::HashMap, env, sync::Arc};
use anyhow::Result;
use tokio::{net::{TcpListener, TcpStream}, signal, sync::RwLock, task::JoinSet};
use crate::modules::{parser::RedisParser, values::RedisValue};
mod modules;

type Db = Arc<RwLock<HashMap<String, RedisValue>>>;

async fn handle_client_async(stream: TcpStream, db: Db) -> Result<()> {
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
                    if args.is_empty() {
                        continue;
                    }
                    let command = args[0].get_string()?.to_ascii_uppercase();
                    let response = match command.as_str() {
                        "PING" => RedisValue::String("PONG".to_string()).as_simple_string()?,
                        "ECHO" => {
                            if args.len() != 2 {
                                RedisValue::Error("Err wrong number of arguments for 'ECHO' command".to_string()).encode()
                            } else {
                                args[1].encode()
                            }
                        },
                        "SET" => {
                            if args.len() != 3 {
                                RedisValue::Error("Err wrong number of arguments for 'ECHO' command".to_string()).encode()
                            } else {
                                let key = args[1].clone().get_string()?;
                                let value = args[2].clone();
                                {
                                    let mut map = db.write().await;
                                    map.insert(key, value);
                                }
                                RedisValue::String("OK".to_string()).as_simple_string()?
                            }
                        },
                        "GET" => {
                            if args.len() != 2 {
                                RedisValue::Error("Err wrong number of arguments for 'ECHO' command".to_string()).encode()
                            } else {
                                let key = args[1].clone().get_string()?;
                                let map = db.read().await;
                                let value = map.get(&key);
                                match value {
                                    Some(value) => {
                                        value.encode()
                                    },
                                    None => {
                                        RedisValue::Null.encode()
                                    }
                                }
                            }
                        },
                        c => RedisValue::Error(format!("Err unknown command '{}'", c)).encode(),
                    };
                    parser.send(&response).await?;
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
    let db: Db = Arc::new(RwLock::new(HashMap::new()));
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
                        let db = Arc::clone(&db);
                        handles.spawn(async move {
                            if let Err(e) = handle_client_async(stream, db).await {
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
