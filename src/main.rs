use std::{collections::HashMap, env, sync::Arc};
use anyhow::Result;
use chrono::{DateTime, TimeDelta, Utc};
use tokio::{net::{TcpListener, TcpStream}, signal, sync::RwLock, task::JoinSet};
use crate::modules::{parser::RedisParser, values::RedisValue};
mod modules;

struct DbRecord {
    value: RedisValue,
    time_limit: Option<DateTime<Utc>>,
}

impl DbRecord {
    fn new(value: RedisValue) -> Self {
        Self { value, time_limit: None }
    }

    fn new_with_limit(value: RedisValue, limit: DateTime<Utc>) -> Self {
        Self { value, time_limit: Some(limit) }
    }

    fn is_valid(&self) -> bool {
        if let Some(limit) = self.time_limit {
            let now = Utc::now();
            if now >= limit {
                return false
            }
        }
        true
    }
}

struct Registry {
    channels: HashMap<String, Vec<u32>>,
    subscriptions: HashMap<u32, Vec<String>>,
}

impl Registry {
    fn new() -> Self {
        Self { channels: HashMap::new(), subscriptions: HashMap::new() }
    }
}



async fn handle_client_async(my_id: u32, stream: TcpStream, db: Arc<RwLock<HashMap<String, DbRecord>>>, ps_registry: Arc<RwLock<Registry>>) -> Result<()> {
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
                            if args.len() < 3 {
                                RedisValue::Error("Err wrong number of arguments for 'ECHO' command".to_string()).encode()
                            } else {
                                let key = args[1].clone().get_string()?;
                                let value = args[2].clone();
                                let record;
                                if args.len() > 4 && args[3].get_string()?.to_uppercase() == "PX" {
                                    let milliseconds_limit = usize::from_str_radix(args[4].get_string()?.as_str(), 10)?;
                                    let now = Utc::now();
                                    let delta = TimeDelta::milliseconds(milliseconds_limit as i64);
                                    let limit = now.checked_add_signed(delta).unwrap();
                                    record = DbRecord::new_with_limit(value, limit);
                                } else if args.len() > 4 && args[3].get_string()?.to_uppercase() == "EX" {
                                    let seconds_limit = usize::from_str_radix(args[4].get_string()?.as_str(), 10)?;
                                    let now = Utc::now();
                                    let delta = TimeDelta::seconds(seconds_limit as i64);
                                    let limit = now.checked_add_signed(delta).unwrap();
                                    record = DbRecord::new_with_limit(value, limit);
                                } else {
                                    record = DbRecord::new(value);
                                }
                                {
                                    let mut map = db.write().await;
                                    map.insert(key, record);
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
                                let record = map.get(&key);
                                match record {
                                    Some(record) => {
                                        if record.is_valid() {
                                            record.value.encode()
                                        } else {
                                            RedisValue::Null.encode()
                                        }
                                    },
                                    None => {
                                        RedisValue::Null.encode()
                                    }
                                }
                            }
                        },
                        "SUBSCRIBE" =>  {
                            if args.len() != 2 {
                                RedisValue::Error("Err wrong number of arguments for 'ECHO' command".to_string()).encode()
                            } else {
                                let channel = args[1].get_string()?;
                                {
                                    let mut reg = ps_registry.write().await;
                                    if reg.channels.contains_key(&channel) {
                                        reg.channels.get_mut(&channel).unwrap().push(my_id);
                                    } else {
                                        reg.channels.insert(channel.clone(), vec![my_id]);
                                    }
                                    if reg.subscriptions.contains_key(&my_id) {
                                        reg.subscriptions.get_mut(&my_id).unwrap().push(channel.clone());
                                    } else {
                                        reg.subscriptions.insert(my_id, vec![channel.clone()]);
                                    }
                                }
                                let reg = ps_registry.read().await;
                                let current_subscriptions = reg.subscriptions.get(&my_id).unwrap().len();
                                let mut response = vec![];
                                response.push(RedisValue::String("subscribe".to_string()));
                                response.push(RedisValue::String(channel));
                                response.push(RedisValue::Int(current_subscriptions as i64));
                                RedisValue::Array(response).encode()
                            }
                        }
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
    let db = Arc::new(RwLock::new(HashMap::new()));
    let ps_registry = Arc::new(RwLock::new(Registry::new()));
    let ctrl_c_signal = signal::ctrl_c();
    tokio::pin!(ctrl_c_signal);

    let mut current_thread_id = 0u32;

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
                        let ps_registry = Arc::clone(&ps_registry);
                        handles.spawn(async move {
                            if let Err(e) = handle_client_async(current_thread_id, stream, db, ps_registry).await {
                                eprintln!("Error handling client: {}", e);
                            }
                        });
                        current_thread_id += 1;
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
