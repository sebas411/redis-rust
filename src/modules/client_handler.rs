use std::{collections::{HashMap, HashSet}, sync::Arc};
use anyhow::{Result, anyhow};
use chrono::{DateTime, TimeDelta, Utc};
use tokio::{net::TcpStream, sync::{RwLock, mpsc::{UnboundedReceiver, UnboundedSender}}};

use crate::modules::{parser::RedisParser, values::RedisValue};

const SUBSCRIBE_MODE_COMMANDS: [&str; 6] = ["SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT"];

pub struct DbRecord {
    value: RedisValue,
    time_limit: Option<DateTime<Utc>>,
}

impl DbRecord {
    pub fn new(value: RedisValue) -> Self {
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

pub struct Registry {
    channels: HashMap<String, HashSet<u32>>,
    subscriptions: HashMap<u32, HashSet<String>>,
    pub senders: HashMap<u32, UnboundedSender<Vec<u8>>>,
}

impl Registry {
    pub fn new() -> Self {
        Self { channels: HashMap::new(), subscriptions: HashMap::new(), senders: HashMap::new() }
    }
}

pub struct ClientHandler {
    id: u32,
    db: Arc<RwLock<HashMap<String, DbRecord>>>,
    ps_registry: Arc<RwLock<Registry>>,
    receiver: UnboundedReceiver<Vec<u8>>,
    subscribe_mode: bool,
}

impl ClientHandler {
    pub fn new(id: u32, db: Arc<RwLock<HashMap<String, DbRecord>>>, ps_registry: Arc<RwLock<Registry>>, receiver: UnboundedReceiver<Vec<u8>>) -> Self {
        Self { id, db, ps_registry, receiver, subscribe_mode: false }
    }
    pub async fn handle_client_async(&mut self, stream: TcpStream) -> Result<()> {
        println!("Incoming connection from: {}", stream.peer_addr()?);
        let mut parser = RedisParser::new(stream);
        loop {
            tokio::select! {
                value_read = parser.read_value() => {
                    match value_read {
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

                                if self.subscribe_mode && !SUBSCRIBE_MODE_COMMANDS.contains(&command.as_str()) {
                                    let response = RedisValue::Error(format!("ERR Can't execute '{}' in subscribed mode", command)).encode();
                                    parser.send(&response).await?;
                                    continue;
                                }
                                let response = self.handle_commands(&command, args).await?;
                                parser.send(&response).await?;
                            }
                        },
                    }
                },
                message_to_send = self.receiver.recv() => {
                    match message_to_send {
                        None => {
                           return Err(anyhow!("The internal pipe broke")) 
                        },
                        Some(message) => {
                            parser.send(&message).await?;
                        }
                    }
                }
            }
        }
    }
    async fn handle_commands(&mut self, command: &str, args: Vec<RedisValue>) -> Result<Vec<u8>> {
        let response = match command {
            "PING" =>  {
                if self.subscribe_mode {
                    let mut response = vec![];
                    response.push(RedisValue::String("pong".to_string()));
                    response.push(RedisValue::String("".to_string()));
                    RedisValue::Array(response).encode()
                } else {
                    RedisValue::String("PONG".to_string()).as_simple_string()?
                }
            },
            "ECHO" => {
                if args.len() != 2 {
                    RedisValue::Error("Err wrong number of arguments for 'ECHO' command".to_string()).encode()
                } else {
                    args[1].encode()
                }
            },
            "SET" => {
                if args.len() < 3 {
                    RedisValue::Error("Err wrong number of arguments for 'SET' command".to_string()).encode()
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
                        let mut map = self.db.write().await;
                        map.insert(key, record);
                    }
                    RedisValue::String("OK".to_string()).as_simple_string()?

                    
                }
            },
            "GET" => {
                if args.len() != 2 {
                    RedisValue::Error("Err wrong number of arguments for 'GET' command".to_string()).encode()
                } else {
                    let key = args[1].clone().get_string()?;
                    let map = self.db.read().await;
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
                    RedisValue::Error("Err wrong number of arguments for 'SUBSCRIBE' command".to_string()).encode()
                } else {
                    let channel = args[1].get_string()?;
                    {
                        let mut reg = self.ps_registry.write().await;
                        if reg.channels.contains_key(&channel) {
                            reg.channels.get_mut(&channel).unwrap().insert(self.id);
                        } else {
                            reg.channels.insert(channel.clone(), HashSet::from([self.id]));
                        }
                        if reg.subscriptions.contains_key(&self.id) {
                            reg.subscriptions.get_mut(&self.id).unwrap().insert(channel.clone());
                        } else {
                            reg.subscriptions.insert(self.id, HashSet::from([channel.clone()]));
                        }
                    }
                    let reg = self.ps_registry.read().await;
                    let current_subscriptions = reg.subscriptions.get(&self.id).unwrap().len();
                    self.subscribe_mode = true;
                    let mut response = vec![];
                    response.push(RedisValue::String("subscribe".to_string()));
                    response.push(RedisValue::String(channel));
                    response.push(RedisValue::Int(current_subscriptions as i64));
                    RedisValue::Array(response).encode()
                }
            },
            "PUBLISH" => {
                if args.len() != 3 {
                    RedisValue::Error("Err wrong number of arguments for 'PUBLISH' command".to_string()).encode()
                } else {
                    let channel = args[1].get_string()?;
                    let message_string = args[2].get_string()?;
                    let reg = self.ps_registry.read().await;
                    let current_subscriber_num;
                    if reg.channels.contains_key(&channel) {
                        let current_subscribers = reg.channels.get(&channel).unwrap();
                        for sub in current_subscribers {
                            let sender = reg.senders.get(sub).unwrap();
                            let mut response = vec![];
                            response.push(RedisValue::String("message".to_string()));
                            response.push(RedisValue::String(channel.clone()));
                            response.push(RedisValue::String(message_string.clone()));
                            sender.send(RedisValue::Array(response).encode())?;
                        }
                        current_subscriber_num = current_subscribers.len();
                    } else {
                        current_subscriber_num = 0;
                    }
                    RedisValue::Int(current_subscriber_num as i64).encode()
                }
            },
            c => RedisValue::Error(format!("Err unknown command '{}'", c)).encode(),
        };
        Ok(response)
    }
}

