use std::{cmp::{max, min}, collections::{HashMap, HashSet, VecDeque}, fs::{self, File, OpenOptions}, io::{BufRead, BufReader, Write}, sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::{SystemTime, UNIX_EPOCH}, usize};
use anyhow::{Result, anyhow};
use chrono::{TimeDelta, Utc};
use regex::Regex;
use tokio::{io::AsyncWriteExt, net::{TcpStream, tcp::OwnedWriteHalf}, sync::{Mutex, RwLock, mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel}}, time::{self, Duration}};

use crate::{Replica, ReplicaInfo, User, hash_password, modules::{db::{DB, DbRecord, ListRecord, Registry, SortedSetEntry, SortedSetRecord, StreamEntry, StreamRecord, StringRecord}, geofunctions::{get_distance, location_to_score, score_to_location}, parser::RedisParser, values::RedisValue}};

mod acl;
mod bitmaps;
mod core;
mod geospatial;
mod lists;
mod optimistic_locking;
mod persistence;
mod pubsub;
mod replication;
mod sorted_sets;
mod streams;
mod transactions;

const SUBSCRIBE_MODE_COMMANDS: [&str; 6] = ["SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT"];
const TRANSACTION_COMMANDS: [&str; 5] = ["MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH"];
const WRITE_COMMANDS: [&str; 8] = ["SET", "DEL", "RPUSH", "LPUSH", "LPOP", "BLPOP", "XADD", "INCR"];

pub struct ClientHandler {
    id: u32,
    db: Arc<RwLock<DB>>,
    ps_registry: Arc<RwLock<Registry>>,
    receiver: UnboundedReceiver<Vec<u8>>,
    instruction_receiver: Option<UnboundedReceiver<Vec<RedisValue>>>,
    ack_sender: Option<UnboundedSender<usize>>,
    replicas: Arc<RwLock<Vec<Arc<Mutex<Replica>>>>>,
    subscribe_mode: bool,
    multi_mode: bool,
    watched_keys: Vec<(String, Option<String>)>,
    is_replicating: bool,
    queued_commands: Vec<Vec<RedisValue>>,
    replica_info: Arc<RwLock<ReplicaInfo>>,
    write_stream: Option<Mutex<OwnedWriteHalf>>,
    processed_bytes: usize,
    write_bytes: usize,
    prevent_send: bool,
    config: HashMap<String, String>,
    users: Arc<RwLock<HashMap<String, User>>>,
    current_user: Option<String>,
}


impl ClientHandler {
    pub async fn new(id: u32, db: Arc<RwLock<DB>>, ps_registry: Arc<RwLock<Registry>>, receiver: UnboundedReceiver<Vec<u8>>, repl_info: Arc<RwLock<ReplicaInfo>>, replicadb: Arc<RwLock<Vec<Arc<Mutex<Replica>>>>>, is_replicating: bool, users: Arc<RwLock<HashMap<String, User>>>, config: HashMap<String, String>) -> Self {
        let mut my_self =
        Self { id, db, ps_registry, receiver, subscribe_mode: false, multi_mode: false, queued_commands: vec![], processed_bytes: 0, ack_sender: None, replica_info: repl_info, config: config.clone(),
            write_stream: None, instruction_receiver: None, replicas: replicadb, is_replicating, write_bytes: 0, prevent_send: false, watched_keys: vec![], users, current_user: None };
        if my_self.authenticate_user("default", "").await {
            my_self.current_user = Some("default".to_string());
        }
        if let Some(appendonly) = config.get("appendonly") && appendonly == "yes" {
            match my_self.replay_commands().await {
                Err(e) => println!("Error replaying commands from appendonlyfile: {}", e),
                Ok(_) => println!("Commands from appendonlyfile replayed"),
            }
        }
        my_self
    }

    async fn replay_commands(&mut self) -> Result<()> {
        let manifest_filename = self.config.get("appendmanifestfilename").unwrap();
        let appenddir = self.config.get("appenddirname").unwrap().to_string();
        let dir = self.config.get("dir").unwrap().to_string();
        let manifest_file = File::open(manifest_filename)?;
        let reader = BufReader::new(manifest_file);
        for line in reader.lines() {
            let line = line?;
            if line.ends_with("type i") {
                let filename = format!("{}/{}/{}", dir, appenddir, line.split(' ').nth(1).ok_or(anyhow!("Manifest file malformed"))?);
                let data = fs::read(filename)?;
                
                let mut parser = RedisParser::new(data.as_slice());
                while let Ok(value) = parser.read_value().await {
                    if let RedisValue::Array(args) = &value {
                        if args.is_empty() {
                            continue;
                        }
                        let command = args[0].get_string().unwrap_or_default().to_ascii_uppercase();
                        self.handle_commands(&command, args.clone()).await.unwrap();
                    }
                }
            }
        }
        Ok(())
    }

    async fn authenticate_user(&self, username: &str, password: &str) -> bool {
        let mut successful_auth = false;
        let userdb = self.users.read().await;
        if let Some(user) = userdb.get(username) {
            let password_hash = hash_password(&password);
            for pass in user.password_iter() {
                if pass == &password_hash {
                    successful_auth = true;
                    break;
                }
            }
            for flag in user.flag_iter() {
                if flag == "nopass" {
                    successful_auth = true;
                    break;
                }
            }
        }
        successful_auth
    }

    async fn send(&mut self, src: &[u8], overwrite: bool) -> Result<()>{
        match &self.write_stream {
            Some(stream) => {
                if overwrite || !self.is_replicating {
                    // Lock mutex guard
                    let mut stream = stream.lock().await;
                    stream.write(src).await?;
                }
                Ok(())
            },
            None => Err(anyhow!("No stream to send message to. Line {}", line!())),
        }
    }

    async fn get_instruction(val: Option<&mut UnboundedReceiver<Vec<RedisValue>>>) -> Option<Vec<RedisValue>> {
        match val {
            Some(val) => val.recv().await,
            None => None
        }
    }

    async fn check_replicas(&mut self, replicas_ready: Arc<AtomicUsize>, replicas_expected: usize, timeout_millis: u64) -> Result<()> {
        let replicas = {
            let guard = self.replicas.read().await;
            guard.iter().cloned().collect::<Vec<_>>()
        };
        let replica_num = replicas.len();
        
        if self.write_bytes == 0 {
            replicas_ready.store(replica_num, Ordering::Relaxed);
            return Ok(())
        }
        
        let message = vec![RedisValue::String("REPLCONF".into()), RedisValue::String("GETACK".into()), RedisValue::String("*".into())];
        
        let mut handles = vec![];
        for replica in replicas.into_iter() {
            let message = message.clone();
            let replicas_ready = replicas_ready.clone();
            let expected_bytes = self.write_bytes;

            handles.push(tokio::spawn(async move {
                let mut r = replica.lock().await;
                r.send(message).unwrap();

                match time::timeout(Duration::from_millis(timeout_millis), r.receive()).await {
                    Ok(ack_bytes) => {
                        let ack_bytes = ack_bytes.unwrap_or_default();
                        if ack_bytes == expected_bytes {
                            replicas_ready.fetch_add(1, Ordering::Relaxed);
                        } else {
                            println!("Ack bytes didn't match the written bytes. Expected: {}, got: {}", expected_bytes, ack_bytes);
                        }
                    },
                    Err(_e) => (),
                }
            }));
        }
        self.write_bytes += RedisValue::Array(message.clone()).encode().len();
        for h in handles {
            let _ = h.await;
            if replicas_ready.load(Ordering::Relaxed) >= replicas_expected {
                break;
            }
        }
        Ok(())
    }

    pub async fn handle_client_async(&mut self, stream: TcpStream) -> Result<()> {
        let (read_stream, write_stream) = stream.into_split();
        self.write_stream = Some(Mutex::new(write_stream));
        let mut parser = RedisParser::new(read_stream);
        loop {
            let receiver = &mut self.receiver;
            let instruction_receiver = self.instruction_receiver.as_mut();
            self.processed_bytes = parser.get_processed_bytes();
            tokio::select! {
                value_read = parser.read_value() => {
                    match value_read {
                        Err(e) => {
                            println!("{}", e);
                            return Ok(())
                        },
                        Ok(value) => {
                            if let RedisValue::Array(args) = &value {
                                if args.is_empty() {
                                    continue;
                                }
                                let command = args[0].get_string()?.to_ascii_uppercase();

                                if self.subscribe_mode && !SUBSCRIBE_MODE_COMMANDS.contains(&command.as_str()) {
                                    let response = RedisValue::Error(format!("ERR Can't execute '{}' in subscribed mode", command)).encode();
                                    self.send(&response, false).await?;

                                    continue;
                                }
                                let response = self.handle_commands(&command, args.clone()).await?;
                                // Send to replication replicas
                                if WRITE_COMMANDS.contains(&command.as_str()) {
                                    if let Some(appendonly) = self.config.get("appendonly") && appendonly == "yes" {
                                        let filename = self.config.get("appendfilename").ok_or(anyhow!("Variable appendfilename not found in config"))?;
                                        let appenddir = self.config.get("appenddirname").ok_or(anyhow!("Variable appenddirname not found in config"))?;
                                        let dir = self.config.get("dir").ok_or(anyhow!("Variable dir not found in config"))?;
                                        let complete_filename = format!("{}/{}/{}.1.incr.aof", dir, appenddir, filename);
                                        let mut file = OpenOptions::new().create(true).append(true).open(complete_filename)?;
                                        file.write_all(&value.clone().encode())?;
                                    }
                                    if !self.replicas.read().await.is_empty() {
                                        let replicadb = self.replicas.read().await;
                                        for i in 0..replicadb.len() {
                                            let replica = replicadb.get(i).unwrap().lock().await;
                                            replica.send(args.clone()).unwrap();
                                        }
                                        let processed_bytes = parser.get_processed_bytes();
                                        self.write_bytes +=  processed_bytes - self.processed_bytes;
                                    }
                                }
                                if !self.prevent_send {
                                    self.send(&response, false).await?;
                                } else {
                                    self.prevent_send = false;
                                }
                            }
                        },
                    }
                },
                message_to_send = receiver.recv(), if !self.is_replicating => {
                    match message_to_send {
                        None => {
                            return Err(anyhow!("The internal pipe broke. Line {}, File {}", line!(), file!())) 
                        },
                        Some(message) => {
                            self.send(&message, false).await?;
                        }
                    }
                },
                instruction_message = Self::get_instruction(instruction_receiver), if instruction_receiver.is_some() && !self.is_replicating => {
                    match instruction_message {
                        None => {
                           return Err(anyhow!("The internal pipe broke. Line {}, File {}", line!(), file!())) 
                        },
                        Some(message) => {
                            self.send(&RedisValue::Array(message.clone()).encode(), false).await?;
                        }
                    }
                }
            }
        }
    }

    async fn handle_commands(&mut self, command: &str, args: Vec<RedisValue>) -> Result<Vec<u8>> {
        if let None = self.current_user && command != "AUTH" {
            return Ok(RedisValue::Error("NOAUTH Authentication required.".to_string()).encode())
        }
        if self.multi_mode && !TRANSACTION_COMMANDS.contains(&command) {
            self.queued_commands.push(args);
            return Ok(RedisValue::String("QUEUED".as_bytes().to_vec()).as_simple_string()?);
        }
        match command {
            "EXEC" => self.exec_queued().await,
            _ => self.execute_command(command, args).await,
        }
    }

    async fn execute_command(&mut self, command: &str, args: Vec<RedisValue>) -> Result<Vec<u8>> {
        match command {
            "PING" | "ECHO" | "SET" | "GET" | "TYPE" | "INCR" => {
                self.execute_core_command(command, args).await
            }
            "SUBSCRIBE" | "PUBLISH" | "UNSUBSCRIBE" => {
                self.execute_pubsub_command(command, args).await
            }
            "RPUSH" | "LRANGE" | "LPUSH" | "LLEN" | "LPOP" | "BLPOP" => {
                self.execute_lists_command(command, args).await
            }
            "XADD" | "XRANGE" | "XREAD" => self.execute_streams_command(command, args).await,
            "MULTI" | "DISCARD" => self.execute_transactions_command(command, args).await,
            "WATCH" | "UNWATCH" => self.execute_optimistic_locking_command(command, args).await,
            "INFO" | "REPLCONF" | "PSYNC" | "WAIT" => {
                self.execute_replication_command(command, args).await
            }
            "CONFIG" | "KEYS" => self.execute_persistence_command(command, args).await,
            "ZADD" | "ZRANK" | "ZRANGE" | "ZCARD" | "ZSCORE" | "ZREM" => {
                self.execute_sorted_sets_command(command, args).await
            }
            "GEOADD" | "GEOPOS" | "GEODIST" | "GEOSEARCH" => {
                self.execute_geospatial_command(command, args).await
            }
            "ACL" | "AUTH" => self.execute_acl_command(command, args).await,
            "SETBIT" | "GETBIT" | "STRLEN" | "BITCOUNT" => self.execute_bitmaps_command(command, args).await,
            command => Ok(RedisValue::Error(format!("Err unknown command '{}'", command)).encode()),
        }
    }
}
