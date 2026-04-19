use std::{cmp::{max, min}, collections::{HashMap, HashSet, VecDeque}, sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::{SystemTime, UNIX_EPOCH}, usize};
use anyhow::{Result, anyhow};
use chrono::{TimeDelta, Utc};
use regex::Regex;
use tokio::{io::AsyncWriteExt, net::{TcpStream, tcp::OwnedWriteHalf}, sync::{Mutex, RwLock, mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel}}, time::{self, Duration}};

use crate::{Replica, ReplicaInfo, modules::{db::{DB, DbRecord, ListRecord, Registry, SortedSetEntry, SortedSetRecord, StreamEntry, StreamRecord, StringRecord}, geofunctions::location_to_score, parser::RedisParser, values::RedisValue}};

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
    db_dir: Option<String>,
    db_filename: Option<String>,
}


impl ClientHandler {
    pub fn new(id: u32, db: Arc<RwLock<DB>>, ps_registry: Arc<RwLock<Registry>>, receiver: UnboundedReceiver<Vec<u8>>, repl_info: Arc<RwLock<ReplicaInfo>>, replicadb: Arc<RwLock<Vec<Arc<Mutex<Replica>>>>>, is_replicating: bool, db_dir: Option<String>, db_filename: Option<String>) -> Self {
        Self { id, db, ps_registry, receiver, subscribe_mode: false, multi_mode: false, queued_commands: vec![], processed_bytes: 0, ack_sender: None, replica_info: repl_info,
            write_stream: None, instruction_receiver: None, replicas: replicadb, is_replicating, write_bytes: 0, prevent_send: false, db_dir, db_filename, watched_keys: vec![] }
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
                            if let RedisValue::Array(args) = value {
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
                                if WRITE_COMMANDS.contains(&command.as_str()) && !self.replicas.read().await.is_empty() {
                                    let replicadb = self.replicas.read().await;
                                    for i in 0..replicadb.len() {
                                        let replica = replicadb.get(i).unwrap().lock().await;
                                        replica.send(args.clone()).unwrap();
                                    }
                                    let processed_bytes = parser.get_processed_bytes();
                                    self.write_bytes +=  processed_bytes - self.processed_bytes;
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
        if self.multi_mode && !TRANSACTION_COMMANDS.contains(&command) {
            self.queued_commands.push(args);
            return Ok(RedisValue::String("QUEUED".to_string()).as_simple_string()?);
        }
        match command {
            "EXEC" => self.exec_queued().await,
            _ => self.execute_command(command, args).await,
        }
    }

    async fn execute_command(&mut self, command: &str, args: Vec<RedisValue>) -> Result<Vec<u8>> {
        
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
                        record = StringRecord::new_with_limit(value, limit);
                    } else if args.len() > 4 && args[3].get_string()?.to_uppercase() == "EX" {
                        let seconds_limit = usize::from_str_radix(args[4].get_string()?.as_str(), 10)?;
                        let now = Utc::now();
                        let delta = TimeDelta::seconds(seconds_limit as i64);
                        let limit = now.checked_add_signed(delta).unwrap();
                        record = StringRecord::new_with_limit(value, limit);
                    } else {
                        record = StringRecord::new(value);
                    }
                    {
                        let mut w_db = self.db.write().await;
                        w_db.insert(key, DbRecord::String(record));
                    }
                    RedisValue::String("OK".to_string()).as_simple_string()?
                }
            },
            "GET" => {
                if args.len() != 2 {
                    RedisValue::Error("Err wrong number of arguments for 'GET' command".to_string()).encode()
                } else {
                    let key = args[1].clone().get_string()?;
                    let db = self.db.read().await;
                    let record = db.get(&key);
                    match record {
                        Some(record) => {
                            let string_record = record.get_string();
                            if string_record.is_some() && string_record.unwrap().is_valid() {
                                string_record.unwrap().get_value().encode()
                            } else {
                                RedisValue::NullString.encode()
                            }
                        },
                        None => {
                            RedisValue::NullString.encode()
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
                        match reg.channels.get_mut(&channel) {
                            Some(map) => {
                                map.insert(self.id);
                            },
                            None => {
                                reg.channels.insert(channel.clone(), HashSet::from([self.id]));
                            }
                        }
                        match reg.subscriptions.get_mut(&self.id) {
                            Some(map) => {
                                map.insert(channel.clone());
                            },
                            None => {
                                reg.subscriptions.insert(self.id, HashSet::from([channel.clone()]));
                            }
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
            "UNSUBSCRIBE" => {
                if args.len() != 2 {
                    RedisValue::Error("Err wrong number of arguments for 'UNSUBSCRIBE' command".to_string()).encode()
                } else {
                    let channel = args[1].get_string()?;
                    {
                        let mut reg = self.ps_registry.write().await;
                        if let Some(map) = reg.channels.get_mut(&channel) {
                            map.remove(&self.id);
                        }
                        if let Some(map) = reg.subscriptions.get_mut(&self.id) {
                            map.remove(&channel);
                        }
                    }
                    let reg = self.ps_registry.read().await;
                    let current_subscriptions = reg.subscriptions.get(&self.id).unwrap().len();
                    if current_subscriptions == 0 {
                        self.subscribe_mode = false;
                    }
                    let mut response = vec![];
                    response.push(RedisValue::String("unsubscribe".to_string()));
                    response.push(RedisValue::String(channel));
                    response.push(RedisValue::Int(current_subscriptions as i64));
                    RedisValue::Array(response).encode()
                }
            },
            "RPUSH" => {
                if args.len() < 3 {
                    RedisValue::Error("Err wrong number of arguments for 'RPUSH' command".to_string()).encode()
                } else {
                    let list_name = args[1].get_string()?;
                    let prev_records;
                    let pushed_records = args.len() - 2;
                    {
                        let mut db = self.db.write().await;
                        match db.get_mut(&list_name) {
                            Some(record) => {
                                if let Some(list_record) = record.get_mut_list() {
                                    prev_records = list_record.len();
                                    for val in args.iter().skip(2) {
                                        list_record.push_back(val.get_string()?);
                                    }
                                } else {
                                    return Err(anyhow!("Record is not of type list. Line {}", line!()))
                                }
                            },
                            None => {
                                let mut values = VecDeque::new();
                                prev_records = 0;
                                for val in args.iter().skip(2) {
                                    values.push_back(val.get_string()?);
                                }
                                db.insert(list_name.clone(), DbRecord::List(ListRecord::from_list(values)));
                            }
                        }
                    }
                    RedisValue::Int((prev_records + pushed_records) as i64).encode()
                }
            },
            "LRANGE" => {
                if args.len() != 4 {
                    RedisValue::Error("Err wrong number of arguments for 'LRANGE' command".to_string()).encode()
                } else {
                    let list_name = args[1].get_string()?;
                    let start_string = args[2].get_string()?;
                    let stop_string = args[3].get_string()?;

                    let mut start = i64::from_str_radix(&start_string, 10)?;
                    let mut stop = i64::from_str_radix(&stop_string, 10)?;

                    let db = self.db.read().await;
                    let list = match db.get(&list_name) {
                        Some(record) => {
                            if let Some(list_record) = record.get_list() {
                                list_record.get_list()
                            } else {
                                VecDeque::new()
                            }
                        },
                        None => VecDeque::new()
                    };
                    let list_len = list.len() as i64;

                    if start < 0 { start = max(list_len + start, 0) }
                    if stop < 0 { stop = max(list_len + stop, 0)}
                    stop = min(stop, list_len - 1);

                    let start = start as usize;
                    let stop = stop as usize;

                    let mut return_list = vec![];
                    if start < list.len() && start <= stop {
                        for item in list.range(start..=stop) {
                            return_list.push(RedisValue::String(item.clone()));
                        }
                    }

                    RedisValue::Array(return_list) .encode()
                }
            },
            "LPUSH" => {
                if args.len() < 3 {
                    RedisValue::Error("Err wrong number of arguments for 'LPUSH' command".to_string()).encode()
                } else {
                    let list_name = args[1].get_string()?;
                    let prev_records;
                    let pushed_records = args.len() - 2;
                    {
                        let mut db = self.db.write().await;
                        match db.get_mut(&list_name) {
                            Some(record) => {
                                if let Some(list_record) = record.get_mut_list() {
                                    prev_records = list_record.len();
                                    for val in args.iter().skip(2) {
                                        list_record.push_front(val.get_string()?);
                                    }
                                } else {
                                    return Err(anyhow!("Record is not of type list. Line {}", line!()))
                                }
                            },
                            None => {
                                let mut values = VecDeque::new();
                                prev_records = 0;
                                for val in args.iter().skip(2) {
                                    values.push_front(val.get_string()?);
                                }
                                db.insert(list_name.clone(), DbRecord::List(ListRecord::from_list(values)));
                            }
                        }
                    }
                    RedisValue::Int((prev_records + pushed_records) as i64).encode()
                }
            },
            "LLEN" => {
                if args.len() != 2 {
                    RedisValue::Error("Err wrong number of arguments for 'LLEN' command".to_string()).encode()
                } else {
                    let list_name = args[1].get_string()?;
                    let list_len = self.db.read().await.get(&list_name).unwrap_or(&DbRecord::List(ListRecord::new())).get_list().unwrap_or(&ListRecord::new()).len();
                    RedisValue::Int(list_len as i64).encode()
                }
            },
            "LPOP" => {
                if args.len() < 2 || args.len() > 3 {
                    RedisValue::Error("Err wrong number of arguments for 'LPOP' command".to_string()).encode()
                } else {
                    let list_name = args[1].get_string()?;
                    let pop_amount = if args.len() == 3 { usize::from_str_radix(&args[2].get_string()?, 10)? } else { 1 };
                    let mut returned_items = vec![];
                    {
                        let mut db = self.db.write().await;
                        if let Some(record) = db.get_mut(&list_name) && let Some(list_record) = record.get_mut_list() {
                            for _ in 0..pop_amount {
                                match list_record.pop_front() {
                                    Some(popped) => {
                                        returned_items.push(RedisValue::String(popped));
                                    },
                                    None => {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if pop_amount == 1 {
                        returned_items[0].encode()
                    } else {
                        RedisValue::Array(returned_items).encode()
                    }
                }
            },
            "BLPOP" => {
                if args.len() != 3 {
                    RedisValue::Error("Err wrong number of arguments for 'BLPOP' command".to_string()).encode()
                } else {
                    let list_name = args[1].get_string()?;
                    let timeout = args[2].get_string()?.parse::<f64>()?;
                    let mut value = None;
                    let mut waiter = None;
                    // block to either get the value via pop or setup a waiter for when values come
                    {
                        let mut db = self.db.write().await;
                        let list_record = match db.get_mut(&list_name) {
                            None => {
                                db.insert(list_name.clone(), DbRecord::List(ListRecord::new()));
                                db.get_mut(&list_name).unwrap().get_mut_list().unwrap()
                            },
                            Some(record) => {
                                if let Some(list_record) = record.get_mut_list() {
                                    list_record
                                } else {
                                    return Err(anyhow!("Record is not of type list. Line {}", line!()))
                                }
                            }
                        };
                        if !list_record.is_empty() {
                            value = list_record.pop_front();
                        } else {
                            let (sender, receiver) = unbounded_channel::<String>();
                            list_record.subscribe_waiter(sender);
                            waiter = Some(receiver);
                        }
                    }
                    // wait for some value, either with timeout or stay waiting
                    if let Some(mut receiver) = waiter {
                        if timeout == 0.0 {
                            value = receiver.recv().await;
                        } else {
                            tokio::select! {
                                result = receiver.recv() => {
                                    value = result;
                                }
                                _ = time::sleep(Duration::from_secs_f64(timeout)) => ()
                            }
                        }
                    }
                    // actually respond to the client
                    if let Some(value) = value {
                        let array = vec![RedisValue::String(list_name), RedisValue::String(value)];
                        RedisValue::Array(array).encode()
                    } else {
                        RedisValue::NullArray.encode()
                    }
                }
            },
            "TYPE" => {
                if args.len() != 2 {
                    RedisValue::Error("Err wrong number of arguments for 'TYPE' command".to_string()).encode()
                } else {
                    let varname = args[1].get_string()?;
                    let db = self.db.read().await;
                    match db.get(&varname) {
                        Some(record) => {
                            RedisValue::String(record.get_type()).as_simple_string()?
                        },
                        None => RedisValue::String("none".to_string()).as_simple_string()?
                    }
                }
            },
            "XADD" => {
                if args.len() < 5 || args.len() % 2 != 1 {
                    RedisValue::Error("Err wrong number of arguments for 'XADD' command".to_string()).encode()
                } else {
                    let mut error_response = None;
                    let stream_name = args[1].get_string()?;
                    let mut entry_id = args[2].get_string()?;

                    let re = Regex::new(r"^((\d+|\*)-(\d+|\*)|\*)$").unwrap();

                    if !re.is_match(&entry_id) {
                        return Err(anyhow!("Bad format for stream id. Line {}", line!()))
                    }

                    if entry_id == "*" {
                        entry_id = "*-*".to_string();
                    }

                    let mut id_split = entry_id.split("-");
                    let milliseconds_str = id_split.next().unwrap();
                    let mut milliseconds = i64::from_str_radix(milliseconds_str, 10).unwrap_or(-1);
                    let sequence_str = id_split.next().unwrap();
                    let mut sequence = i64::from_str_radix(sequence_str, 10).unwrap_or(-1);

                    if milliseconds == 0 && sequence == 0 {
                        error_response = Some(RedisValue::Error("ERR The ID specified in XADD must be greater than 0-0".to_string()).encode())
                    }

                    let mut values = HashMap::new();

                    for i in (3..args.len()).step_by(2) {
                        let key = args[i].get_string()?;
                        let value = args[i+1].get_string()?;
                        values.insert(key, value);
                    }
                    
                    if error_response.is_none() {
                        let mut db = self.db.write().await;
                        match db.get_mut(&stream_name) {
                            Some(record) => {
                                if let Some(stream_record) = record.get_mut_stream() {
                                    let last_id = stream_record.peek_last();
                                    let mut last_id_split = last_id.get_id().split("-");
                                    let last_milli = i64::from_str_radix(last_id_split.next().unwrap(), 10).unwrap();
                                    let last_seq = i64::from_str_radix(last_id_split.next().unwrap(), 10).unwrap();
                                    if milliseconds_str == "*" {
                                        let now = SystemTime::now();
                                        let since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
                                        milliseconds = since_epoch.as_millis() as i64;
                                    }
                                    if sequence_str == "*" {
                                        if last_milli == milliseconds {
                                            sequence = last_seq + 1;
                                        } else {
                                            sequence = 0;
                                        }
                                    }
                                    entry_id = format!("{}-{}", milliseconds, sequence);
                                    let stream_entry = StreamEntry::new(&entry_id, Some(values));
                                    if last_milli > milliseconds || (last_milli == milliseconds && last_seq >= sequence ) {
                                        error_response = Some(RedisValue::Error("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()).encode())
                                    } else {
                                        stream_record.push(stream_entry);
                                    }
                                }
                            },
                            None => {
                                if milliseconds_str == "*" {
                                    let now = SystemTime::now();
                                    let since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
                                    milliseconds = since_epoch.as_millis() as i64;
                                }
                                if sequence_str == "*" {
                                    if milliseconds == 0 {
                                        sequence = 1;
                                    } else {
                                        sequence = 0;
                                    }
                                }
                                entry_id = format!("{}-{}", milliseconds, sequence);
                                let stream_entry = StreamEntry::new(&entry_id, Some(values));
                                let mut stream_record = StreamRecord::new();
                                stream_record.push(stream_entry);
                                let record = DbRecord::Stream(stream_record);
                                db.insert(stream_name, record);
                            }
                        }
                    }

                    match error_response {
                        None => RedisValue::String(entry_id).encode(),
                        Some(err) => err,
                    }
                }
            },
            "XRANGE" => {
                if args.len() != 4 {
                    RedisValue::Error("Err wrong number of arguments for 'XRANGE' command".to_string()).encode()
                } else {
                    let stream_name = args[1].get_string()?;
                    let re = Regex::new(r"^\d+(-\d+)?$").unwrap();
                    let mut lower_end = args[2].get_string()?;
                    if lower_end == "-" { lower_end = "0".to_string() }
                    let mut higher_end = args[3].get_string()?;
                    if higher_end == "+" { higher_end = format!("{}", usize::MAX) }
                    if !re.is_match(&lower_end) {
                        return Err(anyhow!("Bad format for stream id in range's lower end. Line {}", line!()))
                    }
                    if !re.is_match(&higher_end) {
                        return Err(anyhow!("Bad format for stream id in range's lower end. Line {}", line!()))
                    }
                    let lower_milliseconds;
                    let lower_sequence;
                    if lower_end.contains('-') {
                        let mut lower_split = lower_end.split('-');
                        lower_milliseconds = usize::from_str_radix(&lower_split.next().unwrap(), 10).unwrap();
                        lower_sequence = usize::from_str_radix(&lower_split.next().unwrap(), 10).unwrap();
                    } else {
                        lower_milliseconds = usize::from_str_radix(&lower_end, 10).unwrap();
                        lower_sequence = 0;
                    }
                    let higher_milliseconds;
                    let higher_sequence;
                    if higher_end.contains('-') {
                        let mut higher_split = higher_end.split('-');
                        higher_milliseconds = usize::from_str_radix(&higher_split.next().unwrap(), 10).unwrap();
                        higher_sequence = usize::from_str_radix(&higher_split.next().unwrap(), 10).unwrap();
                    } else {
                        higher_milliseconds = usize::from_str_radix(&higher_end, 10).unwrap();
                        higher_sequence = usize::MAX;
                    }
                    let mut response_array = vec![];
                    let db = self.db.read().await;
                    if let Some(record) = db.get(&stream_name) && let Some(stream_record) = record.get_stream() {
                        for entry in stream_record {
                            let mut entry_id = entry.get_id().split('-');
                            let entry_millis = usize::from_str_radix(entry_id.next().unwrap(), 10).unwrap();
                            let entry_seq = usize::from_str_radix(entry_id.next().unwrap(), 10).unwrap();
                            if entry_millis < lower_milliseconds || entry_millis == lower_milliseconds && entry_seq < lower_sequence {
                                continue;
                            } else if entry_millis > higher_milliseconds || entry_millis == higher_milliseconds && entry_seq > higher_sequence {
                                break;
                            }
                            let mut entry_array = vec![];
                            entry_array.push(RedisValue::String(entry.get_id().to_string()));
                            let mut values_array = vec![];
                            for (k, v) in entry {
                                values_array.push(RedisValue::String(k.clone()));
                                values_array.push(RedisValue::String(v.clone()));
                            }
                            entry_array.push(RedisValue::Array(values_array));
                            response_array.push(RedisValue::Array(entry_array));
                        }
                    }
                    RedisValue::Array(response_array).encode()
                }
            },
            "XREAD" => {
                if args.len() < 4 {
                    RedisValue::Error("Err wrong number of arguments for 'XRANGE' command".to_string()).encode()
                } else {
                    let re = Regex::new(r"^\d+-\d+$").unwrap();
                    let mut response_array = vec![];
                    let mut reached_deadline = false;
                    let is_blocked;
                    let block_args;
                    let block_timeout;
                    if args[1].get_string()?.to_lowercase() == "block" {
                        is_blocked = true;
                        block_args = 2;
                        block_timeout = u64::from_str_radix(&args[2].get_string()?, 10)?;
                    } else if args[1].get_string()?.to_lowercase() == "streams" {
                        is_blocked = false;
                        block_args = 0;
                        block_timeout = 0;
                    } else {
                        return Err(anyhow!("XREAD only compatible with STREAMS"));
                    }
                    for i in 0..(args.len() - block_args - 2) / 2 {
                        let stream_name = args[2+i+block_args].get_string()?;
                        let mut entry_id = args[(args.len() - block_args) / 2 + 1 + i + block_args].get_string()?;
                        if entry_id == "$" {
                            let db = self.db.read().await;
                            if let Some(record) = db.get(&stream_name) && let Some(stream_record) = record.get_stream() {
                                entry_id = stream_record.peek_last().get_id().to_string();
                            }
                        }
                        if !re.is_match(&entry_id) {
                            return Err(anyhow!("Bad format for stream id. Line {}", line!()))
                        }
                        let mut entry_id_split = entry_id.split('-');
                        let entry_milliseconds = usize::from_str_radix(&entry_id_split.next().unwrap(), 10).unwrap();
                        let entry_sequence = usize::from_str_radix(&entry_id_split.next().unwrap(), 10).unwrap();
                        
                        let mut stream_array = vec![];
                        stream_array.push(RedisValue::String(stream_name.clone()));
                        
                        let mut entries_array = vec![];
                        
                        {
                            let db = self.db.read().await;
                            if let Some(record) = db.get(&stream_name) && let Some(stream_record) = record.get_stream() {
                                for entry in stream_record {
                                    let mut entry_id = entry.get_id().split('-');
                                    let entry_millis = usize::from_str_radix(entry_id.next().unwrap(), 10).unwrap();
                                    let entry_seq = usize::from_str_radix(entry_id.next().unwrap(), 10).unwrap();
                                    if entry_millis < entry_milliseconds || entry_millis == entry_milliseconds && entry_seq <= entry_sequence {
                                        continue;
                                    }
                                    let mut entry_array = vec![];
                                    entry_array.push(RedisValue::String(entry.get_id().to_string()));
                                    let mut values_array = vec![];
                                    for (k, v) in entry {
                                        values_array.push(RedisValue::String(k.clone()));
                                        values_array.push(RedisValue::String(v.clone()));
                                    }
                                    entry_array.push(RedisValue::Array(values_array));
                                    entries_array.push(RedisValue::Array(entry_array));
                                }
                            }
                        }
                        if is_blocked && entries_array.is_empty() {
                            let (sender, mut receiver) = unbounded_channel();
                            {
                                let mut db = self.db.write().await;
                                if db.contains_key(&stream_name) {
                                    let record = db.get_mut(&stream_name).unwrap();
                                    if let Some(stream_record) = record.get_mut_stream() {
                                        stream_record.subscribe_waiter(sender);
                                    }
                                } else {
                                    let mut stream_record = StreamRecord::new();
                                    stream_record.subscribe_waiter(sender);
                                    db.insert(stream_name, DbRecord::Stream(stream_record));
                                }
                            }
                            // wait for value
                            let mut value = None;
                            if block_timeout == 0 {
                                loop {
                                    let msg = receiver.recv().await;
                                    if let Some(entry) = &msg {
                                        let mut entry_id = entry.get_id().split('-');
                                        let entry_millis = usize::from_str_radix(entry_id.next().unwrap(), 10).unwrap();
                                        let entry_seq = usize::from_str_radix(entry_id.next().unwrap(), 10).unwrap();
                                        if entry_millis > entry_milliseconds || entry_millis == entry_milliseconds && entry_seq > entry_sequence {
                                            value = msg;
                                            break;
                                        }
                                    }
                                }
                            } else {
                                let deadline = time::sleep(Duration::from_millis(block_timeout));
                                tokio::pin!(deadline);
                                loop {
                                    tokio::select! {
                                        msg = receiver.recv() => {
                                            if let Some(entry) = &msg {
                                                let mut entry_id = entry.get_id().split('-');
                                                let entry_millis = usize::from_str_radix(entry_id.next().unwrap(), 10).unwrap();
                                                let entry_seq = usize::from_str_radix(entry_id.next().unwrap(), 10).unwrap();
                                                if entry_millis > entry_milliseconds || entry_millis == entry_milliseconds && entry_seq > entry_sequence {
                                                    value = msg;
                                                    break;
                                                }
                                            }
                                        }
                                        _ = &mut deadline => {
                                            reached_deadline = true;
                                            break;
                                        }
                                    }

                                }
                            }
                            if let Some(entry) = value {
                                let mut entry_array = vec![];
                                entry_array.push(RedisValue::String(entry.get_id().to_string()));
                                let mut values_array = vec![];
                                for (k, v) in &entry {
                                    values_array.push(RedisValue::String(k.clone()));
                                    values_array.push(RedisValue::String(v.clone()));
                                }
                                entry_array.push(RedisValue::Array(values_array));
                                entries_array.push(RedisValue::Array(entry_array));
                            }
                        }
                        stream_array.push(RedisValue::Array(entries_array));
                        response_array.push(RedisValue::Array(stream_array));
                    }
                    if reached_deadline {
                        RedisValue::NullArray.encode()
                    } else {
                        RedisValue::Array(response_array).encode()
                    }
                }
            },
            "INCR" => {
                if args.len() != 2 {
                    RedisValue::Error("Err wrong number of arguments for 'INCR' command".to_string()).encode()
                } else {
                    let key = args[1].get_string()?;
                    let mut new_value = 0;
                    let mut db = self.db.write().await;
                    let mut error = None;
                    match db.get_mut(&key) {
                        Some(value) => {
                            if let DbRecord::String(value) = value {
                                if let Ok(number) = i64::from_str_radix(&value.get_value().get_string()?, 10) {
                                    new_value = number + 1;
                                    value.set_value(RedisValue::String(format!("{}", new_value)));
                                }  else {
                                    error = Some("ERR value is not an integer or out of range");
                                }
                            }
                        },
                        None => {
                            db.insert(key, DbRecord::String(StringRecord::new(RedisValue::String("1".to_string()))));
                            new_value = 1;
                        }
                    }
                    if let Some(error) = error {
                        RedisValue::Error(format!("{}", error)).encode()
                    } else {
                        RedisValue::Int(new_value).encode()
                    }
                }
            },
            "MULTI" => {
                if args.len() != 1 {
                    RedisValue::Error("Err wrong number of arguments for 'MULTI' command".to_string()).encode()
                } else {
                    self.multi_mode = true;
                    RedisValue::String("OK".to_string()).as_simple_string()?
                }
            },
            "DISCARD" => {
                if args.len() != 1 {
                    RedisValue::Error("Err wrong number of arguments for 'DISCARD' command".to_string()).encode()
                } else {
                    if self.multi_mode {
                        self.multi_mode = false;
                        self.queued_commands = vec![];
                        self.watched_keys = vec![];
                        RedisValue::String("OK".to_string()).as_simple_string()?
                    } else {
                        RedisValue::Error("ERR DISCARD without MULTI".to_string()).encode()
                    }
                }
            },
            "INFO" => {
                if args.len() > 2 {
                    RedisValue::Error("Err wrong number of arguments for 'INFO' command".to_string()).encode()
                } else {
                    let mut response = String::new();
                    if args.len() == 2 && args[1].get_string()?.to_lowercase() == "replication" {
                        response.push_str("# Replication\n");
                        response.push_str(&format!("role:{}\n", self.replica_info.read().await.get_role()));
                        response.push_str(&format!("master_replid:{}\n", self.replica_info.read().await.get_replid()));
                        response.push_str("master_repl_offset:0\n");
                    }
                    RedisValue::String(response).encode()
                }
            },
            "REPLCONF" => {
                if args.len() < 3 {
                    RedisValue::Error("Err wrong number of arguments for 'REPLCONF' command".to_string()).encode()
                } else {
                    match args[1].get_string()?.to_lowercase().as_str() {
                        "getack" => {
                            if !self.is_replicating {
                                return Ok(RedisValue::Error("Err cannot answer 'REPLCONF GETACK' request because this is not a replica.".to_string()).encode());
                            }
                            self.send(&RedisValue::array_from_string_vec(vec!["REPLCONF", "ACK", &format!("{}", &self.processed_bytes)]).encode(), true).await?;
                        },
                        "ack" => {
                            match &self.ack_sender {
                                Some(ack_sender) => {
                                    let ack_bytes = usize::from_str_radix(&args[2].get_string()?, 10)?;
                                    ack_sender.send(ack_bytes)?;
                                    self.prevent_send = true;
                                },
                                None => {
                                    return Ok(RedisValue::Error("Err cannot answer 'REPLCONF ACK' request because you are not registered as a replica.".to_string()).encode());
                                }
                            }
                        },
                        _ => (),
                    }
                    RedisValue::String("OK".to_string()).as_simple_string()?
                }
            },
            "PSYNC" => {
                if args.len() != 3 {
                    RedisValue::Error("Err wrong number of arguments for 'PSYNC' command".to_string()).encode()
                } else {
                    // Create communication channels for this replica
                    let (sender, receiver) = unbounded_channel();
                    let (ack_sender, ack_receiver) = unbounded_channel();
                    let replica = Replica::new(sender, ack_receiver);
                    self.instruction_receiver = Some(receiver);
                    self.ack_sender = Some(ack_sender);
                    
                    let response = RedisValue::String(format!("FULLRESYNC {} 0", self.replica_info.read().await.get_replid())).as_simple_string()?;
                    self.send(&response, false).await?;
                    let mut content = vec![];
                    let hex_empty_rdb_file = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
                    {
                        let mut replicadb = self.replicas.write().await;
                        replicadb.push(Arc::new(Mutex::new(replica)));
                    }
                    let raw_content = hex::decode(hex_empty_rdb_file)?;
                    content.extend(format!("${}\r\n", raw_content.len()).as_bytes());
                    content.extend(raw_content);
                    content
                }
            },
            "WAIT" => {
                if args.len() != 3 {
                    RedisValue::Error("Err wrong number of arguments for 'WAIT' command".to_string()).encode()
                } else {
                    let replicas_ready = Arc::new(AtomicUsize::new(0));
                    let replicas_expected = usize::from_str_radix(&args[1].get_string()?, 10)?;
                    let timeout_millis = u64::from_str_radix(&args[2].get_string()?, 10)?;
                    
                    self.check_replicas(Arc::clone(&replicas_ready), replicas_expected, timeout_millis).await?;

                    let replicas_ready = replicas_ready.load(Ordering::Relaxed) as i64;
                    RedisValue::Int(replicas_ready).encode()
                }
            },
            "CONFIG" => {
                if args.len() != 3 {
                    RedisValue::Error("Err wrong number of arguments for 'CONFIG' command".to_string()).encode()
                } else {
                    if args[1].get_string()? != "GET" {
                        return Ok(RedisValue::Error("Expected 'GET' after 'CONFIG'".to_string()).encode())
                    }
                    let variable = args[2].get_string()?;
                    let mut response = vec![];
                    match variable.as_str() {
                        "dir" => {
                            response.push("dir");
                            if let Some(dirname) = &self.db_dir {
                                response.push(dirname.as_str());
                            }
                        },
                        "dbfilename" => {
                            response.push("dbfilename");
                            if let Some(filename) = &self.db_filename {
                                response.push(filename.as_str());
                            }
                        },
                        x => response.push(x),
                    }
                    RedisValue::array_from_string_vec(response).encode()
                }
            },
            "KEYS" => {
                if args.len() != 2 {
                    RedisValue::Error("Err wrong number of arguments for 'KEYS' command".to_string()).encode()
                } else {
                    let mut response = vec![];
                    let db = self.db.read().await;
                    for key in db.keys() {
                        response.push(key.as_str());
                    }
                    RedisValue::array_from_string_vec(response).encode()
                }
            },
            "ZADD" => {
                if args.len() != 4 {
                    RedisValue::Error("Err wrong number of arguments for 'ZADD' command".to_string()).encode()
                } else {
                    let key = args[1].get_string()?;
                    let score = args[2].get_string()?.parse::<f64>()?;
                    let member = args[3].get_string()?;
                    let entry = SortedSetEntry::new(&member, score);
                    let mut added_members = 0;
                    
                    let mut db = self.db.write().await;
                    if let Some(record) = db.get_mut(&key) {
                        if let DbRecord::SortedSet(record) = record {
                            let n = record.insert(entry);
                            added_members = n;
                        }
                    } else {
                        let mut record = SortedSetRecord::new();
                        let n = record.insert(entry);
                        db.insert(key, DbRecord::SortedSet(record));
                        added_members = n;
                    }
                    RedisValue::Int(added_members).encode()
                }
            },
            "ZRANK" => {
                if args.len() != 3 {
                    RedisValue::Error("Err wrong number of arguments for 'ZRANK' command".to_string()).encode()
                } else {
                    let key = args[1].get_string()?;
                    let member = args[2].get_string()?;
                    let db = self.db.read().await;
                    if let Some(record) = db.get(&key) {
                        if let DbRecord::SortedSet(record) = record && let Some(rank) = record.get_rank(&member) {
                            RedisValue::Int(rank).encode()
                        } else {
                            RedisValue::NullString.encode()
                        }
                    } else {
                        RedisValue::NullString.encode()
                    }
                }
            },
            "ZRANGE" => {
                if args.len() != 4 {
                    RedisValue::Error("Err wrong number of arguments for 'ZRANGE' command".to_string()).encode()
                } else {
                    let key = args[1].get_string()?;
                    let db = self.db.read().await;
                    if let Some(record) = db.get(&key) && let DbRecord::SortedSet(set) = record {
                        let mut lower_end = args[2].get_string()?.parse::<i64>()?;
                        let mut higher_end = args[3].get_string()?.parse::<i64>()?;
                        if lower_end < 0 {
                            lower_end = max(lower_end + set.len() as i64, 0);
                        }
                        if higher_end < 0 {
                            higher_end = max(higher_end + set.len() as i64, 0);
                        } else if higher_end >= set.len() as i64 {
                            higher_end = set.len() as i64 - 1;
                        }
                        if higher_end < lower_end || lower_end >= set.len() as i64 {
                            RedisValue::Array(vec![]).encode()
                        } else {
                            let members = set.get_range(lower_end as usize, higher_end as usize);
                            let response_array = members.iter().map(|ss| ss.get_value()).collect();
                            RedisValue::array_from_string_vec(response_array).encode()
                        }
                    } else {
                        RedisValue::Array(vec![]).encode()
                    }
                }
            },
            "ZCARD" => {
                if args.len() != 2 {
                    RedisValue::Error("Err wrong number of arguments for 'ZCARD' command".to_string()).encode()
                } else {
                    let key = args[1].get_string()?;
                    let db = self.db.read().await;
                    if let Some(record) = db.get(&key) && let DbRecord::SortedSet(set) = record {
                        RedisValue::Int(set.len() as i64).encode()
                    } else {
                        RedisValue::Int(0).encode()
                    }
                }
            },
            "ZSCORE" => {
                if args.len() != 3 {
                    RedisValue::Error("Err wrong number of arguments for 'ZSCORE' command".to_string()).encode()
                } else {
                    let key = args[1].get_string()?;
                    let member_name = args[2].get_string()?;
                    let db = self.db.read().await;
                    if let Some(record) = db.get(&key) && let DbRecord::SortedSet(set) = record && let Some(member) = set.get(&member_name) {
                        RedisValue::String(format!("{}", member.get_score())).encode()
                    } else {
                        RedisValue::NullString.encode()
                    }
                }
            },
            "ZREM" => {
                if args.len() != 3 {
                    RedisValue::Error("Err wrong number of arguments for 'ZREM' command".to_string()).encode()
                } else {
                    let key = args[1].get_string()?;
                    let member_name = args[2].get_string()?;
                    let mut db = self.db.write().await;
                    if let Some(record) = db.get_mut(&key) && let DbRecord::SortedSet(set) = record {
                        let n = set.remove(&member_name);
                        RedisValue::Int(n).encode()
                    } else {
                        RedisValue::Int(0).encode()
                    }
                }
            },
            "WATCH" => {
                if args.len() < 2 {
                    RedisValue::Error("Err wrong number of arguments for 'WATCH' command".to_string()).encode()
                } else {
                    if self.multi_mode {
                        RedisValue::Error("ERR WATCH inside MULTI is not allowed".to_string()).encode()
                    } else {
                        for key in &args[1..] {
                            let key = key.get_string()?;
                            let db = self.db.read().await;
                            let value = match db.get(&key) {
                                Some(DbRecord::String(string_record)) => {
                                    match string_record.get_value() {
                                        RedisValue::String(value) => Some(value.clone()),
                                        _ => None
                                    }
                                },
                                _ => None,
                            };
                            self.watched_keys.push((key, value));
                        }
                        RedisValue::String("OK".to_string()).as_simple_string()?
                    }
                }
            },
            "UNWATCH" => {
                if args.len() != 1 {
                    RedisValue::Error("Err wrong number of arguments for 'UNWATCH' command".to_string()).encode()
                } else {
                    self.watched_keys = vec![];
                    RedisValue::String("OK".to_string()).as_simple_string()?
                }
            },
            "GEOADD" => {
                if args.len() != 5 {
                    RedisValue::Error("Err wrong number of arguments for 'GEOADD' command".to_string()).encode()
                } else {
                    let key = args[1].get_string()?;
                    let longitude = args[2].get_string()?;
                    let latitude = args[3].get_string()?;
                    let name = args[4].get_string()?;

                    let longitude = longitude.parse::<f64>()?;
                    let latitude = latitude.parse::<f64>()?;

                    if longitude < -180.0 || longitude > 180.0 || latitude < -85.05112878 || latitude > 85.05112878 {
                        RedisValue::Error(format!("ERR invalid longitude,latitude pair {:.6},{}", longitude, latitude)).encode()
                    } else {
                        let entry = SortedSetEntry::new(&name, location_to_score(longitude, latitude));
                        let mut db = self.db.write().await;
                        match db.get_mut(&key) {
                            Some(record) => {
                                if let DbRecord::SortedSet(record) = record {
                                    record.insert(entry);
                                }
                            },
                            None => {
                                let mut record = SortedSetRecord::new();
                                record.insert(entry);
                                db.insert(key, DbRecord::SortedSet(record));
                            }
                        }
                        RedisValue::Int(1).encode()
                    }

                }
            },
            c => RedisValue::Error(format!("Err unknown command '{}'", c)).encode(),
        };
        Ok(response)
    }

    async fn exec_queued(&mut self) -> Result<Vec<u8>> {
        if self.multi_mode {
            for (k, v) in &self.watched_keys {
                let db = self.db.read().await;
                let new_value = match db.get(k) {
                    Some(DbRecord::String(string_record)) => {
                        match string_record.get_value() {
                            RedisValue::String(value) => Some(value.clone()),
                            _ => None
                        }
                    },
                    _ => None
                };
                if new_value != *v {
                    self.multi_mode = false;
                    self.watched_keys = vec![];
                    self.queued_commands = vec![];
                    return Ok(RedisValue::NullArray.encode())
                }
            }
            let mut outputs = vec![];
            for queued_command in self.queued_commands.clone() {
                let command = &queued_command[0];
                let value = self.execute_command(&command.get_string()?, queued_command.clone()).await?;
                outputs.push(value);
            }
            let mut exec_output = format!("*{}\r\n", outputs.len()).as_bytes().to_vec();
            for output in outputs {
                exec_output.extend(output);
            }
            self.multi_mode = false;
            self.watched_keys = vec![];
            Ok(exec_output)
        } else {
            Ok(RedisValue::Error("ERR EXEC without MULTI".to_string()).encode())
        }
    }
}
