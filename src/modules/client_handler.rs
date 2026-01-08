use std::{cmp::{max, min}, collections::{HashMap, HashSet, VecDeque}, sync::Arc, time::{SystemTime, UNIX_EPOCH}, usize};
use anyhow::{Result, anyhow};
use chrono::{DateTime, TimeDelta, Utc};
use regex::Regex;
use tokio::{net::TcpStream, sync::{RwLock, mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel}}, time::{self, Duration}};

use crate::modules::{parser::RedisParser, values::RedisValue};

const SUBSCRIBE_MODE_COMMANDS: [&str; 6] = ["SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT"];

enum DbRecord {
    String(StringRecord),
    List(ListRecord),
    Stream(StreamRecord),
}

impl DbRecord {
    fn get_string(&self) -> Option<&StringRecord> {
        match self {
            Self::String(string_record) => Some(string_record),
            _ => None
        }
    }
    fn get_list(&self) -> Option<&ListRecord> {
        match self {
            Self::List(list_record) => Some(list_record),
            _ => None,
        }
    }
    fn get_mut_list(&mut self) -> Option<&mut ListRecord> {
        match self {
            Self::List(list_record) => Some(list_record),
            _ => None,
        }
    }
    fn get_mut_stream(&mut self) -> Option<&mut StreamRecord> {
        match self {
            Self::Stream(stream_record) => Some(stream_record),
            _ => None,
        }
    }
    fn get_stream(&self) -> Option<&StreamRecord> {
        match self {
            Self::Stream(stream_record) => Some(stream_record),
            _ => None,
        }
    }
    fn get_type(&self) -> String{
        match self {
            Self::List(_) => "list".to_string(),
            Self::String(_) => "string".to_string(),
            Self::Stream(_) => "stream".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StringRecord {
    value: RedisValue,
    time_limit: Option<DateTime<Utc>>,
}

impl StringRecord {
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

struct StreamRecord(Vec<StreamEntry>);

impl StreamRecord {
    fn new(entry: StreamEntry) -> Self {
        Self(vec![entry])
    }
    fn push(&mut self, entry: StreamEntry) {
        self.0.push(entry);
    }
    fn peek_last(&self) -> &StreamEntry {
        self.0.last().unwrap()
    }
}

struct StreamEntry {
    id: String,
    kv: HashMap<String, String>,
}

impl StreamEntry {
    fn new(id: &str, values: Option<HashMap<String, String>>) -> Self {
        let stream = match values {
            Some(val) => val,
            None => HashMap::new(),
        };
        Self { id: id.to_string(), kv: stream }
    }
}

pub struct ListRecord {
    list: VecDeque<String>,
    waiters: VecDeque<UnboundedSender<String>>
}

impl ListRecord {
    fn new() -> Self {
        Self { list: VecDeque::new(), waiters: VecDeque::new() }
    }
    fn from_list(list: VecDeque<String>) -> Self {
        Self { list, waiters: VecDeque::new() }
    }
    fn push_front(&mut self, value: String) {
        if !self.waiters.is_empty() {
            let waiter = self.waiters.pop_front().unwrap();
            let mut result = waiter.send(value.clone());
            while result.is_err() {
                if self.waiters.is_empty() {
                    self.list.push_front(value);
                    return;
                }
                let waiter = self.waiters.pop_front().unwrap();
                result = waiter.send(value.clone());
            }
        } else {
            self.list.push_front(value);
        }
    }
    fn push_back(&mut self, value: String) {
        if !self.waiters.is_empty() {
            let waiter = self.waiters.pop_front().unwrap();
            let mut result = waiter.send(value.clone());
            while result.is_err() {
                if self.waiters.is_empty() {
                    self.list.push_back(value);
                    return;
                }
                let waiter = self.waiters.pop_front().unwrap();
                result = waiter.send(value.clone());
            }
        } else {
            self.list.push_back(value);
        }
    }
    fn pop_front(&mut self) -> Option<String> {
        self.list.pop_front()
    }
    fn subscribe_waiter(&mut self, waiter: UnboundedSender<String>) {
        self.waiters.push_back(waiter);
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
    db: Arc<RwLock<DB>>,
    ps_registry: Arc<RwLock<Registry>>,
    receiver: UnboundedReceiver<Vec<u8>>,
    subscribe_mode: bool,
}

pub struct DB {
    kv_db: HashMap<String, DbRecord>,
    //list_db: HashMap<String, DbListRecord>,
}

impl DB {
    pub fn new() -> Self {
        Self { kv_db: HashMap::new() }
    }
}

impl ClientHandler {
    pub fn new(id: u32, db: Arc<RwLock<DB>>, ps_registry: Arc<RwLock<Registry>>, receiver: UnboundedReceiver<Vec<u8>>) -> Self {
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
                        w_db.kv_db.insert(key, DbRecord::String(record));
                    }
                    RedisValue::String("OK".to_string()).as_simple_string()?

                    
                }
            },
            "GET" => {
                if args.len() != 2 {
                    RedisValue::Error("Err wrong number of arguments for 'GET' command".to_string()).encode()
                } else {
                    let key = args[1].clone().get_string()?;
                    let r_db = self.db.read().await;
                    let map = &r_db.kv_db;
                    let record = map.get(&key);
                    match record {
                        Some(record) => {
                            let string_record = record.get_string();
                            if string_record.is_some() && string_record.unwrap().is_valid() {
                                string_record.unwrap().value.encode()
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
                        match db.kv_db.get_mut(&list_name) {
                            Some(record) => {
                                if let Some(list_record) = record.get_mut_list() {
                                    prev_records = list_record.list.len();
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
                                db.kv_db.insert(list_name.clone(), DbRecord::List(ListRecord::from_list(values)));
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
                    let list = match db.kv_db.get(&list_name) {
                        Some(record) => {
                            if let Some(list_record) = record.get_list() {
                                list_record.list.clone()
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
                        match db.kv_db.get_mut(&list_name) {
                            Some(record) => {
                                if let Some(list_record) = record.get_mut_list() {
                                    prev_records = list_record.list.len();
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
                                db.kv_db.insert(list_name.clone(), DbRecord::List(ListRecord::from_list(values)));
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
                    let list_len = self.db.read().await.kv_db.get(&list_name).unwrap_or(&DbRecord::List(ListRecord::new())).get_list().unwrap_or(&ListRecord::new()).list.len();
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
                        if let Some(record) = db.kv_db.get_mut(&list_name) && let Some(list_record) = record.get_mut_list() {
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
                        let list_record = match db.kv_db.get_mut(&list_name) {
                            None => {
                                db.kv_db.insert(list_name.clone(), DbRecord::List(ListRecord::new()));
                                db.kv_db.get_mut(&list_name).unwrap().get_mut_list().unwrap()
                            },
                            Some(record) => {
                                if let Some(list_record) = record.get_mut_list() {
                                    list_record
                                } else {
                                    return Err(anyhow!("Record is not of type list. Line {}", line!()))
                                }
                            }
                        };
                        if !list_record.list.is_empty() {
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
                    match db.kv_db.get(&varname) {
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
                        match db.kv_db.get_mut(&stream_name) {
                            Some(record) => {
                                if let Some(stream_record) = record.get_mut_stream() {
                                    let mut last_id = stream_record.peek_last().id.split("-");
                                    let last_milli = i64::from_str_radix(last_id.next().unwrap(), 10).unwrap();
                                    let last_seq = i64::from_str_radix(last_id.next().unwrap(), 10).unwrap();
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
                                let record = DbRecord::Stream(StreamRecord::new(stream_entry));
                                db.kv_db.insert(stream_name, record);
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
                    let lower_end = args[2].get_string()?;
                    let higher_end = args[3].get_string()?;
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
                    if let Some(record) = db.kv_db.get(&stream_name) && let Some(stream_record) = record.get_stream() {
                        for entry in &stream_record.0 {
                            let mut entry_id = entry.id.split('-');
                            let entry_millis = usize::from_str_radix(entry_id.next().unwrap(), 10).unwrap();
                            let entry_seq = usize::from_str_radix(entry_id.next().unwrap(), 10).unwrap();
                            if entry_millis < lower_milliseconds || entry_millis == lower_milliseconds && entry_seq < lower_sequence {
                                continue;
                            } else if entry_millis > higher_milliseconds || entry_millis == higher_milliseconds && entry_seq > higher_sequence {
                                break;
                            }
                            let mut entry_array = vec![];
                            entry_array.push(RedisValue::String(entry.id.clone()));
                            let mut values_array = vec![];
                            for (k, v) in &entry.kv {
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
            c => RedisValue::Error(format!("Err unknown command '{}'", c)).encode(),
        };
        Ok(response)
    }
}

