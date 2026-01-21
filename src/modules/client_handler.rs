use std::{cmp::{max, min}, collections::{HashMap, HashSet, VecDeque}, sync::Arc, time::{SystemTime, UNIX_EPOCH}, usize};
use anyhow::{Result, anyhow};
use chrono::{TimeDelta, Utc};
use regex::Regex;
use tokio::{net::TcpStream, sync::{RwLock, mpsc::{UnboundedReceiver, unbounded_channel}}, time::{self, Duration}};

use crate::modules::{db::{DB, DbRecord, ListRecord, Registry, StreamEntry, StreamRecord, StringRecord}, parser::RedisParser, values::RedisValue};

const SUBSCRIBE_MODE_COMMANDS: [&str; 6] = ["SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT"];
const TRANSACTION_COMMANDS: [&str; 3] = ["MULTI", "EXEC", "DISCARD"];

pub struct ClientHandler {
    id: u32,
    db: Arc<RwLock<DB>>,
    ps_registry: Arc<RwLock<Registry>>,
    receiver: UnboundedReceiver<Vec<u8>>,
    subscribe_mode: bool,
    multi_mode: bool,
    queued_commands: Vec<Vec<RedisValue>>,
}


impl ClientHandler {
    pub fn new(id: u32, db: Arc<RwLock<DB>>, ps_registry: Arc<RwLock<Registry>>, receiver: UnboundedReceiver<Vec<u8>>) -> Self {
        Self { id, db, ps_registry, receiver, subscribe_mode: false, multi_mode: false, queued_commands: vec![] }
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
            c => RedisValue::Error(format!("Err unknown command '{}'", c)).encode(),
        };
        Ok(response)
    }

    async fn exec_queued(&mut self) -> Result<Vec<u8>> {
        if self.multi_mode {
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
            Ok(exec_output)
        } else {
            Ok(RedisValue::Error("ERR EXEC without MULTI".to_string()).encode())
        }
    }
}
