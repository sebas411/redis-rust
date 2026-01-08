use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, Utc};
use tokio::sync::mpsc::UnboundedSender;

use crate::modules::values::RedisValue;

pub enum DbRecord {
    String(StringRecord),
    List(ListRecord),
    Stream(StreamRecord),
}

impl DbRecord {
    pub fn get_string(&self) -> Option<&StringRecord> {
        match self {
            Self::String(string_record) => Some(string_record),
            _ => None
        }
    }
    pub fn get_list(&self) -> Option<&ListRecord> {
        match self {
            Self::List(list_record) => Some(list_record),
            _ => None,
        }
    }
    pub fn get_mut_list(&mut self) -> Option<&mut ListRecord> {
        match self {
            Self::List(list_record) => Some(list_record),
            _ => None,
        }
    }
    pub fn get_mut_stream(&mut self) -> Option<&mut StreamRecord> {
        match self {
            Self::Stream(stream_record) => Some(stream_record),
            _ => None,
        }
    }
    pub fn get_stream(&self) -> Option<&StreamRecord> {
        match self {
            Self::Stream(stream_record) => Some(stream_record),
            _ => None,
        }
    }
    pub fn get_type(&self) -> String{
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

    pub fn new_with_limit(value: RedisValue, limit: DateTime<Utc>) -> Self {
        Self { value, time_limit: Some(limit) }
    }

    pub fn is_valid(&self) -> bool {
        if let Some(limit) = self.time_limit {
            let now = Utc::now();
            if now >= limit {
                return false
            }
        }
        true
    }

    pub fn get_value(&self) -> &RedisValue {
        &self.value
    }
}

pub struct StreamRecord(Vec<StreamEntry>);

impl StreamRecord {
    pub fn new(entry: StreamEntry) -> Self {
        Self(vec![entry])
    }
    pub fn push(&mut self, entry: StreamEntry) {
        self.0.push(entry);
    }
    pub fn peek_last(&self) -> &StreamEntry {
        self.0.last().unwrap()
    }
}

impl<'a> IntoIterator for &'a StreamRecord {
    type Item = &'a StreamEntry;
    type IntoIter = std::slice::Iter<'a, StreamEntry>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

pub struct StreamEntry {
    id: String,
    kv: HashMap<String, String>,
}

impl StreamEntry {
    pub fn new(id: &str, values: Option<HashMap<String, String>>) -> Self {
        let stream = match values {
            Some(val) => val,
            None => HashMap::new(),
        };
        Self { id: id.to_string(), kv: stream }
    }
    pub fn get_id(&self) -> &str {
        &self.id
    }
}

impl<'a> IntoIterator for &'a StreamEntry {
    type Item = (&'a String, &'a String);
    type IntoIter = std::collections::hash_map::Iter<'a, String, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.kv.iter()
    }
}

pub struct ListRecord {
    list: VecDeque<String>,
    waiters: VecDeque<UnboundedSender<String>>
}

impl ListRecord {
    pub fn new() -> Self {
        Self { list: VecDeque::new(), waiters: VecDeque::new() }
    }
    pub fn from_list(list: VecDeque<String>) -> Self {
        Self { list, waiters: VecDeque::new() }
    }
    pub fn get_list(&self) -> VecDeque<String> {
        self.list.clone()
    }
    pub fn len(&self) -> usize {
        self.list.len()
    }
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }
    pub fn push_front(&mut self, value: String) {
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
    pub fn push_back(&mut self, value: String) {
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
    pub fn pop_front(&mut self) -> Option<String> {
        self.list.pop_front()
    }
    pub fn subscribe_waiter(&mut self, waiter: UnboundedSender<String>) {
        self.waiters.push_back(waiter);
    }
}

pub struct Registry {
    pub channels: HashMap<String, HashSet<u32>>,
    pub subscriptions: HashMap<u32, HashSet<String>>,
    pub senders: HashMap<u32, UnboundedSender<Vec<u8>>>,
}

impl Registry {
    pub fn new() -> Self {
        Self { channels: HashMap::new(), subscriptions: HashMap::new(), senders: HashMap::new() }
    }
}