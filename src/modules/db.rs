use std::collections::{HashMap, HashSet, VecDeque};
use chrono::{DateTime, Utc};
use tokio::sync::mpsc::UnboundedSender;
use crate::modules::values::RedisValue;

pub type DB = HashMap<String, DbRecord>;

pub enum DbRecord {
    String(StringRecord),
    List(ListRecord),
    Stream(StreamRecord),
    SortedSet(SortedSetRecord),
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
            Self::SortedSet(_) => "sorted_set".to_string(),
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

    pub fn set_value(&mut self, value: RedisValue) {
        self.value = value;
    } 

    pub fn get_value(&self) -> &RedisValue {
        &self.value
    }
}

pub struct StreamRecord {
    entries: Vec<StreamEntry>,
    waiters: VecDeque<UnboundedSender<StreamEntry>>,
}

impl StreamRecord {
    pub fn new() -> Self {
        Self { entries: vec![], waiters: VecDeque::new() }
    }
    pub fn push(&mut self, entry: StreamEntry) {
        let mut to_remove = vec![];
        for i in 0..self.waiters.len() {
            if let Some(waiter) = self.waiters.get(i) {
                let result = waiter.send(entry.clone());
                if result.is_err() {
                    to_remove.push(i);
                }
            }
        }
        for rem in to_remove {
            self.waiters.remove(rem);
        }
        self.entries.push(entry);
    }
    pub fn subscribe_waiter(&mut self, waiter: UnboundedSender<StreamEntry>) {
        self.waiters.push_back(waiter);
    }
    pub fn peek_last(&self) -> StreamEntry {
        match self.entries.last() {
            None => StreamEntry::new("0-0", None),
            Some(entry) => entry.clone(),
        }
    }
}

impl<'a> IntoIterator for &'a StreamRecord {
    type Item = &'a StreamEntry;
    type IntoIter = std::slice::Iter<'a, StreamEntry>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

#[derive(Debug, Clone)]
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

pub struct SortedSetRecord {
    set: Vec<SortedSetEntry>
}

impl SortedSetRecord {
    pub fn new() -> Self {
        Self { set: vec![] }
    }
    pub fn insert(&mut self, entry: SortedSetEntry) -> i64 {
        if !self.contains(&entry.value) {
            let i = self.set.partition_point(|e| e < &entry);
            self.set.insert(i, entry);
            1
        } else {
            for member in &mut self.set {
                if member.value == entry.value {
                    member.score = entry.score;
                    break;
                }
            }
            0
        }
    }
    pub fn get(&self, member_name: &str) -> Option<SortedSetEntry> {
        let mut result = None;
        for member in &self.set {
            if member.value == member_name {
                result = Some(member.clone())
            }
        }
        result
    }
    pub fn len(&self) -> usize {
        self.set.len()
    }
    pub fn get_rank(&self, member_name: &str) -> Option<i64> {
        for (i, member) in self.set.iter().enumerate() {
            if member.value == member_name {
                return Some(i as i64);
            }
        }
        None
    }
    pub fn get_range(&self, lower_bound: usize, upper_bound: usize) -> Vec<SortedSetEntry> {
        self.set.iter().cloned().skip(lower_bound).take(upper_bound - lower_bound + 1).collect()
    }
    fn contains(&self, member_name: &str) -> bool {
        for member in &self.set {
            if member_name == member.value {
                return true
            }
        }
        false
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SortedSetEntry {
    value: String,
    score: f64,
}

impl SortedSetEntry {
    pub fn new(value: &str, score: f64) -> Self {
        Self { value: value.to_string(), score }
    }
    pub fn get_value(&self) -> &str {
        &self.value
    }
    pub fn get_score(&self) -> f64 {
        self.score
    }
}

impl PartialOrd for SortedSetEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.score == other.score {
            Some(self.value.cmp(&other.value))
        } else {
            self.score.partial_cmp(&other.score)
        }
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