use std::{fs::File, io::Read, path::PathBuf, sync::Arc};
use anyhow::Result;
use tokio::sync::RwLock;
use crate::modules::{db::{DB, DbRecord, StringRecord}, values::RedisValue};


pub struct FileHandler {
    db: Arc<RwLock<DB>>,
}

impl FileHandler {
    pub fn new(db: Arc<RwLock<DB>>) -> Self {
        Self { db }
    }

    pub async fn read_file(&self, filepath: PathBuf) -> Result<()> {
        let mut buf = vec![];
        let mut file = File::open(filepath)?;
        file.read_to_end(&mut buf)?;

        let mut current = 0usize;
        current += 9; // Skip header
        while buf[current] == 0xFA { // skip metadata section
            current += 1;
            let mut size = buf[current] as usize; // skip key
            current += 1 + size;
            size = buf[current] as usize; // skip value
            match size {
                0xC0 => {
                    size = 1;
                },
                0xC1 => {
                    size = 2;
                },
                0xC2 => {
                    size = 4;
                },
                _ => ()
            }
            current += 1 + size;
        }

        current += 3; // skip to hashmap info
        let hashmap_size = buf[current] as usize;
        current += 2; // jump to first key

        for _ in 0..hashmap_size {
            current += 1; // skip type (dont care)
            let key_size = buf[current] as usize;
            let key = String::from_utf8(buf[current+1..current+1+key_size].to_vec())?;
            current += 1 + key_size; // jump to value
            let value_size = buf[current] as usize;
            let value = String::from_utf8(buf[current+1..current+1+value_size].to_vec())?;
            current += 1 + value_size; // jump to value
            { // save the key/value pair in the db
                let mut db = self.db.write().await;
                db.insert(key, DbRecord::String(StringRecord::new(RedisValue::String(value))));
            }
        }

        Ok(())
    }
}