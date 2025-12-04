use anyhow::{Result, anyhow};

pub enum RedisValue {
    String(String),
    _Int(i64),
    Array(Vec<RedisValue>),
    Error(String),
}

impl RedisValue {
    pub fn get_string(&self) -> Result<String> {
        if let Self::String(s) = self {
            Ok(s.to_owned())
        } else {
            Err(anyhow!("Value is not a string."))
        }
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = vec![];
        match self {
            Self::String(s) => {
                let content = s.as_bytes().to_vec();
                let s_size = content.len();
                encoded.extend(format!("${}\r\n", s_size).as_bytes());
                encoded.extend(content);
                encoded.extend("\r\n".as_bytes());
            },
            Self::Error(e) => {
                let content = e.as_bytes().to_vec();
                encoded.push(b'-');
                encoded.extend(content);
                encoded.extend("\r\n".as_bytes());
            },
            _ => (),
        }
        encoded
    }
}
