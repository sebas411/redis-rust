use anyhow::{Result, anyhow};

#[derive(Debug, Clone)]
pub enum RedisValue {
    String(String),
    Int(i64),
    Array(Vec<RedisValue>),
    Error(String),
    Null,
}

impl RedisValue {
    pub fn get_string(&self) -> Result<String> {
        if let Self::String(s) = self {
            Ok(s.to_owned())
        } else {
            Err(anyhow!("Value is not a string."))
        }
    }
    pub fn _get_int(&self) -> Result<i64> {
        if let Self::Int(i) = self {
            Ok(*i)
        } else {
            Err(anyhow!("Value is not an integer."))
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
            Self::Null => {
                encoded.extend(b"$-1\r\n");
            },
            Self::Int(i) => {
                encoded.extend(format!(":{}\r\n", i).as_bytes());
            },
            Self::Array(values) => {
                encoded.extend(format!("*{}\r\n", values.len()).as_bytes());
                for value in values {
                    encoded.extend(value.encode());
                }
            },
        }
        encoded
    }
    pub fn as_simple_string(&self) -> Result<Vec<u8>> {
        if let Self::String(s) = self {
            let mut encoded = vec![];
            let content = s.as_bytes().to_vec();
            encoded.push(b'+');
            encoded.extend(content);
            encoded.extend("\r\n".as_bytes());
            Ok(encoded)
        } else {
            Err(anyhow!("Value type is not a string."))
        }
    }
}
