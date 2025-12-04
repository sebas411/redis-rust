use std::{cmp::max, io::{Read, Write}, net::TcpStream};
use anyhow::{Result, anyhow};
use crate::modules::values::RedisValue;

fn is_digit(b: u8) -> bool {
    b >= b'0' && b <= b'9'
}

fn read_uint(blob: &[u8]) -> Result<usize> {
    let str_repr = String::from_utf8(blob.to_vec().into_iter().take_while(|b| is_digit(*b)).collect())?;
    Ok(usize::from_str_radix(&str_repr, 10)?)
}

pub struct RedisParser {
    stream: TcpStream,
    buffer: [u8; 1024],
    position: usize,
}

impl RedisParser {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream, buffer: [0u8; 1024], position: 0 }
    }
    pub fn read_value(&mut self) -> Result<RedisValue> {
        if self.position == 0 {
            if let Err(_) = self.stream.read_exact(&mut self.buffer[..1]) {
                return Err(anyhow!("Client '{}' disconnected. ", self.stream.peer_addr()?))
            }
            self.position = 1;
        }
        match self.buffer[0] {
            b'+' => self.simple_string(),
            b'*' => self.array(),
            b'$' => self.bulk_string(),
            _ => Err(anyhow!("Unrecognized value start {}", self.buffer[0]))
        }
    }
    pub fn send(&mut self, content: &[u8]) -> Result<()> {
        self.stream.write_all(content)?;
        Ok(())
    }
    fn read_blob(&mut self) -> Result<Vec<u8>> {
        match self.buffer[..self.position].iter().position(|c| *c == b'\r') {
            None => {
                let nread = self.stream.read(&mut self.buffer[self.position..])?;
                if nread == 0 {
                    return Err(anyhow!("Client disconnected."))
                }
                self.position += nread;
                self.read_blob()
            },
            Some(p) => {
                if self.buffer[p+1] != b'\n' {
                    self.read_blob()
                } else {
                    let blob = self.buffer[..(p+2)].to_vec();
                    self.buffer.copy_within(p+2..self.position, 0);
                    self.position = max(self.position - (p + 2), 0);
                    Ok(blob)
                }
            }
        }
    }
    fn simple_string(&mut self) -> Result<RedisValue> {
        let blob = self.read_blob()?;
        Ok(RedisValue::String(String::from_utf8(blob[1..blob.len()-2].to_vec())?))
    }
    fn bulk_string(&mut self) -> Result<RedisValue> {
        let blob = self.read_blob()?;
        let n = read_uint(&blob[1..])?;
        let decoded_string = String::from_utf8(self.buffer[..n].to_vec())?;
        self.buffer.copy_within(n+2.., 0);
        self.position = max(0, self.position - (n+2));
        Ok(RedisValue::String(decoded_string))
    }
    fn array(&mut self) -> Result<RedisValue> {
        let blob = self.read_blob()?;
        let n = read_uint(&blob[1..])?;
        let mut values = vec![];
        for _ in 0..n {
            values.push(self.read_value()?);
        }
        Ok(RedisValue::Array(values))
    }
}
