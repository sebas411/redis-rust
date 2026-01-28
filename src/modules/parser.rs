use std::{cmp::max, pin::Pin};
use tokio::{io::AsyncReadExt, net::{tcp::{OwnedReadHalf}}};

use anyhow::{Result, anyhow};
use crate::modules::values::RedisValue;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

fn is_digit(b: u8) -> bool {
    b >= b'0' && b <= b'9'
}

fn read_uint(blob: &[u8]) -> Result<usize> {
    let str_repr = String::from_utf8(blob.to_vec().into_iter().take_while(|b| is_digit(*b)).collect())?;
    Ok(usize::from_str_radix(&str_repr, 10)?)
}

pub struct RedisParser {
    stream: OwnedReadHalf,
    buffer: [u8; 1024],
    position: usize,
}

impl RedisParser {
    pub fn new(stream: OwnedReadHalf) -> Self {
        Self { stream, buffer: [0u8; 1024], position: 0 }
    }
    pub fn read_value(&mut self) -> BoxFuture<'_, Result<RedisValue>> {
        Box::pin(async move {
            if self.position == 0 {
                if let Err(_) = self.stream.read_exact(&mut self.buffer[..1]).await {
                    return Err(anyhow!("Client '{}' disconnected. ", self.stream.peer_addr()?))
                }
                self.position = 1;
            }
            match self.buffer[0] {
                b'+' => self.simple_string().await,
                b'*' => self.array().await,
                b'$' => self.bulk_string().await,
                b':' => self.integer().await,
                _ => Err(anyhow!("Unrecognized value start {}", self.buffer[0]))
            }
        })
    }
    async fn read_blob(&mut self) -> Result<Vec<u8>> {
        loop {
            // Look for \r in the part of the buffer we've filled so far
            let maybe_cr = self.buffer[..self.position].iter().position(|c| *c == b'\r');
            match maybe_cr {
                None => {
                    // no \r yet -> read more data
                    let nread = self.stream.read(&mut self.buffer[self.position..]).await?;
                    if nread == 0 {
                        return Err(anyhow!("Client disconnected."))
                    }
                    self.position += nread;
                },
                Some(p) => {
                    // \r found, now looking for \n
                    if p+1 >= self.position {
                        // \r is at the end of read data so lets get more bytes
                        let nread = self.stream.read(&mut self.buffer[self.position..]).await?;
                        if nread == 0 {
                            return Err(anyhow!("Client disconnected."))
                        }
                        self.position += nread;
                        continue;
                    }

                    // check next byte for \n
                    if self.buffer[p+1] != b'\n' {
                        continue;
                    } else {
                        let blob = self.buffer[..(p+2)].to_vec();
                        self.buffer.copy_within(p+2..self.position, 0);
                        self.position = max(self.position - (p + 2), 0);
                        return Ok(blob)
                    }
                }
            };
        }
    }
    async fn simple_string(&mut self) -> Result<RedisValue> {
        let blob = self.read_blob().await?;
        Ok(RedisValue::String(String::from_utf8(blob[1..blob.len()-2].to_vec())?))
    }
    async fn integer(&mut self) -> Result<RedisValue> {
        let blob = self.read_blob().await?;
        let number_string = String::from_utf8(blob[1..blob.len()-2].to_vec())?;
        let number = i64::from_str_radix(&number_string, 10)?;
        
        Ok(RedisValue::Int(number))
    }
    async fn bulk_string(&mut self) -> Result<RedisValue> {
        let blob = self.read_blob().await?;
        let n = read_uint(&blob[1..])?;
        let decoded_string = String::from_utf8(self.buffer[..n].to_vec())?;
        self.buffer.copy_within(n+2.., 0);
        self.position = max(0, self.position - (n+2));
        Ok(RedisValue::String(decoded_string))
    }
    async fn array(&mut self) -> Result<RedisValue> {
        let blob = self.read_blob().await?;
        let n = read_uint(&blob[1..])?;
        let mut values = vec![];
        for _ in 0..n {
            values.push(self.read_value().await?);
        }
        Ok(RedisValue::Array(values))
    }
}
