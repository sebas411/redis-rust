use super::*;

impl ClientHandler {
    pub(super) async fn execute_bitmaps_command(
        &mut self,
        command: &str,
        args: Vec<RedisValue>,
    ) -> Result<Vec<u8>> {
        let response = match command {
            "SETBIT" => {
                if args.len() != 4 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'SETBIT' command".to_string(),
                    )
                    .encode()
                } else {
                    let key = args[1].clone().get_string()?;
                    let offset = args[2].clone().get_string()?.parse::<i32>()?;
                    let value = args[3].clone().get_string()?.parse::<u8>()?;
                    let my_mask = 1 << (7 - (offset % 8));
                    let record;
                    let mut original = 0;
                    match self.db.read().await.get(&key) {
                        Some(DbRecord::String(s_record)) => {
                            let mut raw = vec![];
                            if let RedisValue::String(old_value) = s_record.get_value() {
                                raw.extend(old_value);
                            }
                            for _ in raw.len() .. (offset / 8 + 1) as usize {
                                raw.push(0);
                            }
                            if let Some(mut_byte) = raw.get_mut((offset / 8) as usize) {
                                if *mut_byte & my_mask > 0 {
                                    original = 1;
                                }
                                if value == 1 {
                                    *mut_byte |= my_mask;
                                } else {
                                    *mut_byte &= !my_mask;
                                }
                            }
                            record = StringRecord::new(RedisValue::String(raw));
                        }
                        _ => {
                            let mut raw = vec![];
                            for _ in 0 .. offset / 8 {
                                raw.push(0);
                            }
                            raw.push(value << (7 - (offset % 8)));
                            record = StringRecord::new(RedisValue::String(raw));
                        }
                    }
                    {
                        let mut w_db = self.db.write().await;
                        w_db.insert(key, DbRecord::String(record));
                    }
                    RedisValue::Int(original).encode()
                }
            },
            "GETBIT" => {
                if args.len() != 3 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'GETBIT' command".to_string(),
                    )
                    .encode()
                } else {
                    let key = args[1].clone().get_string()?;
                    let offset = args[2].clone().get_string()?.parse::<i32>()?;
                    let mut response = 0;
                    
                    let my_mask = 1 << (7 - (offset % 8));
                    match self.db.read().await.get(&key) {
                        Some(DbRecord::String(s_record)) => {
                            if let RedisValue::String(raw) = s_record.get_value() {
                                if let Some(original_byte) = raw.get((offset / 8) as usize) {
                                    if original_byte & my_mask > 0 {
                                        response = 1;
                                    }
                                }
                            }
                        },
                        _ => ()
                    }
                    RedisValue::Int(response).encode()
                }
            },
            "STRLEN" => {
                if args.len() != 2 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'STRLEN' command".to_string(),
                    )
                    .encode()
                } else {
                    let key = args[1].clone().get_string()?;
                    let mut length = 0;
                    
                    match self.db.read().await.get(&key) {
                        Some(DbRecord::String(s_record)) => {
                            if let RedisValue::String(raw) = s_record.get_value() {
                                length = raw.len();
                            }
                        },
                        _ => ()
                    }
                    RedisValue::Int(length as i64).encode()
                }
            },
            "BITCOUNT" => {
                if !(args.len() == 2 || args.len() == 4) {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'BITCOUNT' command".to_string(),
                    )
                    .encode()
                } else {
                    let key = args[1].clone().get_string()?;
                    let mut total_bits = 0;
                    
                    match self.db.read().await.get(&key) {
                        Some(DbRecord::String(s_record)) => {
                            if let RedisValue::String(raw) = s_record.get_value() {
                                let start;
                                let end;
                                if args.len() == 4 {
                                    start = max(args[2].get_string()?.parse::<usize>()?, 0);
                                    end = min(args[3].get_string()?.parse::<usize>()?, raw.len() - 1);
                                } else {
                                    start = 0;
                                    end = raw.len() - 1;
                                }
                                for i in start..=end {
                                    let my_byte = raw[i];
                                    for j in 0..8 {
                                        let mask = 1u8 << j;
                                        if my_byte & mask > 0 {
                                            total_bits += 1;
                                        }
                                    }
                                }
                            }
                        },
                        _ => ()
                    }
                    RedisValue::Int(total_bits as i64).encode()
                }
            },
            _ => unreachable!("command routed to the wrong handler: {command}"),
        };
        Ok(response)
    }
}
