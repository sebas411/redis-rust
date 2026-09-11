use super::*;

impl ClientHandler {
    pub(super) async fn execute_core_command(
        &mut self,
        command: &str,
        args: Vec<RedisValue>,
    ) -> Result<Vec<u8>> {
        let response = match command {
            "PING" => {
                if self.subscribe_mode {
                    let mut response = vec![];
                    response.push(RedisValue::String("pong".to_string()));
                    response.push(RedisValue::String("".to_string()));
                    RedisValue::Array(response).encode()
                } else {
                    RedisValue::String("PONG".to_string()).as_simple_string()?
                }
            }
            "ECHO" => {
                if args.len() != 2 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'ECHO' command".to_string(),
                    )
                    .encode()
                } else {
                    args[1].encode()
                }
            }
            "SET" => {
                if args.len() < 3 {
                    RedisValue::Error("Err wrong number of arguments for 'SET' command".to_string())
                        .encode()
                } else {
                    let key = args[1].clone().get_string()?;
                    let value = args[2].clone();
                    let record;
                    if args.len() > 4 && args[3].get_string()?.to_uppercase() == "PX" {
                        let milliseconds_limit =
                            usize::from_str_radix(args[4].get_string()?.as_str(), 10)?;
                        let now = Utc::now();
                        let delta = TimeDelta::milliseconds(milliseconds_limit as i64);
                        let limit = now.checked_add_signed(delta).unwrap();
                        record = StringRecord::new_with_limit(value, limit);
                    } else if args.len() > 4 && args[3].get_string()?.to_uppercase() == "EX" {
                        let seconds_limit =
                            usize::from_str_radix(args[4].get_string()?.as_str(), 10)?;
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
            }
            "GET" => {
                if args.len() != 2 {
                    RedisValue::Error("Err wrong number of arguments for 'GET' command".to_string())
                        .encode()
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
                        }
                        None => RedisValue::NullString.encode(),
                    }
                }
            }
            "TYPE" => {
                if args.len() != 2 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'TYPE' command".to_string(),
                    )
                    .encode()
                } else {
                    let varname = args[1].get_string()?;
                    let db = self.db.read().await;
                    match db.get(&varname) {
                        Some(record) => RedisValue::String(record.get_type()).as_simple_string()?,
                        None => RedisValue::String("none".to_string()).as_simple_string()?,
                    }
                }
            }
            "INCR" => {
                if args.len() != 2 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'INCR' command".to_string(),
                    )
                    .encode()
                } else {
                    let key = args[1].get_string()?;
                    let mut new_value = 0;
                    let mut db = self.db.write().await;
                    let mut error = None;
                    match db.get_mut(&key) {
                        Some(value) => {
                            if let DbRecord::String(value) = value {
                                if let Ok(number) =
                                    i64::from_str_radix(&value.get_value().get_string()?, 10)
                                {
                                    new_value = number + 1;
                                    value.set_value(RedisValue::String(format!("{}", new_value)));
                                } else {
                                    error = Some("ERR value is not an integer or out of range");
                                }
                            }
                        }
                        None => {
                            db.insert(
                                key,
                                DbRecord::String(StringRecord::new(RedisValue::String(
                                    "1".to_string(),
                                ))),
                            );
                            new_value = 1;
                        }
                    }
                    if let Some(error) = error {
                        RedisValue::Error(format!("{}", error)).encode()
                    } else {
                        RedisValue::Int(new_value).encode()
                    }
                }
            }
            _ => unreachable!("command routed to the wrong handler: {command}"),
        };
        Ok(response)
    }
}
