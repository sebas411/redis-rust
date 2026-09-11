use super::*;

impl ClientHandler {
    pub(super) async fn execute_sorted_sets_command(
        &mut self,
        command: &str,
        args: Vec<RedisValue>,
    ) -> Result<Vec<u8>> {
        let response = match command {
            "ZADD" => {
                if args.len() != 4 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'ZADD' command".to_string(),
                    )
                    .encode()
                } else {
                    let key = args[1].get_string()?;
                    let score = args[2].get_string()?.parse::<f64>()?;
                    let member = args[3].get_string()?;
                    let entry = SortedSetEntry::new(&member, score);
                    let mut added_members = 0;

                    let mut db = self.db.write().await;
                    if let Some(record) = db.get_mut(&key) {
                        if let DbRecord::SortedSet(record) = record {
                            let n = record.insert(entry);
                            added_members = n;
                        }
                    } else {
                        let mut record = SortedSetRecord::new();
                        let n = record.insert(entry);
                        db.insert(key, DbRecord::SortedSet(record));
                        added_members = n;
                    }
                    RedisValue::Int(added_members).encode()
                }
            }
            "ZRANK" => {
                if args.len() != 3 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'ZRANK' command".to_string(),
                    )
                    .encode()
                } else {
                    let key = args[1].get_string()?;
                    let member = args[2].get_string()?;
                    let db = self.db.read().await;
                    if let Some(record) = db.get(&key) {
                        if let DbRecord::SortedSet(record) = record
                            && let Some(rank) = record.get_rank(&member)
                        {
                            RedisValue::Int(rank).encode()
                        } else {
                            RedisValue::NullString.encode()
                        }
                    } else {
                        RedisValue::NullString.encode()
                    }
                }
            }
            "ZRANGE" => {
                if args.len() != 4 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'ZRANGE' command".to_string(),
                    )
                    .encode()
                } else {
                    let key = args[1].get_string()?;
                    let db = self.db.read().await;
                    if let Some(record) = db.get(&key)
                        && let DbRecord::SortedSet(set) = record
                    {
                        let mut lower_end = args[2].get_string()?.parse::<i64>()?;
                        let mut higher_end = args[3].get_string()?.parse::<i64>()?;
                        if lower_end < 0 {
                            lower_end = max(lower_end + set.len() as i64, 0);
                        }
                        if higher_end < 0 {
                            higher_end = max(higher_end + set.len() as i64, 0);
                        } else if higher_end >= set.len() as i64 {
                            higher_end = set.len() as i64 - 1;
                        }
                        if higher_end < lower_end || lower_end >= set.len() as i64 {
                            RedisValue::Array(vec![]).encode()
                        } else {
                            let members = set.get_range(lower_end as usize, higher_end as usize);
                            let response_array = members.iter().map(|ss| ss.get_value()).collect();
                            RedisValue::array_from_string_vec(response_array).encode()
                        }
                    } else {
                        RedisValue::Array(vec![]).encode()
                    }
                }
            }
            "ZCARD" => {
                if args.len() != 2 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'ZCARD' command".to_string(),
                    )
                    .encode()
                } else {
                    let key = args[1].get_string()?;
                    let db = self.db.read().await;
                    if let Some(record) = db.get(&key)
                        && let DbRecord::SortedSet(set) = record
                    {
                        RedisValue::Int(set.len() as i64).encode()
                    } else {
                        RedisValue::Int(0).encode()
                    }
                }
            }
            "ZSCORE" => {
                if args.len() != 3 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'ZSCORE' command".to_string(),
                    )
                    .encode()
                } else {
                    let key = args[1].get_string()?;
                    let member_name = args[2].get_string()?;
                    let db = self.db.read().await;
                    if let Some(record) = db.get(&key)
                        && let DbRecord::SortedSet(set) = record
                        && let Some(member) = set.get(&member_name)
                    {
                        RedisValue::String(format!("{}", member.get_score())).encode()
                    } else {
                        RedisValue::NullString.encode()
                    }
                }
            }
            "ZREM" => {
                if args.len() != 3 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'ZREM' command".to_string(),
                    )
                    .encode()
                } else {
                    let key = args[1].get_string()?;
                    let member_name = args[2].get_string()?;
                    let mut db = self.db.write().await;
                    if let Some(record) = db.get_mut(&key)
                        && let DbRecord::SortedSet(set) = record
                    {
                        let n = set.remove(&member_name);
                        RedisValue::Int(n).encode()
                    } else {
                        RedisValue::Int(0).encode()
                    }
                }
            }
            _ => unreachable!("command routed to the wrong handler: {command}"),
        };
        Ok(response)
    }
}
