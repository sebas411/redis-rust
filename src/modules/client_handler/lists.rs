use super::*;

impl ClientHandler {
    pub(super) async fn execute_lists_command(
        &mut self,
        command: &str,
        args: Vec<RedisValue>,
    ) -> Result<Vec<u8>> {
        let response = match command {
            "RPUSH" => {
                if args.len() < 3 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'RPUSH' command".to_string(),
                    )
                    .encode()
                } else {
                    let list_name = args[1].get_string()?;
                    let prev_records;
                    let pushed_records = args.len() - 2;
                    {
                        let mut db = self.db.write().await;
                        match db.get_mut(&list_name) {
                            Some(record) => {
                                if let Some(list_record) = record.get_mut_list() {
                                    prev_records = list_record.len();
                                    for val in args.iter().skip(2) {
                                        list_record.push_back(val.get_string()?);
                                    }
                                } else {
                                    return Err(anyhow!(
                                        "Record is not of type list. Line {}",
                                        line!()
                                    ));
                                }
                            }
                            None => {
                                let mut values = VecDeque::new();
                                prev_records = 0;
                                for val in args.iter().skip(2) {
                                    values.push_back(val.get_string()?);
                                }
                                db.insert(
                                    list_name.clone(),
                                    DbRecord::List(ListRecord::from_list(values)),
                                );
                            }
                        }
                    }
                    RedisValue::Int((prev_records + pushed_records) as i64).encode()
                }
            }
            "LRANGE" => {
                if args.len() != 4 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'LRANGE' command".to_string(),
                    )
                    .encode()
                } else {
                    let list_name = args[1].get_string()?;
                    let start_string = args[2].get_string()?;
                    let stop_string = args[3].get_string()?;

                    let mut start = i64::from_str_radix(&start_string, 10)?;
                    let mut stop = i64::from_str_radix(&stop_string, 10)?;

                    let db = self.db.read().await;
                    let list = match db.get(&list_name) {
                        Some(record) => {
                            if let Some(list_record) = record.get_list() {
                                list_record.get_list()
                            } else {
                                VecDeque::new()
                            }
                        }
                        None => VecDeque::new(),
                    };
                    let list_len = list.len() as i64;

                    if start < 0 {
                        start = max(list_len + start, 0)
                    }
                    if stop < 0 {
                        stop = max(list_len + stop, 0)
                    }
                    stop = min(stop, list_len - 1);

                    let start = start as usize;
                    let stop = stop as usize;

                    let mut return_list = vec![];
                    if start < list.len() && start <= stop {
                        for item in list.range(start..=stop) {
                            return_list.push(RedisValue::String(item.clone()));
                        }
                    }

                    RedisValue::Array(return_list).encode()
                }
            }
            "LPUSH" => {
                if args.len() < 3 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'LPUSH' command".to_string(),
                    )
                    .encode()
                } else {
                    let list_name = args[1].get_string()?;
                    let prev_records;
                    let pushed_records = args.len() - 2;
                    {
                        let mut db = self.db.write().await;
                        match db.get_mut(&list_name) {
                            Some(record) => {
                                if let Some(list_record) = record.get_mut_list() {
                                    prev_records = list_record.len();
                                    for val in args.iter().skip(2) {
                                        list_record.push_front(val.get_string()?);
                                    }
                                } else {
                                    return Err(anyhow!(
                                        "Record is not of type list. Line {}",
                                        line!()
                                    ));
                                }
                            }
                            None => {
                                let mut values = VecDeque::new();
                                prev_records = 0;
                                for val in args.iter().skip(2) {
                                    values.push_front(val.get_string()?);
                                }
                                db.insert(
                                    list_name.clone(),
                                    DbRecord::List(ListRecord::from_list(values)),
                                );
                            }
                        }
                    }
                    RedisValue::Int((prev_records + pushed_records) as i64).encode()
                }
            }
            "LLEN" => {
                if args.len() != 2 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'LLEN' command".to_string(),
                    )
                    .encode()
                } else {
                    let list_name = args[1].get_string()?;
                    let list_len = self
                        .db
                        .read()
                        .await
                        .get(&list_name)
                        .unwrap_or(&DbRecord::List(ListRecord::new()))
                        .get_list()
                        .unwrap_or(&ListRecord::new())
                        .len();
                    RedisValue::Int(list_len as i64).encode()
                }
            }
            "LPOP" => {
                if args.len() < 2 || args.len() > 3 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'LPOP' command".to_string(),
                    )
                    .encode()
                } else {
                    let list_name = args[1].get_string()?;
                    let pop_amount = if args.len() == 3 {
                        usize::from_str_radix(&args[2].get_string()?, 10)?
                    } else {
                        1
                    };
                    let mut returned_items = vec![];
                    {
                        let mut db = self.db.write().await;
                        if let Some(record) = db.get_mut(&list_name)
                            && let Some(list_record) = record.get_mut_list()
                        {
                            for _ in 0..pop_amount {
                                match list_record.pop_front() {
                                    Some(popped) => {
                                        returned_items.push(RedisValue::String(popped));
                                    }
                                    None => {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if pop_amount == 1 {
                        returned_items[0].encode()
                    } else {
                        RedisValue::Array(returned_items).encode()
                    }
                }
            }
            "BLPOP" => {
                if args.len() != 3 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'BLPOP' command".to_string(),
                    )
                    .encode()
                } else {
                    let list_name = args[1].get_string()?;
                    let timeout = args[2].get_string()?.parse::<f64>()?;
                    let mut value = None;
                    let mut waiter = None;
                    // block to either get the value via pop or setup a waiter for when values come
                    {
                        let mut db = self.db.write().await;
                        let list_record = match db.get_mut(&list_name) {
                            None => {
                                db.insert(list_name.clone(), DbRecord::List(ListRecord::new()));
                                db.get_mut(&list_name).unwrap().get_mut_list().unwrap()
                            }
                            Some(record) => {
                                if let Some(list_record) = record.get_mut_list() {
                                    list_record
                                } else {
                                    return Err(anyhow!(
                                        "Record is not of type list. Line {}",
                                        line!()
                                    ));
                                }
                            }
                        };
                        if !list_record.is_empty() {
                            value = list_record.pop_front();
                        } else {
                            let (sender, receiver) = unbounded_channel::<String>();
                            list_record.subscribe_waiter(sender);
                            waiter = Some(receiver);
                        }
                    }
                    // wait for some value, either with timeout or stay waiting
                    if let Some(mut receiver) = waiter {
                        if timeout == 0.0 {
                            value = receiver.recv().await;
                        } else {
                            tokio::select! {
                                result = receiver.recv() => {
                                    value = result;
                                }
                                _ = time::sleep(Duration::from_secs_f64(timeout)) => ()
                            }
                        }
                    }
                    // actually respond to the client
                    if let Some(value) = value {
                        let array = vec![RedisValue::String(list_name), RedisValue::String(value)];
                        RedisValue::Array(array).encode()
                    } else {
                        RedisValue::NullArray.encode()
                    }
                }
            }
            _ => unreachable!("command routed to the wrong handler: {command}"),
        };
        Ok(response)
    }
}
