use super::*;

impl ClientHandler {
    pub(super) async fn execute_streams_command(
        &mut self,
        command: &str,
        args: Vec<RedisValue>,
    ) -> Result<Vec<u8>> {
        let response = match command {
            "XADD" => {
                if args.len() < 5 || args.len() % 2 != 1 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'XADD' command".to_string(),
                    )
                    .encode()
                } else {
                    let mut error_response = None;
                    let stream_name = args[1].get_string()?;
                    let mut entry_id = args[2].get_string()?;

                    let re = Regex::new(r"^((\d+|\*)-(\d+|\*)|\*)$").unwrap();

                    if !re.is_match(&entry_id) {
                        return Err(anyhow!("Bad format for stream id. Line {}", line!()));
                    }

                    if entry_id == "*" {
                        entry_id = "*-*".to_string();
                    }

                    let mut id_split = entry_id.split("-");
                    let milliseconds_str = id_split.next().unwrap();
                    let mut milliseconds = i64::from_str_radix(milliseconds_str, 10).unwrap_or(-1);
                    let sequence_str = id_split.next().unwrap();
                    let mut sequence = i64::from_str_radix(sequence_str, 10).unwrap_or(-1);

                    if milliseconds == 0 && sequence == 0 {
                        error_response = Some(
                            RedisValue::Error(
                                "ERR The ID specified in XADD must be greater than 0-0".to_string(),
                            )
                            .encode(),
                        )
                    }

                    let mut values = HashMap::new();

                    for i in (3..args.len()).step_by(2) {
                        let key = args[i].get_string()?;
                        let value = args[i + 1].get_string()?;
                        values.insert(key, value);
                    }

                    if error_response.is_none() {
                        let mut db = self.db.write().await;
                        match db.get_mut(&stream_name) {
                            Some(record) => {
                                if let Some(stream_record) = record.get_mut_stream() {
                                    let last_id = stream_record.peek_last();
                                    let mut last_id_split = last_id.get_id().split("-");
                                    let last_milli =
                                        i64::from_str_radix(last_id_split.next().unwrap(), 10)
                                            .unwrap();
                                    let last_seq =
                                        i64::from_str_radix(last_id_split.next().unwrap(), 10)
                                            .unwrap();
                                    if milliseconds_str == "*" {
                                        let now = SystemTime::now();
                                        let since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
                                        milliseconds = since_epoch.as_millis() as i64;
                                    }
                                    if sequence_str == "*" {
                                        if last_milli == milliseconds {
                                            sequence = last_seq + 1;
                                        } else {
                                            sequence = 0;
                                        }
                                    }
                                    entry_id = format!("{}-{}", milliseconds, sequence);
                                    let stream_entry = StreamEntry::new(&entry_id, Some(values));
                                    if last_milli > milliseconds
                                        || (last_milli == milliseconds && last_seq >= sequence)
                                    {
                                        error_response = Some(RedisValue::Error("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()).encode())
                                    } else {
                                        stream_record.push(stream_entry);
                                    }
                                }
                            }
                            None => {
                                if milliseconds_str == "*" {
                                    let now = SystemTime::now();
                                    let since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
                                    milliseconds = since_epoch.as_millis() as i64;
                                }
                                if sequence_str == "*" {
                                    if milliseconds == 0 {
                                        sequence = 1;
                                    } else {
                                        sequence = 0;
                                    }
                                }
                                entry_id = format!("{}-{}", milliseconds, sequence);
                                let stream_entry = StreamEntry::new(&entry_id, Some(values));
                                let mut stream_record = StreamRecord::new();
                                stream_record.push(stream_entry);
                                let record = DbRecord::Stream(stream_record);
                                db.insert(stream_name, record);
                            }
                        }
                    }

                    match error_response {
                        None => RedisValue::String(entry_id).encode(),
                        Some(err) => err,
                    }
                }
            }
            "XRANGE" => {
                if args.len() != 4 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'XRANGE' command".to_string(),
                    )
                    .encode()
                } else {
                    let stream_name = args[1].get_string()?;
                    let re = Regex::new(r"^\d+(-\d+)?$").unwrap();
                    let mut lower_end = args[2].get_string()?;
                    if lower_end == "-" {
                        lower_end = "0".to_string()
                    }
                    let mut higher_end = args[3].get_string()?;
                    if higher_end == "+" {
                        higher_end = format!("{}", usize::MAX)
                    }
                    if !re.is_match(&lower_end) {
                        return Err(anyhow!(
                            "Bad format for stream id in range's lower end. Line {}",
                            line!()
                        ));
                    }
                    if !re.is_match(&higher_end) {
                        return Err(anyhow!(
                            "Bad format for stream id in range's lower end. Line {}",
                            line!()
                        ));
                    }
                    let lower_milliseconds;
                    let lower_sequence;
                    if lower_end.contains('-') {
                        let mut lower_split = lower_end.split('-');
                        lower_milliseconds =
                            usize::from_str_radix(&lower_split.next().unwrap(), 10).unwrap();
                        lower_sequence =
                            usize::from_str_radix(&lower_split.next().unwrap(), 10).unwrap();
                    } else {
                        lower_milliseconds = usize::from_str_radix(&lower_end, 10).unwrap();
                        lower_sequence = 0;
                    }
                    let higher_milliseconds;
                    let higher_sequence;
                    if higher_end.contains('-') {
                        let mut higher_split = higher_end.split('-');
                        higher_milliseconds =
                            usize::from_str_radix(&higher_split.next().unwrap(), 10).unwrap();
                        higher_sequence =
                            usize::from_str_radix(&higher_split.next().unwrap(), 10).unwrap();
                    } else {
                        higher_milliseconds = usize::from_str_radix(&higher_end, 10).unwrap();
                        higher_sequence = usize::MAX;
                    }
                    let mut response_array = vec![];
                    let db = self.db.read().await;
                    if let Some(record) = db.get(&stream_name)
                        && let Some(stream_record) = record.get_stream()
                    {
                        for entry in stream_record {
                            let mut entry_id = entry.get_id().split('-');
                            let entry_millis =
                                usize::from_str_radix(entry_id.next().unwrap(), 10).unwrap();
                            let entry_seq =
                                usize::from_str_radix(entry_id.next().unwrap(), 10).unwrap();
                            if entry_millis < lower_milliseconds
                                || entry_millis == lower_milliseconds && entry_seq < lower_sequence
                            {
                                continue;
                            } else if entry_millis > higher_milliseconds
                                || entry_millis == higher_milliseconds
                                    && entry_seq > higher_sequence
                            {
                                break;
                            }
                            let mut entry_array = vec![];
                            entry_array.push(RedisValue::String(entry.get_id().to_string()));
                            let mut values_array = vec![];
                            for (k, v) in entry {
                                values_array.push(RedisValue::String(k.clone()));
                                values_array.push(RedisValue::String(v.clone()));
                            }
                            entry_array.push(RedisValue::Array(values_array));
                            response_array.push(RedisValue::Array(entry_array));
                        }
                    }
                    RedisValue::Array(response_array).encode()
                }
            }
            "XREAD" => {
                if args.len() < 4 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'XRANGE' command".to_string(),
                    )
                    .encode()
                } else {
                    let re = Regex::new(r"^\d+-\d+$").unwrap();
                    let mut response_array = vec![];
                    let mut reached_deadline = false;
                    let is_blocked;
                    let block_args;
                    let block_timeout;
                    if args[1].get_string()?.to_lowercase() == "block" {
                        is_blocked = true;
                        block_args = 2;
                        block_timeout = u64::from_str_radix(&args[2].get_string()?, 10)?;
                    } else if args[1].get_string()?.to_lowercase() == "streams" {
                        is_blocked = false;
                        block_args = 0;
                        block_timeout = 0;
                    } else {
                        return Err(anyhow!("XREAD only compatible with STREAMS"));
                    }
                    for i in 0..(args.len() - block_args - 2) / 2 {
                        let stream_name = args[2 + i + block_args].get_string()?;
                        let mut entry_id = args[(args.len() - block_args) / 2 + 1 + i + block_args]
                            .get_string()?;
                        if entry_id == "$" {
                            let db = self.db.read().await;
                            if let Some(record) = db.get(&stream_name)
                                && let Some(stream_record) = record.get_stream()
                            {
                                entry_id = stream_record.peek_last().get_id().to_string();
                            }
                        }
                        if !re.is_match(&entry_id) {
                            return Err(anyhow!("Bad format for stream id. Line {}", line!()));
                        }
                        let mut entry_id_split = entry_id.split('-');
                        let entry_milliseconds =
                            usize::from_str_radix(&entry_id_split.next().unwrap(), 10).unwrap();
                        let entry_sequence =
                            usize::from_str_radix(&entry_id_split.next().unwrap(), 10).unwrap();

                        let mut stream_array = vec![];
                        stream_array.push(RedisValue::String(stream_name.clone()));

                        let mut entries_array = vec![];

                        {
                            let db = self.db.read().await;
                            if let Some(record) = db.get(&stream_name)
                                && let Some(stream_record) = record.get_stream()
                            {
                                for entry in stream_record {
                                    let mut entry_id = entry.get_id().split('-');
                                    let entry_millis =
                                        usize::from_str_radix(entry_id.next().unwrap(), 10)
                                            .unwrap();
                                    let entry_seq =
                                        usize::from_str_radix(entry_id.next().unwrap(), 10)
                                            .unwrap();
                                    if entry_millis < entry_milliseconds
                                        || entry_millis == entry_milliseconds
                                            && entry_seq <= entry_sequence
                                    {
                                        continue;
                                    }
                                    let mut entry_array = vec![];
                                    entry_array
                                        .push(RedisValue::String(entry.get_id().to_string()));
                                    let mut values_array = vec![];
                                    for (k, v) in entry {
                                        values_array.push(RedisValue::String(k.clone()));
                                        values_array.push(RedisValue::String(v.clone()));
                                    }
                                    entry_array.push(RedisValue::Array(values_array));
                                    entries_array.push(RedisValue::Array(entry_array));
                                }
                            }
                        }
                        if is_blocked && entries_array.is_empty() {
                            let (sender, mut receiver) = unbounded_channel();
                            {
                                let mut db = self.db.write().await;
                                if db.contains_key(&stream_name) {
                                    let record = db.get_mut(&stream_name).unwrap();
                                    if let Some(stream_record) = record.get_mut_stream() {
                                        stream_record.subscribe_waiter(sender);
                                    }
                                } else {
                                    let mut stream_record = StreamRecord::new();
                                    stream_record.subscribe_waiter(sender);
                                    db.insert(stream_name, DbRecord::Stream(stream_record));
                                }
                            }
                            // wait for value
                            let mut value = None;
                            if block_timeout == 0 {
                                loop {
                                    let msg = receiver.recv().await;
                                    if let Some(entry) = &msg {
                                        let mut entry_id = entry.get_id().split('-');
                                        let entry_millis =
                                            usize::from_str_radix(entry_id.next().unwrap(), 10)
                                                .unwrap();
                                        let entry_seq =
                                            usize::from_str_radix(entry_id.next().unwrap(), 10)
                                                .unwrap();
                                        if entry_millis > entry_milliseconds
                                            || entry_millis == entry_milliseconds
                                                && entry_seq > entry_sequence
                                        {
                                            value = msg;
                                            break;
                                        }
                                    }
                                }
                            } else {
                                let deadline = time::sleep(Duration::from_millis(block_timeout));
                                tokio::pin!(deadline);
                                loop {
                                    tokio::select! {
                                        msg = receiver.recv() => {
                                            if let Some(entry) = &msg {
                                                let mut entry_id = entry.get_id().split('-');
                                                let entry_millis = usize::from_str_radix(entry_id.next().unwrap(), 10).unwrap();
                                                let entry_seq = usize::from_str_radix(entry_id.next().unwrap(), 10).unwrap();
                                                if entry_millis > entry_milliseconds || entry_millis == entry_milliseconds && entry_seq > entry_sequence {
                                                    value = msg;
                                                    break;
                                                }
                                            }
                                        }
                                        _ = &mut deadline => {
                                            reached_deadline = true;
                                            break;
                                        }
                                    }
                                }
                            }
                            if let Some(entry) = value {
                                let mut entry_array = vec![];
                                entry_array.push(RedisValue::String(entry.get_id().to_string()));
                                let mut values_array = vec![];
                                for (k, v) in &entry {
                                    values_array.push(RedisValue::String(k.clone()));
                                    values_array.push(RedisValue::String(v.clone()));
                                }
                                entry_array.push(RedisValue::Array(values_array));
                                entries_array.push(RedisValue::Array(entry_array));
                            }
                        }
                        stream_array.push(RedisValue::Array(entries_array));
                        response_array.push(RedisValue::Array(stream_array));
                    }
                    if reached_deadline {
                        RedisValue::NullArray.encode()
                    } else {
                        RedisValue::Array(response_array).encode()
                    }
                }
            }
            _ => unreachable!("command routed to the wrong handler: {command}"),
        };
        Ok(response)
    }
}
