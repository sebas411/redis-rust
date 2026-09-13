use super::*;

impl ClientHandler {
    pub(super) async fn execute_optimistic_locking_command(
        &mut self,
        command: &str,
        args: Vec<RedisValue>,
    ) -> Result<Vec<u8>> {
        let response = match command {
            "WATCH" => {
                if args.len() < 2 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'WATCH' command".to_string(),
                    )
                    .encode()
                } else {
                    if self.multi_mode {
                        RedisValue::Error("ERR WATCH inside MULTI is not allowed".to_string())
                            .encode()
                    } else {
                        for key in &args[1..] {
                            let key = key.get_string()?;
                            let db = self.db.read().await;
                            let value = match db.get(&key) {
                                Some(DbRecord::String(string_record)) => {
                                    match string_record.get_value() {
                                        RedisValue::String(value) => {
                                            let s = String::from_utf8_lossy(value).into_owned();
                                            Some(s)
                                        },
                                        _ => None,
                                    }
                                }
                                _ => None,
                            };
                            self.watched_keys.push((key, value));
                        }
                        RedisValue::String("OK".as_bytes().to_vec()).as_simple_string()?
                    }
                }
            }
            "UNWATCH" => {
                if args.len() != 1 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'UNWATCH' command".to_string(),
                    )
                    .encode()
                } else {
                    self.watched_keys = vec![];
                    RedisValue::String("OK".as_bytes().to_vec()).as_simple_string()?
                }
            }
            _ => unreachable!("command routed to the wrong handler: {command}"),
        };
        Ok(response)
    }
}
