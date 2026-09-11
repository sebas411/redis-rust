use super::*;

impl ClientHandler {
    pub(super) async fn exec_queued(&mut self) -> Result<Vec<u8>> {
        if self.multi_mode {
            for (k, v) in &self.watched_keys {
                let db = self.db.read().await;
                let new_value = match db.get(k) {
                    Some(DbRecord::String(string_record)) => match string_record.get_value() {
                        RedisValue::String(value) => Some(value.clone()),
                        _ => None,
                    },
                    _ => None,
                };
                if new_value != *v {
                    self.multi_mode = false;
                    self.watched_keys = vec![];
                    self.queued_commands = vec![];
                    return Ok(RedisValue::NullArray.encode());
                }
            }
            let mut outputs = vec![];
            for queued_command in self.queued_commands.clone() {
                let command = &queued_command[0];
                let value = self
                    .execute_command(&command.get_string()?, queued_command.clone())
                    .await?;
                outputs.push(value);
            }
            let mut exec_output = format!("*{}\r\n", outputs.len()).as_bytes().to_vec();
            for output in outputs {
                exec_output.extend(output);
            }
            self.multi_mode = false;
            self.watched_keys = vec![];
            Ok(exec_output)
        } else {
            Ok(RedisValue::Error("ERR EXEC without MULTI".to_string()).encode())
        }
    }

    pub(super) async fn execute_transactions_command(
        &mut self,
        command: &str,
        args: Vec<RedisValue>,
    ) -> Result<Vec<u8>> {
        let response = match command {
            "MULTI" => {
                if args.len() != 1 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'MULTI' command".to_string(),
                    )
                    .encode()
                } else {
                    self.multi_mode = true;
                    RedisValue::String("OK".to_string()).as_simple_string()?
                }
            }
            "DISCARD" => {
                if args.len() != 1 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'DISCARD' command".to_string(),
                    )
                    .encode()
                } else {
                    if self.multi_mode {
                        self.multi_mode = false;
                        self.queued_commands = vec![];
                        self.watched_keys = vec![];
                        RedisValue::String("OK".to_string()).as_simple_string()?
                    } else {
                        RedisValue::Error("ERR DISCARD without MULTI".to_string()).encode()
                    }
                }
            }
            _ => unreachable!("command routed to the wrong handler: {command}"),
        };
        Ok(response)
    }
}
