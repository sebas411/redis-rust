use super::*;

impl ClientHandler {
    pub(super) async fn execute_persistence_command(
        &mut self,
        command: &str,
        args: Vec<RedisValue>,
    ) -> Result<Vec<u8>> {
        let response = match command {
            "CONFIG" => {
                if args.len() != 3 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'CONFIG' command".to_string(),
                    )
                    .encode()
                } else {
                    if args[1].get_string()? != "GET" {
                        return Ok(
                            RedisValue::Error("Expected 'GET' after 'CONFIG'".to_string()).encode(),
                        );
                    }
                    let variable = args[2].get_string()?;
                    let mut response = vec![];
                    response.push(variable.as_str());
                    if let Some(value) = self.config.get(&variable) {
                        response.push(value);
                    }
                    RedisValue::array_from_string_vec(response).encode()
                }
            }
            "KEYS" => {
                if args.len() != 2 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'KEYS' command".to_string(),
                    )
                    .encode()
                } else {
                    let mut response = vec![];
                    let db = self.db.read().await;
                    for key in db.keys() {
                        response.push(key.as_str());
                    }
                    RedisValue::array_from_string_vec(response).encode()
                }
            }
            _ => unreachable!("command routed to the wrong handler: {command}"),
        };
        Ok(response)
    }
}
