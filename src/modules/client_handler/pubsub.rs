use super::*;

impl ClientHandler {
    pub(super) async fn execute_pubsub_command(
        &mut self,
        command: &str,
        args: Vec<RedisValue>,
    ) -> Result<Vec<u8>> {
        let response = match command {
            "SUBSCRIBE" => {
                if args.len() != 2 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'SUBSCRIBE' command".to_string(),
                    )
                    .encode()
                } else {
                    let channel = args[1].get_string()?;
                    {
                        let mut reg = self.ps_registry.write().await;
                        match reg.channels.get_mut(&channel) {
                            Some(map) => {
                                map.insert(self.id);
                            }
                            None => {
                                reg.channels
                                    .insert(channel.clone(), HashSet::from([self.id]));
                            }
                        }
                        match reg.subscriptions.get_mut(&self.id) {
                            Some(map) => {
                                map.insert(channel.clone());
                            }
                            None => {
                                reg.subscriptions
                                    .insert(self.id, HashSet::from([channel.clone()]));
                            }
                        }
                    }
                    let reg = self.ps_registry.read().await;
                    let current_subscriptions = reg.subscriptions.get(&self.id).unwrap().len();
                    self.subscribe_mode = true;
                    let mut response = vec![];
                    response.push(RedisValue::String("subscribe".as_bytes().to_vec()));
                    response.push(RedisValue::String(channel.as_bytes().to_vec()));
                    response.push(RedisValue::Int(current_subscriptions as i64));
                    RedisValue::Array(response).encode()
                }
            }
            "PUBLISH" => {
                if args.len() != 3 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'PUBLISH' command".to_string(),
                    )
                    .encode()
                } else {
                    let channel = args[1].get_string()?;
                    let message_string = args[2].get_string()?;
                    let reg = self.ps_registry.read().await;
                    let current_subscriber_num;
                    if reg.channels.contains_key(&channel) {
                        let current_subscribers = reg.channels.get(&channel).unwrap();
                        for sub in current_subscribers {
                            let sender = reg.senders.get(sub).unwrap();
                            let mut response = vec![];
                            response.push(RedisValue::String("message".as_bytes().to_vec()));
                            response.push(RedisValue::String(channel.as_bytes().to_vec()));
                            response.push(RedisValue::String(message_string.as_bytes().to_vec()));
                            sender.send(RedisValue::Array(response).encode())?;
                        }
                        current_subscriber_num = current_subscribers.len();
                    } else {
                        current_subscriber_num = 0;
                    }
                    RedisValue::Int(current_subscriber_num as i64).encode()
                }
            }
            "UNSUBSCRIBE" => {
                if args.len() != 2 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'UNSUBSCRIBE' command".to_string(),
                    )
                    .encode()
                } else {
                    let channel = args[1].get_string()?;
                    {
                        let mut reg = self.ps_registry.write().await;
                        if let Some(map) = reg.channels.get_mut(&channel) {
                            map.remove(&self.id);
                        }
                        if let Some(map) = reg.subscriptions.get_mut(&self.id) {
                            map.remove(&channel);
                        }
                    }
                    let reg = self.ps_registry.read().await;
                    let current_subscriptions = reg.subscriptions.get(&self.id).unwrap().len();
                    if current_subscriptions == 0 {
                        self.subscribe_mode = false;
                    }
                    let mut response = vec![];
                    response.push(RedisValue::String("unsubscribe".as_bytes().to_vec()));
                    response.push(RedisValue::String(channel.as_bytes().to_vec()));
                    response.push(RedisValue::Int(current_subscriptions as i64));
                    RedisValue::Array(response).encode()
                }
            }
            _ => unreachable!("command routed to the wrong handler: {command}"),
        };
        Ok(response)
    }
}
