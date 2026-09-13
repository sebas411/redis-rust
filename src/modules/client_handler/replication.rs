use super::*;

impl ClientHandler {
    pub(super) async fn execute_replication_command(
        &mut self,
        command: &str,
        args: Vec<RedisValue>,
    ) -> Result<Vec<u8>> {
        let response = match command {
            "INFO" => {
                if args.len() > 2 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'INFO' command".to_string(),
                    )
                    .encode()
                } else {
                    let mut response = String::new();
                    if args.len() == 2 && args[1].get_string()?.to_lowercase() == "replication" {
                        response.push_str("# Replication\n");
                        response.push_str(&format!(
                            "role:{}\n",
                            self.replica_info.read().await.get_role()
                        ));
                        response.push_str(&format!(
                            "master_replid:{}\n",
                            self.replica_info.read().await.get_replid()
                        ));
                        response.push_str("master_repl_offset:0\n");
                    }
                    RedisValue::String(response.as_bytes().to_vec()).encode()
                }
            }
            "REPLCONF" => {
                if args.len() < 3 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'REPLCONF' command".to_string(),
                    )
                    .encode()
                } else {
                    match args[1].get_string()?.to_lowercase().as_str() {
                        "getack" => {
                            if !self.is_replicating {
                                return Ok(RedisValue::Error("Err cannot answer 'REPLCONF GETACK' request because this is not a replica.".to_string()).encode());
                            }
                            self.send(
                                &RedisValue::array_from_string_vec(vec![
                                    "REPLCONF",
                                    "ACK",
                                    &format!("{}", &self.processed_bytes),
                                ])
                                .encode(),
                                true,
                            )
                            .await?;
                        }
                        "ack" => match &self.ack_sender {
                            Some(ack_sender) => {
                                let ack_bytes = usize::from_str_radix(&args[2].get_string()?, 10)?;
                                ack_sender.send(ack_bytes)?;
                                self.prevent_send = true;
                            }
                            None => {
                                return Ok(RedisValue::Error("Err cannot answer 'REPLCONF ACK' request because you are not registered as a replica.".to_string()).encode());
                            }
                        },
                        _ => (),
                    }
                    RedisValue::String("OK".as_bytes().to_vec()).as_simple_string()?
                }
            }
            "PSYNC" => {
                if args.len() != 3 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'PSYNC' command".to_string(),
                    )
                    .encode()
                } else {
                    // Create communication channels for this replica
                    let (sender, receiver) = unbounded_channel();
                    let (ack_sender, ack_receiver) = unbounded_channel();
                    let replica = Replica::new(sender, ack_receiver);
                    self.instruction_receiver = Some(receiver);
                    self.ack_sender = Some(ack_sender);

                    let response = RedisValue::String(format!(
                        "FULLRESYNC {} 0",
                        self.replica_info.read().await.get_replid()
                    ).as_bytes().to_vec())
                    .as_simple_string()?;
                    self.send(&response, false).await?;
                    let mut content = vec![];
                    let hex_empty_rdb_file = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
                    {
                        let mut replicadb = self.replicas.write().await;
                        replicadb.push(Arc::new(Mutex::new(replica)));
                    }
                    let raw_content = hex::decode(hex_empty_rdb_file)?;
                    content.extend(format!("${}\r\n", raw_content.len()).as_bytes());
                    content.extend(raw_content);
                    content
                }
            }
            "WAIT" => {
                if args.len() != 3 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'WAIT' command".to_string(),
                    )
                    .encode()
                } else {
                    let replicas_ready = Arc::new(AtomicUsize::new(0));
                    let replicas_expected = usize::from_str_radix(&args[1].get_string()?, 10)?;
                    let timeout_millis = u64::from_str_radix(&args[2].get_string()?, 10)?;

                    self.check_replicas(
                        Arc::clone(&replicas_ready),
                        replicas_expected,
                        timeout_millis,
                    )
                    .await?;

                    let replicas_ready = replicas_ready.load(Ordering::Relaxed) as i64;
                    RedisValue::Int(replicas_ready).encode()
                }
            }
            _ => unreachable!("command routed to the wrong handler: {command}"),
        };
        Ok(response)
    }
}
