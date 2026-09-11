use super::*;

impl ClientHandler {
    pub(super) async fn execute_acl_command(
        &mut self,
        command: &str,
        args: Vec<RedisValue>,
    ) -> Result<Vec<u8>> {
        let response = match command {
            "ACL" => {
                if args.len() < 2 {
                    RedisValue::Error("Err wrong number of arguments for 'ACL' command".to_string())
                        .encode()
                } else {
                    let acl_command = args[1].get_string()?;
                    let mut response = vec![];
                    match acl_command.as_str() {
                        "WHOAMI" => {
                            if let Some(user) = &self.current_user {
                                response = RedisValue::String(user.to_string()).encode()
                            }
                        }
                        "GETUSER" => {
                            let user_req = args[2].get_string()?;
                            response = self
                                .users
                                .read()
                                .await
                                .get(&user_req)
                                .unwrap()
                                .get_info()
                                .encode();
                        }
                        "SETUSER" => {
                            let user_req = args[2].get_string()?;
                            let password =
                                args[3].get_string()?.strip_prefix('>').unwrap().to_string();
                            let mut userdb = self.users.write().await;
                            let user = userdb.get_mut(&user_req).unwrap();
                            user.add_password(&password);
                            response = RedisValue::String("OK".to_string()).as_simple_string()?;
                        }
                        _ => (),
                    }
                    response
                }
            }
            "AUTH" => {
                if args.len() != 3 {
                    RedisValue::Error(
                        "Err wrong number of arguments for 'AUTH' command".to_string(),
                    )
                    .encode()
                } else {
                    let username = args[1].get_string()?;
                    let password = args[2].get_string()?;
                    let successful_auth = self.authenticate_user(&username, &password).await;

                    if successful_auth {
                        self.current_user = Some(username);
                        RedisValue::String("OK".to_string()).as_simple_string()?
                    } else {
                        RedisValue::Error(
                            "WRONGPASS invalid username-password pair or user is disabled."
                                .to_string(),
                        )
                        .encode()
                    }
                }
            }
            _ => unreachable!("command routed to the wrong handler: {command}"),
        };
        Ok(response)
    }
}
