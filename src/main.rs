use std::{collections::HashMap, env::current_dir, format, fs, path::PathBuf, println, slice::Iter, sync::Arc};
use anyhow::Result;
use rand::{distr::{Alphanumeric, SampleString}, rng};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, signal, sync::{RwLock, mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel}}, task::JoinSet};
use sha2::{Sha256, Digest};
use crate::modules::{client_handler::ClientHandler, db::{DB, Registry}, file_handler::FileHandler, values::RedisValue};
mod modules;

fn generate_random_alphanumeric(length: usize) -> String {
    Alphanumeric.sample_string(&mut rng(), length)
}

fn hash_password(password: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(password);
    let result = hasher.finalize();
    let password_hash = hex::encode(result);
    password_hash
}

struct User {
    flags: Vec<String>,
    passwords: Vec<String>,
}

impl User {
    pub fn new() -> Self {
        Self { flags: Vec::new(), passwords: Vec::new() }
    }
    pub fn get_info(&self) -> RedisValue {
        let flags = RedisValue::array_from_string_vec(self.flags.iter().map(|s| s.as_str()).collect());
        let passwords = RedisValue::array_from_string_vec(self.passwords.iter().map(|s| s.as_str()).collect());
        RedisValue::Array(vec![RedisValue::String("flags".to_string()), flags, RedisValue::String("passwords".to_string()), passwords])
    }
    pub fn add_flag(&mut self, flag: &str) {
        self.flags.push(flag.to_string());
    }
    pub fn add_password(&mut self, password: &str) {
        let password_hash = hash_password(password);
        self.passwords.push(password_hash);

        for i in 0..self.flags.len() {
            if self.flags[i] == "nopass" {
                self.flags.remove(i);
                break;
            }
        }
    }
    pub fn password_iter(&self) -> Iter<'_, String> {
        self.passwords.iter()
    }
    pub fn flag_iter(&self) -> Iter<'_, String> {
        self.flags.iter()
    }
}

struct Replica {
    instruction_sender: UnboundedSender<Vec<RedisValue>>,
    ack_receiver: UnboundedReceiver<usize>,
}

impl Replica {
    fn new(instruction_sender: UnboundedSender<Vec<RedisValue>>, ack_receiver: UnboundedReceiver<usize>) -> Self {
        Self { instruction_sender, ack_receiver }
    }
    fn send(&self, value: Vec<RedisValue>) -> Result<()> {
        self.instruction_sender.send(value)?;
        Ok(())
    }
    async fn receive(&mut self) -> Option<usize> {
        self.ack_receiver.recv().await
    }
}

struct ReplicaInfo {
    role: String,
    master_replid: String,
    master_address: String,
}

impl ReplicaInfo {
    fn new(role: &str, master_replid: &str, master_address: &str) -> Self {
        Self { role: role.to_string(), master_replid: master_replid.to_string(), master_address: master_address.to_string() }
    }

    pub fn get_role(&self) -> String {
        self.role.clone()
    }

    pub fn get_replid(&self) -> String {
        self.master_replid.clone()
    }

    pub fn get_address(&self) -> String {
        self.master_address.clone()
    }
}

async fn slave_handshake(rep: &Arc<RwLock<ReplicaInfo>>, port: &str, mut client_handler: ClientHandler) -> Result<()> {
    let mut stream = TcpStream::connect(rep.read().await.get_address()).await?;
    let mut buffer = [0; 1024];
    // PING
    stream.write_all(&RedisValue::Array(vec![RedisValue::String("PING".to_string())]).encode()).await?;
    //read +PONG
    stream.read_exact(&mut buffer[0..7]).await?;

    // REPLCONF listening-port <port>
    stream.write_all(&RedisValue::array_from_string_vec(vec!["REPLCONF", "listening-port", port]).encode()).await?;
    //read +OK
    stream.read_exact(&mut buffer[0..5]).await?;

    // REPLCONF capa psync2
    stream.write_all(&RedisValue::array_from_string_vec(vec!["REPLCONF", "capa", "psync2"]).encode()).await?;
    //read +OK
    stream.read_exact(&mut buffer[0..5]).await?;

    // PSYNC ? -1
    stream.write_all(&RedisValue::array_from_string_vec(vec!["PSYNC", "?", "-1"]).encode()).await?;
    //read +FULLRESYNC ...
    stream.read_exact(&mut buffer[0..56]).await?;
    //read $<n> (to get filesize)
    stream.read_exact(&mut buffer[0..5]).await?;
    //read the sync file
    let filesize = usize::from_str_radix(&String::from_utf8(buffer[1..3].to_vec()).unwrap(), 10).unwrap();
    let mut buffer = vec![0u8; filesize];
    stream.read_exact(&mut buffer).await?;

    // call client handler to handle incomming commands
    client_handler.handle_client_async(stream).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    const CONFIG_FLAGS : [&str; 6] = ["dir", "dbfilename", "appendonly", "appenddirname", "appendfilename", "appendfsync"];
    let args = std::env::args().collect::<Vec<_>>();
    let port = match args.iter().skip_while(|a| a != &"--port").skip(1).next() {
        None => "6379",
        Some(port) => port,
    };
    let mut config = HashMap::new();
    for i in 0..args.len() {
        let arg = &args[i];
        if let Some(unprefixed) = arg.strip_prefix("--") && CONFIG_FLAGS.contains(&unprefixed) {
            if i + 1 < args.len() {
                let value = &args[i+1];
                config.insert(unprefixed.to_string(), value.to_string());
            }
        }
    }
    for flag in CONFIG_FLAGS {
        if !config.contains_key(flag) {
            let default = match flag {
                "dir" => current_dir().unwrap_or_default().to_string_lossy().into_owned(),
                "appendonly" => "no".to_string(),
                "appenddirname" => "appendonlydir".to_string(),
                "appendfilename" => "appendonly.aof".to_string(),
                "appendfsync" => "everysec".to_string(),
                _ => "".to_string()
            };
            config.insert(flag.to_string(), default);
        }
    }

    if let Some(appendonly) = config.get("appendonly") && appendonly == "yes" {
        let appendonlydirname = config.get("appenddirname").map_or("appendonlydir", String::as_str);
        let appendonlyfilename = config.get("appendfilename").map_or("appendonly.aof", String::as_str);
        let dir = config.get("dir").map_or(".", String::as_str);

        let complete_dirname = format!("{}/{}", dir, appendonlydirname);
        let complete_filename = format!("{}/{}.1.incr.aof", complete_dirname, appendonlyfilename);
        
        if fs::create_dir(&complete_dirname).is_ok() {
            println!("Created dir: {}", complete_dirname);
        } else {
            println!("Error creating dir: {}", complete_dirname);
        }
        if fs::File::create(&complete_filename).is_ok() {
            println!("Created append only file: {}", complete_filename);
        } else {
            println!("Error creating append only file: {}", complete_filename);
        }

        config.insert("completeappendfilename".to_string(), complete_filename);
    }

    let role;
    let master_address;
    match args.iter().skip_while(|a| a != &"--replicaof").skip(1).next() {
        None => {
            role = "master";
            master_address = "".to_string();
        },
        Some(addr) => {
            role = "slave";
            master_address = addr.replace(' ', ":");
            
        },
    };
    let master_id = generate_random_alphanumeric(40);
    let replica = ReplicaInfo::new(role, &master_id, &master_address);
    let mut default_user = User::new();
    default_user.add_flag("nopass");
    let users = Arc::new(RwLock::new(HashMap::new()));
    users.write().await.insert("default".to_string(), default_user);
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;
    println!("Listening on 127.0.0.1:{}", port);
    
    let mut handles = JoinSet::new();
    let db = Arc::new(RwLock::new(DB::new()));
    let ps_registry = Arc::new(RwLock::new(Registry::new()));
    let replicadb = Arc::new(RwLock::new(Vec::new()));
    let repl_info = Arc::new(RwLock::new(replica));
    let ctrl_c_signal = signal::ctrl_c();
    tokio::pin!(ctrl_c_signal);

    // read file if it was provided
    if let Some(filename) = config.get("dbfilename") && let Some(dirname) = config.get("dir") {
        let path = PathBuf::from(dirname).join(filename);
        let file_handler = FileHandler::new(db.clone());
        if path.is_file() {
            file_handler.read_file(path).await?;
        }
    }
    
    let mut current_thread_id = 0u32;
    if role == "slave" {
        let db = Arc::clone(&db);
        let replicadb = Arc::clone(&replicadb);
        let repl_info = Arc::clone(&repl_info);
        let repl_info2 = Arc::clone(&repl_info);
        let (sender, receiver) = unbounded_channel();
        {
            let mut reg = ps_registry.write().await;
            reg.senders.insert(current_thread_id, sender);
        }
        let ps_registry = Arc::clone(&ps_registry);
        let port = port.to_string();
        let config = config.clone();
        let users = Arc::clone(&users);
        handles.spawn(async move {
            let client_handler = ClientHandler::new(current_thread_id, db, ps_registry, receiver, repl_info, replicadb, true, users, config).await;
            slave_handshake(&repl_info2, &port, client_handler).await.unwrap();
        });
    }
    
    loop {
        tokio::select! {
            biased;
            _ = &mut ctrl_c_signal => {
                println!("\nCtrl+C received! Stopping listener and waiting for clients to finish...");
                break;
            },
            conn = listener.accept() => {
                match conn {
                    Ok((stream, addr)) => {
                        println!("Accepted connection from {}", addr);
                        let db = Arc::clone(&db);
                        let (sender, receiver) = unbounded_channel::<Vec<u8>>();
                        {
                            let mut reg = ps_registry.write().await;
                            reg.senders.insert(current_thread_id, sender);
                        }
                        let ps_registry = Arc::clone(&ps_registry);
                        let replicadb = Arc::clone(&replicadb);
                        let repl_info = Arc::clone(&repl_info);
                        let users = Arc::clone(&users);
                        let config = config.clone();
                        handles.spawn(async move {
                            let mut client_handler = ClientHandler::new(current_thread_id, db, ps_registry, receiver, repl_info, replicadb, false, users, config).await;
                            if let Err(e) = client_handler.handle_client_async(stream).await {
                                eprintln!("Error handling client: {}", e);
                            }
                        });
                        current_thread_id += 1;
                    },
                    Err(e) => {
                        eprintln!("error accepting connection: {}", e);
                    }
                }
            },
            _ = handles.join_next() => {}
        }
    }
    Ok(())
}
