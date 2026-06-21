use std::{collections::HashMap, path::PathBuf, sync::Arc};
use anyhow::Result;
use rand::{distr::{Alphanumeric, SampleString}, rng};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, signal, sync::{RwLock, mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel}}, task::JoinSet};

use crate::modules::{client_handler::ClientHandler, db::{DB, Registry}, file_handler::FileHandler, values::RedisValue};
mod modules;

fn generate_random_alphanumeric(length: usize) -> String {
    Alphanumeric.sample_string(&mut rng(), length)
}

struct User {
    name: String,
    flags: Vec<String>,
    passwords: Vec<String>,
}

impl User {
    pub fn new(name: &str) -> Self {
        Self { name: name.to_string(), flags: Vec::new(), passwords: Vec::new() }
    }
    pub fn get_info(&self) -> RedisValue {
        let flags = RedisValue::array_from_string_vec(self.flags.iter().map(|s| s.as_str()).collect());
        let passwords = RedisValue::array_from_string_vec(self.passwords.iter().map(|s| s.as_str()).collect());
        RedisValue::Array(vec![RedisValue::String("flags".to_string()), flags, RedisValue::String("passwords".to_string()), passwords])
    }
    pub fn add_flag(&mut self, flag: &str) {
        self.flags.push(flag.to_string());
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
    let args = std::env::args().collect::<Vec<_>>();
    let port = match args.iter().skip_while(|a| a != &"--port").skip(1).next() {
        None => "6379",
        Some(port) => port,
    };
    let dir = args.iter().skip_while(|a| a != &"--dir").skip(1).next().cloned();
    let dbfilename = args.iter().skip_while(|a| a != &"--dbfilename").skip(1).next().cloned();
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
    let mut default_user = User::new("default");
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
    if let Some(filename) = &dbfilename {
        let dirname = dir.as_deref().unwrap_or(".");
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
        let dir = dir.clone();
        let dbfilename = dbfilename.clone();
        let users = Arc::clone(&users);
        handles.spawn(async move {
            let client_handler = ClientHandler::new(current_thread_id, db, ps_registry, receiver, repl_info, replicadb, true, dir, dbfilename, users);
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
                        let dir = dir.clone();
                        let dbfilename = dbfilename.clone();
                        handles.spawn(async move {
                            let mut client_handler = ClientHandler::new(current_thread_id, db, ps_registry, receiver, repl_info, replicadb, false, dir, dbfilename, users);
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
