use std::{sync::Arc};
use anyhow::Result;
use rand::{distr::{Alphanumeric, SampleString}, rng};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, signal, sync::{RwLock, mpsc::unbounded_channel}, task::JoinSet};

use crate::modules::{client_handler::ClientHandler, db::{DB, Registry}, values::RedisValue};
mod modules;

fn generate_random_alphanumeric(length: usize) -> String {
    Alphanumeric.sample_string(&mut rng(), length)
}

struct Replica {
    role: String,
    master_replid: String,
    master_address: String,
}

impl Replica {
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

async fn slave_handshake(rep: &Replica, port: &str) -> Result<()> {
    let mut stream = TcpStream::connect(rep.get_address()).await?;
    let mut buffer = [0; 1024];
    // PING
    stream.write_all(&RedisValue::Array(vec![RedisValue::String("PING".to_string())]).encode()).await?;
    stream.read(&mut buffer).await?;
    // REPLCONF listening-port <port>
    stream.write_all(&RedisValue::array_from_string_vec(vec!["REPLCONF", "listening-port", port]).encode()).await?;
    stream.read(&mut buffer).await?;
    // REPLCONF capa psync2
    stream.write_all(&RedisValue::array_from_string_vec(vec!["REPLCONF", "capa", "psync2"]).encode()).await?;
    stream.read(&mut buffer).await?;
    // PSYNC ? -1
    stream.write_all(&RedisValue::array_from_string_vec(vec!["PSYNC", "?", "-1"]).encode()).await?;
    stream.read(&mut buffer).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let port = match args.iter().skip_while(|a| a != &"--port").skip(1).next() {
        None => "6379",
        Some(port) => port,
    };
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
    let replica = Replica::new(role, &master_id, &master_address);
    if role == "slave" {
        slave_handshake(&replica, port).await?;
    }
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;
    println!("Listening on 127.0.0.1:{}", port);

    let mut handles = JoinSet::new();
    let db = Arc::new(RwLock::new(DB::new()));
    let ps_registry = Arc::new(RwLock::new(Registry::new()));
    let repl_info = Arc::new(RwLock::new(replica));
    let ctrl_c_signal = signal::ctrl_c();
    tokio::pin!(ctrl_c_signal);
    
    let mut current_thread_id = 0u32;
    
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
                        let repl_info = Arc::clone(&repl_info);
                        handles.spawn(async move {
                            let mut client_handler = ClientHandler::new(current_thread_id, db, ps_registry, receiver, repl_info);
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
