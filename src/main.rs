use std::{env, sync::Arc};
use anyhow::Result;
use tokio::{net::TcpListener, signal, sync::{RwLock, mpsc::unbounded_channel}, task::JoinSet};

use crate::modules::client_handler::{ClientHandler, DB, Registry};
mod modules;


#[tokio::main]
async fn main() -> Result<()> {
    let mut port = "6379".to_string();
    if let Ok(var_port) = env::var("REDIS_PORT") {
        port = var_port;
    }
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;
    println!("Listening on 127.0.0.1:{}", port);

    let mut handles = JoinSet::new();
    let db = Arc::new(RwLock::new(DB::new()));
    let ps_registry = Arc::new(RwLock::new(Registry::new()));
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
                        handles.spawn(async move {
                            let mut client_handler = ClientHandler::new(current_thread_id, db, ps_registry, receiver);
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
