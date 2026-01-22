use std::{sync::Arc};
use anyhow::Result;
use rand::{distr::{Alphanumeric, SampleString}, rng};
use tokio::{net::TcpListener, signal, sync::{RwLock, mpsc::unbounded_channel}, task::JoinSet};

use crate::modules::{client_handler::ClientHandler, db::{DB, Registry}};
mod modules;

fn generate_random_alphanumeric(length: usize) -> String {
    Alphanumeric.sample_string(&mut rng(), length)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let port = match args.iter().skip_while(|a| a != &"--port").skip(1).next() {
        None => "6379",
        Some(port) => port,
    };
    let role = match args.iter().skip_while(|a| a != &"--replicaof").skip(1).next() {
        None => "master",
        Some(_) => "slave",
    };
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;
    let master_id = generate_random_alphanumeric(40);
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
                        let master_id = master_id.clone();
                        handles.spawn(async move {
                            let mut client_handler = ClientHandler::new(current_thread_id, db, ps_registry, receiver, role, &master_id);
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
