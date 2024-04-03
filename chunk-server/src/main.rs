use client::Client;
use config::get_configuration;
use server::run;
use server::ChunkServer;
use storage::Storage;

use std::sync::Arc;
use tokio::net::TcpListener;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

mod client;
mod config;
mod server;
mod storage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    FmtSubscriber::builder().with_env_filter(filter).init();

    let configuration = get_configuration().expect("Failed to read conifguration");

    let storage = Arc::new(Storage::new("/chunk-server/data"));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let addr = listener.local_addr().unwrap();

    let chunk_server = ChunkServer::new(addr.to_string(), storage.clone());

    let server = run(chunk_server, listener)?;

    let client = Client::new(
        addr.to_string(),
        configuration.master_host,
        configuration.master_port,
        60,
        storage.clone(),
    );

    client.run();

    server.await?;

    Ok(())
}
