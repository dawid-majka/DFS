use std::{sync::Arc, time::Duration};

use common::master_server::{chunk_service_client::ChunkServiceClient, HeartbeatRequest};
use tokio::time::{interval, Interval};
use tonic::Request;
use tracing::{error, info};

use crate::storage::Storage;

pub struct Client {
    server_address: String,
    master_address: String,
    interval: u64,
    storage: Arc<Storage>,
}

impl Client {
    pub fn new(
        server_address: String,
        master_host: String,
        master_port: u16,
        interval: u64,
        storage: Arc<Storage>,
    ) -> Client {
        let master_address = format!("http://{}:{}", master_host, master_port);

        Client {
            server_address,
            master_address,
            interval,
            storage,
        }
    }

    pub fn run(&self) {
        let master_address = self.master_address.clone();
        let server_address = self.server_address.clone();
        let storage = self.storage.clone();
        let mut interval = interval(Duration::from_secs(self.interval));

        tokio::spawn(async move {
            loop {
                interval.tick().await;
                info!(
                    "Sending heartbeat message to master-server on: {}",
                    master_address.clone()
                );

                let used = storage.get_used_storage();
                let available = storage.get_available_storage();
                let chunk_handles = storage.get_chunk_handles();
                let server_address = server_address.clone();

                let request = Request::new(HeartbeatRequest {
                    server_address,
                    used,
                    available,
                    chunk_handles,
                });

                // TODO: try_connect()
                let mut client = ChunkServiceClient::connect(master_address.clone())
                    .await
                    .expect("Client should connect with master server.");

                match client.heartbeat(request).await {
                    Ok(response) => info!(
                        "Heartbeat sent. Resonse.to_delete len: {}",
                        response.into_inner().to_delete.len()
                    ),
                    Err(e) => error!("Failed to send heartbeat: {}", e),
                }
            }
        });
    }
}
