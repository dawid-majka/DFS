use std::{future::Future, sync::Arc};

use common::{
    master_server::chunk_service_server::ChunkServiceServer,
    master_server::client_service_server::ClientServiceServer,
};
use tonic::transport::{Error, Server};
use tracing::info;
use uuid::Uuid;

use crate::medatada::Metadata;

mod chunk_service;
mod client_service;

#[derive(Debug)]
pub struct MasterServer {
    metadata: Arc<Metadata>,
}

impl MasterServer {
    #[tracing::instrument]
    pub fn new(metadata: Arc<Metadata>) -> Self {
        MasterServer { metadata }
    }
}

pub fn run(
    master_server: MasterServer,
    address: String,
) -> Result<impl Future<Output = Result<(), Error>>, Box<dyn std::error::Error>> {
    tracing::info!(message = "Starting server.", %address);

    let addr = address.parse()?;

    let master_server = Arc::new(master_server);

    let server = Server::builder()
        .trace_fn(|_| {
            let request_id = Uuid::new_v4();
            tracing::info_span!("Request span", %request_id)
        })
        .add_service(ClientServiceServer::from_arc(master_server.clone()))
        .add_service(ChunkServiceServer::from_arc(master_server.clone()))
        .serve(addr);

    info!("Master server listening on {}", address);

    Ok(server)
}
