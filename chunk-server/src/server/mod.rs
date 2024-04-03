use std::future::Future;
use std::sync::Arc;
use tokio::net::TcpListener;

use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Error, Server};
use tracing::info;
use uuid::Uuid;

use common::chunk_server::client_service_server::ClientServiceServer;
use common::chunk_server::master_service_server::MasterServiceServer;

use crate::storage::Storage;

mod client_service;
mod master_service;

#[derive(Debug, Default)]
pub struct ChunkServer {
    address: String,
    storage: Arc<Storage>,
}

impl ChunkServer {
    #[tracing::instrument]
    pub fn new(address: String, storage: Arc<Storage>) -> Self {
        ChunkServer { address, storage }
    }
}

pub fn run(
    chunk_server: ChunkServer,
    listener: TcpListener,
) -> Result<impl Future<Output = Result<(), Error>>, Box<dyn std::error::Error>> {
    let chunk_server = Arc::new(chunk_server);

    let server = Server::builder()
        .trace_fn(|_| {
            let request_id = Uuid::new_v4();
            tracing::info_span!("Request span", %request_id)
        })
        .add_service(ClientServiceServer::from_arc(chunk_server.clone()))
        .add_service(MasterServiceServer::from_arc(chunk_server.clone()))
        .serve_with_incoming(TcpListenerStream::new(listener));

    info!("Chunk server listening on {}", &chunk_server.address);

    Ok(server)
}
