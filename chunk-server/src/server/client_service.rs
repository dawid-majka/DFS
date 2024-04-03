use common::{
    chunk_server::{
        client_service_server::ClientService, RetrieveChunkRequest, RetrieveChunkResponse,
        StoreChunkRequest, StoreChunkResponse,
    },
    shared::ChunkData,
};
use tonic::{Request, Response, Status};
use tracing::info;

use super::ChunkServer;

#[tonic::async_trait]
impl ClientService for ChunkServer {
    #[tracing::instrument(skip(self))]
    async fn store_chunk(
        &self,
        request: Request<StoreChunkRequest>,
    ) -> Result<Response<StoreChunkResponse>, Status> {
        info!("Store chunk request: {:?}", request);

        let response = StoreChunkResponse { success: true };

        Ok(Response::new(response))
    }

    #[tracing::instrument(skip(self))]
    async fn retrieve_chunk(
        &self,
        request: Request<RetrieveChunkRequest>,
    ) -> Result<Response<RetrieveChunkResponse>, Status> {
        info!("Retrieve chunk request: {:?}", request);

        let chunk = Some(ChunkData {
            chunk_handle: "1".to_string(),
            data: Vec::new(),
        });

        let response = RetrieveChunkResponse { chunk };

        Ok(Response::new(response))
    }
}
