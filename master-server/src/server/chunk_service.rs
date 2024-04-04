use common::master_server::{
    chunk_service_server::ChunkService, HeartbeatRequest, HeartbeatResponse,
};
use tonic::{Request, Response, Status};
use tracing::info;

use super::MasterServer;

#[tonic::async_trait]
impl ChunkService for MasterServer {
    #[tracing::instrument(skip(self))]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        info!(
            "Heartbeat from: {} received",
            request.into_inner().server_address
        );

        let to_delete = Vec::<String>::new();

        Ok(Response::new(HeartbeatResponse { to_delete }))
    }
}
