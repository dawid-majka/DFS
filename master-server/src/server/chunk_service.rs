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
        let heartbeat_request = request.into_inner();

        info!(
            "Heartbeat from: {} received",
            heartbeat_request.server_address
        );

        // TODO: Should Updata Metadata.chunk_severs list with new data,
        let to_delete = self.metadata.heartbeat_update(heartbeat_request);

        Ok(Response::new(HeartbeatResponse { to_delete }))
    }
}
