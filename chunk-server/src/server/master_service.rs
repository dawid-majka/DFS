use common::{
    chunk_server::{
        master_service_server::MasterService, AcquireChunksRequest, GrantLeaseRequest,
        GrantLeaseResponse,
    },
    shared::EmptyReply,
};
use tonic::{Request, Response, Status};

use super::ChunkServer;

#[tonic::async_trait]
impl MasterService for ChunkServer {
    #[tracing::instrument(skip(self))]
    async fn grant_lease(
        &self,
        request: Request<GrantLeaseRequest>,
    ) -> Result<Response<GrantLeaseResponse>, Status> {
        todo!()
    }

    #[tracing::instrument(skip(self))]
    async fn acquire_chunks(
        &self,
        request: Request<AcquireChunksRequest>,
    ) -> Result<Response<EmptyReply>, Status> {
        todo!()
    }
}
