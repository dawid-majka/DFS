use common::{
    master_server::{
        client_service_server::ClientService, AllocateChunkRequest, AllocateChunkResponse,
        ChunkMetadata, CloseFileRequest, CreateFileRequest, DeleteFileRequest, LsRequest,
        LsResponse, MkdirRequest, OpenFileRequest, OpenFileResponse,
    },
    shared::EmptyReply,
};
use tonic::{Request, Response, Status};
use tracing::info;

use super::MasterServer;

#[tonic::async_trait]
impl ClientService for MasterServer {
    async fn open_file(
        &self,
        request: Request<OpenFileRequest>,
    ) -> Result<Response<OpenFileResponse>, Status> {
        let response = Response::new(OpenFileResponse {
            chunks_metadata: Vec::new(),
        });

        Ok(response)
    }

    async fn close_file(
        &self,
        request: Request<CloseFileRequest>,
    ) -> Result<Response<EmptyReply>, Status> {
        let response = Response::new(EmptyReply {});

        Ok(response)
    }

    async fn create_file(
        &self,
        request: Request<CreateFileRequest>,
    ) -> Result<Response<EmptyReply>, Status> {
        let response = Response::new(EmptyReply {});

        Ok(response)
    }

    async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> Result<Response<EmptyReply>, Status> {
        let response = Response::new(EmptyReply {});

        Ok(response)
    }

    async fn allocate_chunk(
        &self,
        request: Request<AllocateChunkRequest>,
    ) -> Result<Response<AllocateChunkResponse>, Status> {
        let chunk_metadata = Some(ChunkMetadata {
            chunk_handle: "1234".to_owned(),
            locations: Vec::new(),
        });

        let response = Response::new(AllocateChunkResponse { chunk_metadata });

        Ok(response)
    }

    #[tracing::instrument(skip(self))]
    async fn mkdir(&self, request: Request<MkdirRequest>) -> Result<Response<EmptyReply>, Status> {
        let client_address = request
            .remote_addr()
            .expect("Method should provide client address");

        info!("Mkdir request from: {:?} received", client_address);

        let path = request.into_inner().path;

        self.metadata.mkdir(&path);

        let response = Response::new(EmptyReply {});

        Ok(response)
    }

    #[tracing::instrument(skip(self))]
    async fn ls(&self, request: Request<LsRequest>) -> Result<Response<LsResponse>, Status> {
        let client_address = request
            .remote_addr()
            .expect("Method should provide client address");

        info!("Ls request from: {:?} received", client_address);

        let path = request.into_inner().path;

        let content = self
            .metadata
            .ls(&path)
            .into_iter()
            .map(|elem| elem.to_owned())
            .collect();

        let response = Response::new(LsResponse { content });

        Ok(response)
    }
}
