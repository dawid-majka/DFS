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
    #[tracing::instrument(skip(self))]
    async fn open_file(
        &self,
        request: Request<OpenFileRequest>,
    ) -> Result<Response<OpenFileResponse>, Status> {
        let response = Response::new(OpenFileResponse {
            chunks_metadata: Vec::new(),
        });

        Ok(response)
    }

    #[tracing::instrument(skip(self))]
    async fn close_file(
        &self,
        request: Request<CloseFileRequest>,
    ) -> Result<Response<EmptyReply>, Status> {
        let response = Response::new(EmptyReply {});

        Ok(response)
    }

    #[tracing::instrument(skip(self))]
    async fn create_file(
        &self,
        request: Request<CreateFileRequest>,
    ) -> Result<Response<EmptyReply>, Status> {
        let client_address = request
            .remote_addr()
            .expect("Method should provide client address");

        info!("Create file request from: {:?} received", client_address);

        let file_path = request.into_inner().file_path;

        self.metadata.create_file(file_path);

        let response = Response::new(EmptyReply {});

        Ok(response)
    }

    #[tracing::instrument(skip(self))]
    async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> Result<Response<EmptyReply>, Status> {
        // Only marks file to delete and hides it
        // Final delete is during GC scan (interval set in config)
        // GC deletes metadata and sends it in to_delete list (heartbeat) to chunk_server

        let client_address = request
            .remote_addr()
            .expect("Method should provide client address");

        info!("Delete file request from: {:?} received", client_address);

        let file_path = request.into_inner().file_path;

        self.metadata.delete_file(file_path);

        let response = Response::new(EmptyReply {});

        Ok(response)
    }

    #[tracing::instrument(skip(self))]
    async fn allocate_chunk(
        &self,
        request: Request<AllocateChunkRequest>,
    ) -> Result<Response<AllocateChunkResponse>, Status> {
        let client_address = request
            .remote_addr()
            .expect("Method should provide client address");

        info!("Allocate chunk request from: {:?} received", client_address);

        let file_path = request.into_inner().file_path;
       
        let chunk_id = 1;

        let chunk_metadata = Some(self.metadata.allocate_chunk(&file_path, chunk_id));

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
