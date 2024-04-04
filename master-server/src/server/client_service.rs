use common::master_server::{
    client_service_server::ClientService, ChunkLocation, ChunkMetadata, DownloadFileRequest,
    DownloadFileResponse, UploadFileRequest, UploadFileResponse,
};
use tonic::{Request, Response, Status};
use tracing::info;

use super::MasterServer;

#[tonic::async_trait]
impl ClientService for MasterServer {
    #[tracing::instrument(skip(self))]
    async fn upload_file(
        &self,
        request: Request<UploadFileRequest>,
    ) -> Result<Response<UploadFileResponse>, Status> {
        let client_address = request
            .remote_addr()
            .expect("Method should provide client address");

        let chunk_handle = "123";

        info!("Upload file request from: {:?} received", client_address);

        let request = request.into_inner();

        let chunk_location = ChunkLocation {
            address: self
                .metadata
                .locations
                .lock()
                .unwrap()
                .get(chunk_handle)
                .unwrap()
                .first()
                .unwrap()
                .to_string(),
        };

        let chunk_locations = vec![chunk_location];

        let response = Response::new(UploadFileResponse {
            chunk_id: request.chunk_id.parse().expect("Should parse chunk_id"),
            chunk_locations,
        });

        Ok(response)
    }

    #[tracing::instrument(skip(self))]
    async fn download_file(
        &self,
        request: Request<DownloadFileRequest>,
    ) -> Result<Response<DownloadFileResponse>, Status> {
        let client_address = request
            .remote_addr()
            .expect("Method should provide client address");

        info!("Download file request from: {:?} received", client_address);

        let file_name = request.into_inner().file_name;

        let chunk_handle = "123";

        let address = self
            .metadata
            .locations
            .lock()
            .unwrap()
            .get(chunk_handle)
            .unwrap()
            .first()
            .unwrap()
            .to_string();

        let location = ChunkLocation { address };

        let locations = vec![location];

        let chunk_data = ChunkMetadata {
            chunk_id: 1,
            locations,
        };

        let chunks = vec![chunk_data];

        let response = Response::new(DownloadFileResponse { chunks });

        Ok(response)
    }
}
