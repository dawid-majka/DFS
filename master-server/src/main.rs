use std::collections::HashMap;
use std::sync::Arc;

use config::get_configuration;
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};

use common::master_server::chunk_service_server::{ChunkService, ChunkServiceServer};
use common::master_server::client_service_server::{ClientService, ClientServiceServer};
use common::master_server::{
    ChunkLocation, ChunkMetadata, DownloadFileRequest, DownloadFileResponse, HeartbeatRequest,
    HeartbeatResponse, UploadFileRequest, UploadFileResponse,
};

mod config;

#[derive(Debug, Default)]
pub struct MasterServer {
    // stores filename to chunk handles list mapping
    chunk_handles: Mutex<HashMap<String, Vec<String>>>,
    // stores chunk handles locations on chunk servers,
    locations: Mutex<HashMap<String, Vec<String>>>,
    // storest chunk server ids to addressess mappings
    chunk_servers: Mutex<HashMap<String, String>>,
}

#[tonic::async_trait]
impl ClientService for MasterServer {
    async fn upload_file(
        &self,
        request: Request<UploadFileRequest>,
    ) -> Result<Response<UploadFileResponse>, Status> {
        let client_address = request
            .remote_addr()
            .expect("Method should provide client address");

        println!("Upload file request from: {:?} received", client_address);

        let request = request.into_inner();

        let chunk_location = ChunkLocation {
            address: self
                .chunk_servers
                .lock()
                .await
                .values()
                .next()
                .cloned()
                .unwrap(),
        };

        let chunk_locations = vec![chunk_location];

        let response = Response::new(UploadFileResponse {
            chunk_id: request.chunk_id.parse().expect("Should parse chunk_id"),
            chunk_locations,
        });

        Ok(response)
    }

    async fn download_file(
        &self,
        request: Request<DownloadFileRequest>,
    ) -> Result<Response<DownloadFileResponse>, Status> {
        let client_address = request
            .remote_addr()
            .expect("Method should provide client address");

        println!("Download file request from: {:?} received", client_address);

        let file_name = request.into_inner().file_name;

        // TODO: Get from map

        let address = self
            .chunk_servers
            .lock()
            .await
            .values()
            .next()
            .cloned()
            .unwrap();

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

#[tonic::async_trait]
impl ChunkService for MasterServer {
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        println!(
            "Heartbeat from: {} received",
            request.into_inner().server_address
        );

        let to_delete = Vec::<String>::new();

        Ok(Response::new(HeartbeatResponse { to_delete }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let configuration = get_configuration().expect("Failed to read conifguration");

    let addr = format!("{}:{}", configuration.host, configuration.port).parse()?;
    let master = MasterServer::default();

    let master = Arc::new(master);

    Server::builder()
        .add_service(ChunkServiceServer::from_arc(master.clone()))
        .add_service(ClientServiceServer::from_arc(master))
        .serve(addr)
        .await?;

    Ok(())
}
