use std::collections::HashMap;

use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};

use common::master_server::master_service_server::{MasterService, MasterServiceServer};
use common::master_server::{
    ChunkLocation, HeartbeatRequest, HeartbeatResponse, ListChunkServersRequest,
    ListChunkServersResponse, RegisterChunkServerRequest, RegisterChunkServerResponse,
    UploadFileRequest, UploadFileResponse,
};

#[derive(Debug, Default)]
pub struct MyMaster {
    // stores filename to chunk handles list mapping
    chunk_handles: Mutex<HashMap<String, Vec<String>>>,
    // stores chunk handles locations on chunk servers,
    locations: Mutex<HashMap<String, Vec<String>>>,
    // storest chunk server ids to addressess mappings
    chunk_servers: Mutex<HashMap<String, String>>,
}

#[tonic::async_trait]
impl MasterService for MyMaster {
    async fn register_chunk_server(
        &self,
        request: Request<RegisterChunkServerRequest>,
    ) -> Result<Response<RegisterChunkServerResponse>, Status> {
        let server_address = request
            .remote_addr()
            .expect("Method should provide chunk server address");

        println!("Request from: {:?}", server_address);
        let request = request.into_inner();
        let server_id = request.server_id;

        println!(
            "Registering server: {} with address: {}",
            server_id, server_address
        );

        let mut servers = self.chunk_servers.lock().await;
        servers.insert(server_id, server_address.to_string());

        Ok(Response::new(RegisterChunkServerResponse { success: true }))
    }

    async fn list_chunk_servers(
        &self,
        _request: Request<ListChunkServersRequest>,
    ) -> Result<Response<ListChunkServersResponse>, Status> {
        let servers = self.chunk_servers.lock().await.values().cloned().collect();

        Ok(Response::new(ListChunkServersResponse {
            server_address: servers,
        }))
    }

    async fn send_heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        println!(
            "Heartbeat from: {} received",
            request.into_inner().server_id
        );

        Ok(Response::new(HeartbeatResponse { success: true }))
    }

    async fn upload_file(
        &self,
        request: Request<UploadFileRequest>,
    ) -> Result<Response<UploadFileResponse>, Status> {
        let client_address = request
            .remote_addr()
            .expect("Method should provide client address");

        println!("Upload file request from: {:?} received", client_address);

        let chunk_location = Some(ChunkLocation {
            chunk_id: 1,
            address: self
                .chunk_servers
                .lock()
                .await
                .values()
                .next()
                .cloned()
                .unwrap(),
        });

        Ok(Response::new(UploadFileResponse { chunk_location }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let master = MyMaster::default();

    Server::builder()
        .add_service(MasterServiceServer::new(master))
        .serve(addr)
        .await?;

    Ok(())
}
