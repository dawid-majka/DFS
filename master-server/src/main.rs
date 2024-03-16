use std::collections::HashMap;

use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};

use common::master_server::master_service_server::{MasterService, MasterServiceServer};
use common::master_server::{
    ListChunkServersRequest, ListChunkServersResponse, RegisterChunkServerRequest,
    RegisterChunkServerResponse,
};

#[derive(Debug, Default)]
pub struct MyMaster {
    chunk_servers: Mutex<HashMap<String, String>>,
}

#[tonic::async_trait]
impl MasterService for MyMaster {
    async fn register_chunk_server(
        &self,
        request: Request<RegisterChunkServerRequest>,
    ) -> Result<Response<RegisterChunkServerResponse>, Status> {
        let addr = request.remote_addr();

        println!("Request from: {:?}", addr);
        let request = request.into_inner();
        let server_id = request.server_id;
        let server_address = request.server_address;
        println!(
            "Registering server: {} with address: {}",
            server_id, server_address
        );

        let mut servers = self.chunk_servers.lock().await;
        servers.insert(server_id, server_address);

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
