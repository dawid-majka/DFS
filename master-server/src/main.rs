use std::collections::HashMap;

use tokio::sync::{Arc, Mutex};
use tonic::{transport::Server, Request, Response, Status};

use common::dfs::master_server::{Master, MasterServer};

#[derive(Debug, Default)]
pub struct MyMaster {
    chunk_servers: Mutex<HashMap<String, String>>,
}

#[tonic::async_trait]
impl Master for MyMaster {
    async fn register_chunk_server(
        &self,
        request: Request<RegisterChunkServerRequest>,
    ) -> Result<Response<RegisterChunkServerResponse>, Status> {
        let request = request.into_inner();
        let server_id = request.server_id;
        let server_address = request.server_address;
        println!(
            "Registering server: {} with address: {}",
            server_id, server_address
        );

        let mut servers = self.chunk_servers.lock().unwrap();
        servers.insert(server_id, server_address);

        Ok(Response::new(RegisterChunkServerResponse { success: true }))
    }

    async fn list_chunk_servers(
        &self,
        _request: Request<ListChunkServersRequest>,
    ) -> Result<Response<ListChunkServersResponse>, Status> {
        let servers = self.chunk_servers.lock().unwrap().values();

        Ok(Response::new(ListChunkServersResponse {
            server_address: servers.clone(),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let master = MyMaster::default();

    Server::builder()
        .add_service(MasterServer::new(master))
        .serve(addr)
        .await?;

    Ok(())
}
