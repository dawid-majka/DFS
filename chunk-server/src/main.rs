use std::time::Duration;

use rand::Rng;

use common::chunk_server::chunk_service_server::{ChunkService, ChunkServiceServer};
use common::chunk_server::{
    RetrieveChunkRequest, RetrieveChunkResponse, StoreChunkRequest, StoreChunkResponse,
};

use common::master_server::master_service_client::MasterServiceClient;
use common::master_server::{HeartbeatRequest, RegisterChunkServerRequest};
use common::shared::ChunkData;
use tokio::time::interval;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[derive(Debug, Default)]
struct MyChunk {}

#[tonic::async_trait]
impl ChunkService for MyChunk {
    async fn store_chunk(
        &self,
        request: Request<StoreChunkRequest>,
    ) -> Result<Response<StoreChunkResponse>, Status> {
        println!("Store chunk request: {:?}", request);

        let response = StoreChunkResponse { success: true };

        Ok(Response::new(response))
    }

    async fn retrieve_chunk(
        &self,
        request: Request<RetrieveChunkRequest>,
    ) -> Result<Response<RetrieveChunkResponse>, Status> {
        println!("Retrieve chunk request: {:?}", request);

        let chunk = Some(ChunkData {
            chunk_id: "1".to_string(),
            data: Vec::new(),
        });

        let response = RetrieveChunkResponse { chunk };

        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:0".parse()?;
    let chunk = MyChunk::default();

    let server = Server::builder()
        .add_service(ChunkServiceServer::new(chunk))
        .serve(addr);

    println!("Chunk server listening on {}", addr);

    let mut client = MasterServiceClient::connect("http://[::1]:50051").await?;

    let server_id = format!("server_{}", rand::thread_rng().gen::<u32>());
    let server_address = format!("http://{}", addr);

    let request = Request::new(RegisterChunkServerRequest {
        server_id: server_id.clone(),
        server_address,
    });

    let response = client.register_chunk_server(request).await?;

    println!("register_chunk_server response={:?}", response);

    let mut interval = interval(Duration::from_secs(5));

    tokio::spawn(async move {
        loop {
            interval.tick().await;

            let request = Request::new(HeartbeatRequest {
                server_id: server_id.clone(),
            });

            match client.send_heartbeat(request).await {
                Ok(response) => println!(
                    "Heartbeat sent. Response: {}",
                    response.into_inner().success
                ),
                Err(e) => println!("Failed to send heartbeat: {}", e),
            }
        }
    });

    server.await?;

    Ok(())
}
