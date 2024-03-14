use tonic::{transport::Server, Request, Response, Status};

use dfs::master_server::{Master, MasterServer};
use dfs::{ChunkServerAddress, PingRequest, PingResponse};

pub mod dfs {
    tonic::include_proto!("dfs"); // The string specified here must match the proto package name
}

#[derive(Debug, Default)]
pub struct MyMaster {}

#[tonic::async_trait]
impl Master for MyMaster {
    async fn get_chunk_server_address(
        &self,
        request: Request<PingRequest>, // Accept request of type PingRequest
    ) -> Result<Response<ChunkServerAddress>, Status> {
        // Return an instance of type PingReply
        println!("Got a request: {:?}", request);

        let reply = dfs::ChunkServerAddress {
            address: format!("Hello {}!", request.into_inner().message), // We must use .into_inner() as the fields of gRPC requests and responses are private
        };

        Ok(Response::new(reply)) // Send back our formatted greeting
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
