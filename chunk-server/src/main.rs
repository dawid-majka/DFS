use rand::Rng;

use common::dfs::master_server::{Chunk, ChunkServer};

struct MyChunk {}

impl Chunk for MyChunk {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:0".parse()?;
    let chunk = MyChunk::default();

    let server = Server::builder()
        .add_service(ChunkServer::new(chunk))
        .serve(addr);

    let addr = server.local_addr();

    println!("Chunk server listening on {}", addr);

    let mut client = MasterServiceClient::connect("http://[::1]:50051").await?;

    let server_id = format!("server_{}", rand::thread_rng().gen());
    let server_address = format!("http://{}", addr);

    let request = tonic::Request::new(RegisterChunkServerRequest {
        server_id,
        server_address,
    });

    let response = client.register_chunk_server(request).await?;

    println!("register_chunk_server response={:?}", response);

    server.await?;

    Ok(())
}
