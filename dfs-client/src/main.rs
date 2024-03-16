use common::master_server::master_service_client::MasterServiceClient;
use common::master_server::ListChunkServersRequest;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = MasterServiceClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(ListChunkServersRequest {});

    let response = client.list_chunk_servers(request).await?;

    println!("list_chunk_servers response={:?}", response);

    Ok(())
}
