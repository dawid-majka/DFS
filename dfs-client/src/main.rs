use common::dfs::master_server::ListChunkServers;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = MasterClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(ListChunkServers {});

    let response = client.list_chunk_servers(request).await?;

    println!("list_chunk_servers response={:?}", response);

    Ok(())
}
