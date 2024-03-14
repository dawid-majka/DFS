use dfs::master_client::MasterClient;
use dfs::PingRequest;

pub mod dfs {
    tonic::include_proto!("dfs");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = MasterClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(PingRequest {
        message: "Tonic".into(),
    });

    let response = client.get_chunk_server_address(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
