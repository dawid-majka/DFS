use std::fs::File;
use std::io::Read;

use common::chunk_server::chunk_service_client::ChunkServiceClient;
use common::chunk_server::StoreChunkRequest;

use common::master_server::master_service_client::MasterServiceClient;
use common::master_server::UploadFileRequest;
use common::shared::ChunkData;
use tonic::Request;

struct Client {
    master_address: String,
}

impl Client {
    fn new(master_address: &str) -> Self {
        Client {
            master_address: master_address.to_string(),
        }
    }

    pub async fn upload_file(&self, filename: &str, file: Vec<u8>) {
        let mut master_client = MasterServiceClient::connect(self.master_address.clone())
            .await
            .expect("Client should connect to master server");

        let request = Request::new(UploadFileRequest {
            file_name: filename.to_string(),
        });

        let upload_file_response = master_client
            .upload_file(request)
            .await
            .expect("Master server should return response with chunk locations");

        println!("Uplad file response: {:?}", upload_file_response);

        if let Some(chunk_location) = upload_file_response.into_inner().chunk_location {
            let address = format!("http://{}", chunk_location.address.clone());
            let chunk_id = chunk_location.chunk_id.to_string();

            println!("Connecting to chunk server with address: {:?}", address);

            let mut chunk_client = ChunkServiceClient::connect(address)
                .await
                .expect("Client should connect to chunk server");

            let request = Request::new(StoreChunkRequest {
                chunk: Some(ChunkData {
                    chunk_id,
                    data: file,
                }),
            });

            let write_chunk_response = chunk_client
                .store_chunk(request)
                .await
                .expect("Chunk server should return response with status");

            println!("Write chunk response: {:?}", write_chunk_response);
        }
    }

    pub async fn get_file(&self, filename: &str) -> Vec<u8> {
        todo!();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new("http://[::1]:50051");

    let filename = "README.md";
    let mut file = File::open(filename).expect("File should be opened.");
    let mut buffer = Vec::new();

    file.read_to_end(&mut buffer)?;

    println!("{:?}", buffer);

    client.upload_file(filename, buffer).await;

    // TODO: read file
    let retrieved_file = client.get_file(filename).await;

    println!("{:?}", retrieved_file);

    // let mut client = MasterServiceClient::connect("http://[::1]:50051").await?;

    // let request = tonic::Request::new(ListChunkServersRequest {});

    // let response = client.list_chunk_servers(request).await?;

    // println!("list_chunk_servers response={:?}", response);

    Ok(())
}
