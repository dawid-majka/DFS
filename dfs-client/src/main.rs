use std::fs::File;
use std::io::Read;

use bytes::{Bytes, BytesMut};
use common::chunk_server::chunk_service_client::ChunkServiceClient;
use common::chunk_server::{RetrieveChunkRequest, StoreChunkRequest};

use common::master_server::master_service_client::MasterServiceClient;
use common::master_server::{DownloadFileRequest, UploadFileRequest};
use common::shared::ChunkData;
use tonic::Request;

use crate::config::get_configuration;

mod config;

struct Client {
    master_address: String,
}

impl Client {
    fn new(master_address: &str) -> Self {
        Client {
            master_address: master_address.to_string(),
        }
    }

    pub async fn upload_file(
        &self,
        file_name: &str,
        data: Bytes,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: Config
        let chunk_size = 64 * 1024 * 1024;

        let chunks = split_into_chunks(chunk_size, data);

        let mut master_client = MasterServiceClient::connect(self.master_address.clone())
            .await
            .expect("Client should connect to master server");

        for (id, chunk) in chunks.iter().enumerate() {
            let request = Request::new(UploadFileRequest {
                file_name: file_name.to_string(),
                chunk_id: id.to_string(),
            });

            let upload_file_response = master_client
                .upload_file(request)
                .await
                .expect("Master server should return response with chunk location");

            println!("Uplad file response: {:?}", upload_file_response);

            let response = upload_file_response.into_inner();

            for chunk_location in response.chunk_locations {
                let address = chunk_location.address.clone();
                // let address = format!("http://{}", chunk_location.address.clone());
                let chunk_id = response.chunk_id.to_string();

                println!("Connecting to chunk server with address: {:?}", address);

                let mut chunk_client = ChunkServiceClient::connect(address)
                    .await
                    .expect("Client should connect to chunk server");

                let request = Request::new(StoreChunkRequest {
                    chunk: Some(ChunkData {
                        chunk_id,
                        data: chunk.to_vec(),
                    }),
                });

                let write_chunk_response = chunk_client
                    .store_chunk(request)
                    .await
                    .expect("Chunk server should return response with status");

                println!("Write chunk response: {:?}", write_chunk_response);
            }
        }

        Ok(())
    }

    pub async fn get_file(&self, file_name: &str) -> Result<Bytes, Box<dyn std::error::Error>> {
        let mut master_client = MasterServiceClient::connect(self.master_address.clone())
            .await
            .expect("Client should connect to master server");

        let download_file_request = Request::new(DownloadFileRequest {
            file_name: file_name.to_string(),
        });

        let download_file_response = master_client
            .download_file(download_file_request)
            .await
            .expect("Master server should return data with chunks locations");

        let chunks = download_file_response.into_inner().chunks;

        let mut file_data = BytesMut::new();

        for chunk_data in chunks {
            let address = chunk_data.chunk_id.to_string();

            let mut chunk_client = ChunkServiceClient::connect(address)
                .await
                .expect("Client should connect to chunk server");

            let request = Request::new(RetrieveChunkRequest {
                chunk_id: chunk_data.chunk_id.to_string(),
            });

            let response = chunk_client
                .retrieve_chunk(request)
                .await
                .expect("Chunk server should return chunk data");

            let chunk_data = response.into_inner().chunk.unwrap().data;

            file_data.extend_from_slice(&chunk_data);
        }

        let file_data = file_data.freeze();

        Ok(file_data)
    }

    pub async fn delete_file(&self, file_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = get_configuration().expect("Failed to read configuration");
    let address = format!("http://{}:{}", config.master_host, config.master_port);

    let client = Client::new(&address);

    let file_name = "README.md";

    let mut file = File::open(file_name).expect("File should be opened.");
    let mut buffer = Vec::new();

    file.read_to_end(&mut buffer)?;
    println!("{:?}", buffer);

    let data = Bytes::from(buffer);

    client.upload_file(file_name, data).await;

    // TODO: read file
    let retrieved_file = client.get_file(file_name).await;

    println!("{:?}", retrieved_file);

    // let mut client = MasterServiceClient::connect("http://[::1]:50051").await?;

    // let request = tonic::Request::new(ListChunkServersRequest {});

    // let response = client.list_chunk_servers(request).await?;

    // println!("list_chunk_servers response={:?}", response);

    Ok(())
}

fn split_into_chunks(chunk_size: usize, data: Bytes) -> Vec<Bytes> {
    data.chunks(chunk_size)
        .map(|chunk| Bytes::copy_from_slice(chunk))
        .collect()
}
