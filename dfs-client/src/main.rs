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

    pub async fn mkdir(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut master_client = ClientServiceClient::connect(self.master_address.clone())
            .await
            .expect("Client should connect to master server");

        let mkdir_request = Request::new(MkdirRequest {
            path: path.to_owned(),
        });

        let mkdir_response = master_client
            .mkdir(mkdir_request)
            .await
            .expect("Master server should return empty response");

        Ok(())
    }

    pub async fn ls(&self, path: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut master_client = ClientServiceClient::connect(self.master_address.clone())
            .await
            .expect("Client should connect to master server");

        let ls_request = Request::new(LsRequest {
            path: path.to_owned(),
        });

        let ls_response = master_client
            .ls(ls_request)
            .await
            .expect("Master server should return list of nodes in directory");

        Ok(ls_response.into_inner().content)
    }

    pub async fn create_file(&self, file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut master_client = ClientServiceClient::connect(self.master_address.clone())
            .await
            .expect("Client should connect to master server");

        let create_file_request = Request::new(CreateFileRequest {
            file_path: file_path.to_owned(),
        });

        let create_file_response = master_client
            .create_file(create_file_request)
            .await
            .expect("Master server should return empty response");

        Ok(())
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

    client.mkdir("/path/to/new/directory").await.unwrap();
    client.create_file("/path/to/new/file").await.unwrap();

    let content1 = client.ls("/path").await.unwrap();
    let content2 = client.ls("/path/to").await.unwrap();
    let content3 = client.ls("/path/to/new").await.unwrap();
    let content4 = client.ls("/path/to/new/directory").await.unwrap();

    println!("{:?}", content1);
    println!("{:?}", content2);
    println!("{:?}", content3);
    println!("{:?}", content4);

    Ok(())
}

fn split_into_chunks(chunk_size: usize, data: Bytes) -> Vec<Bytes> {
    data.chunks(chunk_size)
        .map(|chunk| Bytes::copy_from_slice(chunk))
        .collect()
}
