use bytes::Bytes;
use tonic::Request;

use common::master_server::client_service_client::ClientServiceClient;
use common::master_server::{CreateFileRequest, LsRequest, MkdirRequest};

use crate::config::get_configuration;

mod config;

// enum Mode {
//     Read,
//     Write,
// }

// // For now, but should be taken from open file repsonse
// struct FileHandle {
//     path: String,
//     chunks: Option<HashMap<String, String>>,
// }

struct Client {
    master_address: String,
}

impl Client {
    fn new(master_address: &str) -> Self {
        Client {
            master_address: master_address.to_string(),
        }
    }

    // // File Handle:
    // // Mode:Read -> Status and List of chunks with locations
    // // Mode:Write -> Status
    // async fn open(&self, path: &str, mode: Mode) -> Result<FileHandle, Box<dyn std::error::Error>> {
    //     let request = Request::new(OpenFileRequest { path, mode });

    //     let master_client = MasterServiceClient::connect(self.master_address)
    //         .await
    //         .expect("Client should connect to master server");

    //     // TODO: Update proto
    //     let response = master_client
    //         .open_file(request)
    //         .await
    //         .expect("Master should return file handle")
    //         .into_inner();

    //     let chunks = match mode {
    //         Mode::Read => Some(response.chunks),
    //         Mode::Write => None,
    //     };

    //     let handle = FileHandle {
    //         path: path.to_string(),
    //         chunks,
    //     };

    //     Ok(handle)
    // }

    // // fn get_metadata(path: &str) -> Result<Metadata, FileSystemError> {
    // //     todo!()
    // // }

    // async fn read(
    //     file_handle: &FileHandle,
    //     offset: usize,
    //     length: usize,
    // ) -> Result<Vec<u8>, std::io::Error> {
    //     let chunks = match file_handle.chunks {
    //         Some(chunks) => chunks,
    //         None => {
    //             return Err(Error::new(
    //                 std::io::ErrorKind::Other,
    //                 "Data about chunks is missing.",
    //             ))
    //         }
    //     };

    //     // Assumption for now:
    //     // offset = 0
    //     // length = max

    //     // TODO: probably should be parralell

    //     for (handle, address) in chunks.iter() {

    //         // TODO: connect to chunk server and get data
    //     }

    //     Ok(vec![])
    // }

    // // fn write(
    // //     file_handle: &FileHandle,
    // //     offset: usize,
    // //     data: &[u8],
    // // ) -> Result<usize, FileSystemError> {
    // //     todo!()
    // // }

    // // fn append(file_handle: &FileHandle, data: &[u8]) -> Result<usize, FileSystemError> {
    // //     todo!()
    // // }

    // // fn close(file_handle: FileHandle) -> Result<(), FileSystemError> {
    // //     todo!()
    // // }

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

        // 1. OpenFile for writing (path/filename) -> Master (file_handle)

        // 2. Split data into chunks

        // 3. Allocate chunk () -> Master (chunk_handle, Vec<Chunk servers (annotate primary)>)

        // 4. Stream chunk data to primary with list of secondaries

        // let chunks = split_into_chunks(chunk_size, data);

        // let mut master_client = MasterServiceClient::connect(self.master_address.clone())
        //     .await
        //     .expect("Client should connect to master server");

        // for (id, chunk) in chunks.iter().enumerate() {
        //     let request = Request::new(UploadFileRequest {
        //         file_name: file_name.to_string(),
        //         chunk_id: id.to_string(),
        //     });

        //     let upload_file_response = master_client
        //         .upload_file(request)
        //         .await
        //         .expect("Master server should return response with chunk location");

        //     println!("Uplad file response: {:?}", upload_file_response);

        //     let response = upload_file_response.into_inner();

        //     for chunk_location in response.chunk_locations {
        //         let address = chunk_location.address.clone();
        //         // let address = format!("http://{}", chunk_location.address.clone());
        //         let chunk_id = response.chunk_id.to_string();

        //         println!("Connecting to chunk server with address: {:?}", address);

        //         let mut chunk_client = ChunkServiceClient::connect(address)
        //             .await
        //             .expect("Client should connect to chunk server");

        //         let request = Request::new(StoreChunkRequest {
        //             chunk: Some(ChunkData {
        //                 chunk_id,
        //                 data: chunk.to_vec(),
        //             }),
        //         });

        //         // TODO: Grpc max size 4mb

        //         let write_chunk_response = chunk_client
        //             .store_chunk(request)
        //             .await
        //             .expect("Chunk server should return response with status");

        //         println!("Write chunk response: {:?}", write_chunk_response);
        //     }
        // }

        Ok(())
    }

    pub async fn get_file(&self, file_name: &str) -> Result<Bytes, Box<dyn std::error::Error>> {
        // let mut master_client = MasterServiceClient::connect(self.master_address.clone())
        //     .await
        //     .expect("Client should connect to master server");

        // let download_file_request = Request::new(DownloadFileRequest {
        //     file_name: file_name.to_string(),
        // });

        // let download_file_response = master_client
        //     .download_file(download_file_request)
        //     .await
        //     .expect("Master server should return data with chunks locations");

        // let chunks = download_file_response.into_inner().chunks;

        // let mut file_data = BytesMut::new();

        // for chunk_data in chunks {
        //     let address = chunk_data.chunk_id.to_string();

        //     let mut chunk_client = ChunkServiceClient::connect(address)
        //         .await
        //         .expect("Client should connect to chunk server");

        //     let request = Request::new(RetrieveChunkRequest {
        //         chunk_id: chunk_data.chunk_id.to_string(),
        //     });

        //     let response = chunk_client
        //         .retrieve_chunk(request)
        //         .await
        //         .expect("Chunk server should return chunk data");

        //     let chunk_data = response.into_inner().chunk.unwrap().data;

        //     file_data.extend_from_slice(&chunk_data);
        // }

        // let file_data = file_data.freeze();

        // Ok(file_data)
        Ok(Bytes::new())
    }

    pub async fn delete_file(&self, file_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}

fn split_into_chunks(chunk_size: usize, data: Bytes) -> Vec<Bytes> {
    data.chunks(chunk_size)
        .map(|chunk| Bytes::copy_from_slice(chunk))
        .collect()
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
