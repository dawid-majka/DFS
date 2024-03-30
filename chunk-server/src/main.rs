use common::master_server::HeartbeatRequest;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use std::vec;
use tokio::net::TcpListener;

use common::chunk_server::data_service_server::{DataService, DataServiceServer};
use common::chunk_server::{
    AcquireChunksRequest, GrantLeaseRequest, GrantLeaseResponse, RetrieveChunkRequest,
    RetrieveChunkResponse, StoreChunkRequest, StoreChunkResponse,
};

use common::master_server::chunk_service_client::ChunkServiceClient;

use common::chunk_server::metadata_service_server::{MetadataService, MetadataServiceServer};

use common::shared::{ChunkData, EmptyReply};
use tokio::time::interval;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use tokio_stream::wrappers::TcpListenerStream;

use config::get_configuration;

mod config;

#[derive(Debug, Default)]
struct StorageData {
    used: u64,
    available: u64,
    data_path: PathBuf,
    chunk_handles: Vec<String>,
}

impl StorageData {
    fn get_storage_data() -> Self {
        // TODO: Get storage data using Command df:

        let path = "/";

        let disc_usage = Command::new("df")
            .arg("-h")
            .arg(path)
            .output()
            .expect("Failed to execute df command");

        println!("disc_usage: {:?}", disc_usage);

        let directory = "/files";

        let files = Command::new("find")
            .arg(directory)
            .arg("-type")
            .arg("f")
            .output()
            .expect("Failed to execute find command");

        println!("disc_usage: {:?}", files);

        let used = 0;
        let available = 0;
        let data_path = PathBuf::from(directory);
        let chunk_handles = vec![];

        StorageData {
            used,
            available,
            data_path,
            chunk_handles,
        }
    }
}

#[derive(Debug, Default)]
struct ChunkServer {
    address: String,
    storage_data: StorageData,
}

impl ChunkServer {
    fn new(address: String) -> Self {
        let storage_data = StorageData::get_storage_data();

        ChunkServer {
            address,
            storage_data,
        }
    }
}

#[tonic::async_trait]
impl DataService for ChunkServer {
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
            chunk_handle: "1".to_string(),
            data: Vec::new(),
        });

        let response = RetrieveChunkResponse { chunk };

        Ok(Response::new(response))
    }
}

#[tonic::async_trait]
impl MetadataService for ChunkServer {
    async fn grant_lease(
        &self,
        request: Request<GrantLeaseRequest>,
    ) -> std::result::Result<tonic::Response<GrantLeaseResponse>, Status> {
        todo!()
    }

    async fn acquire_chunks(
        &self,
        request: Request<AcquireChunksRequest>,
    ) -> std::result::Result<Response<EmptyReply>, Status> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let configuration = get_configuration().expect("Failed to read conifguration");

    // TODO: Get storage data
    let master_addr = format!(
        "http://{}:{}",
        configuration.master_host, configuration.master_port
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let addr = listener.local_addr().unwrap();

    let chunk = ChunkServer::new(addr.to_string());

    let chunk = Arc::new(chunk);

    let server = Server::builder()
        .add_service(DataServiceServer::from_arc(chunk.clone()))
        .add_service(MetadataServiceServer::from_arc(chunk.clone()))
        .serve_with_incoming(TcpListenerStream::new(listener));

    println!("Chunk server listening on {}", &addr);

    //TODO: try_connect
    let mut client = ChunkServiceClient::connect(master_addr).await?;

    let server_address = format!("http://{}", addr);

    let mut interval = interval(Duration::from_secs(5));

    // Move Server run and heatbeat communication to other methods
    tokio::spawn(async move {
        loop {
            interval.tick().await;

            let used = chunk.storage_data.used;
            let available = chunk.storage_data.available;
            let chunk_handles = chunk.storage_data.chunk_handles.clone();
            let server_address = server_address.clone();

            let request = Request::new(HeartbeatRequest {
                server_address,
                used,
                available,
                chunk_handles,
            });

            match client.heartbeat(request).await {
                Ok(response) => println!(
                    "Heartbeat sent. Resonse.to_delete len: {}",
                    response.into_inner().to_delete.len()
                ),
                Err(e) => println!("Failed to send heartbeat: {}", e),
            }
        }
    });

    server.await?;

    Ok(())
}
