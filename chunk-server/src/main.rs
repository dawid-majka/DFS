use common::master_server::HeartbeatRequest;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::{env, fs};
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
    fn new(data_path: &str) -> Self {
        // Create dir if missing

        // TODO: Check if folder exists

        let path: Vec<&str> = data_path.split('/').collect();

        let home = env::var_os("HOME").unwrap();
        let mut full_path = PathBuf::from(home);

        for part in path {
            full_path.push(part);
        }

        println!("Creating directory: {:?}", full_path);

        match fs::create_dir_all(&full_path) {
            Ok(_) => println!("Successfully created directory: {:?}", full_path),
            Err(e) => println!(
                "Failed to create directory: {:?}, because: {}",
                full_path, e
            ),
        }

        // Get disc_usage
        let (used, available) = get_disc_usage();

        // Get stored chunks
        let chunk_handles = get_stored_chunk_handles(full_path.to_str().unwrap());

        StorageData {
            used,
            available,
            data_path: PathBuf::from_str(data_path).expect("data_path should be created"),
            chunk_handles,
        }
    }

fn get_disc_usage() -> (u64, u64) {
    let disc_usage = Command::new("df")
        .arg("-k")
        .arg("--output=used,avail")
        .arg("/")
        .output()
        .expect("Failed to execute df command");

    println!("disc_usage: {:?}", disc_usage);

    let output_str = String::from_utf8_lossy(&disc_usage.stdout);

    let data: Vec<&str> = output_str
        .split_once("\n")
        .unwrap()
        .1
        .split_ascii_whitespace()
        .collect();

    println!("{:?}", data);

    let used = data.first().unwrap().parse().unwrap();
    let available = data.get(1).unwrap().parse().unwrap();

    println!("disc_usage: used:{:?}, available: {:?}", used, available);

    (used, available)
}

fn get_stored_chunk_handles(data_path: &str) -> Vec<String> {
    let files = Command::new("find")
        .arg(data_path)
        .arg("-type")
        .arg("f")
        .output()
        .expect("Failed to execute find command");

    println!("stored files: {:?}", files);

    let files = String::from_utf8(files.stdout).expect("Failed to convert output to string");

    let filenames: Vec<String> = files
        .lines()
        .filter_map(|line| {
            Path::new(line)
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.to_string())
        })
        .collect();

    println!("stored file names: {:?}", filenames);

    filenames
}

#[derive(Debug, Default)]
struct ChunkServer {
    address: String,
    storage_data: StorageData,
}

impl ChunkServer {
    fn new(address: String) -> Self {
        let storage_data = StorageData::new("/chunk-server/data");

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
