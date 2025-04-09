use std::{
    collections::{HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    io::{Error, ErrorKind},
    sync::Mutex,
    time::Instant,
};

use common::master_server::{ChunkMetadata, HeartbeatRequest};

use crate::storage::operation_log::OperationLog;

use super::namespace::Namespace;

#[derive(Debug)]
pub struct ChunkServerStatus {
    pub address: String,
    used: u64,
    available: u64,
    chunk_handles: HashSet<String>,
    last_heartbeat: Instant,
}

impl ChunkServerStatus {
    pub fn new(address: String, used: u64, available: u64, chunk_handles: HashSet<String>) -> Self {
        ChunkServerStatus {
            address,
            used,
            available,
            chunk_handles,
            last_heartbeat: Instant::now(),
        }
    }
}

#[derive(Debug)]
pub struct Metadata {
    namespace: Mutex<Namespace>,
    //TODO: Operation log
    operation_log: Mutex<OperationLog>,
    // stores filename to chunk handles list mapping - updated during alloc
    filepath_to_chunk_handles: Mutex<HashMap<String, HashSet<u64>>>,
    // stores chunk handles locations on chunk servers - updated in heartbeat
    chunk_handle_to_chunk_servers: Mutex<HashMap<String, HashSet<String>>>,
    // stores adressess of chunk servers
    pub chunk_servers: Mutex<HashMap<String, ChunkServerStatus>>,
}

impl Metadata {
    pub fn new() -> Self {
        let namespace = Mutex::new(Namespace::new());
        let operation_log = Mutex::new(OperationLog::new());
        let filepath_to_chunk_handles = Mutex::new(HashMap::new());
        let chunk_handle_to_chunk_servers = Mutex::new(HashMap::new());
        let chunk_servers = Mutex::new(HashMap::new());

        Metadata {
            namespace,
            operation_log,
            filepath_to_chunk_handles,
            chunk_handle_to_chunk_servers,
            chunk_servers,
        }
    }

    pub fn mkdir(&self, path: &str) {
        self.namespace.lock().unwrap().mkdir(path)
    }

    pub fn ls(&self, path: &str) -> Vec<String> {
        self.namespace
            .lock()
            .unwrap()
            .ls(path)
            .into_iter()
            .map(|elem| elem.to_owned())
            .collect()
    }

    pub fn create_file(&self, file_path: String) {
        self.namespace.lock().unwrap().create_file(&file_path);

        // I should probably prevent overriding existing file
        self.filepath_to_chunk_handles
            .lock()
            .unwrap()
            .insert(file_path, HashSet::new());
    }

    pub fn delete_file(&self, file_path: String) {
        self.namespace.lock().unwrap().delete_file(&file_path);

        // Should i delete it form filepath_to_chunk_handles already
        // or during GC ?

        // During gc, but access should be limited by check to namespace if file has not been marked as to delete
        self.filepath_to_chunk_handles
            .lock()
            .unwrap()
            .remove(&file_path);
    }

    pub fn allocate_chunk(&self, file_path: &str, chunk_id: u64) -> ChunkMetadata {
        // Generate chunk handles
        let chunk_handle = self.generate_chunk_handle(file_path, chunk_id);

        // Update lookup table
        match self
            .filepath_to_chunk_handles
            .lock()
            .unwrap()
            .get_mut(file_path)
        {
            Some(handles) => {
                handles.insert(chunk_handle);

                let locations = self.get_locations_for_chunk();

                // TODO: Send Lease Message to one of servers

                let chunk_metadata = ChunkMetadata {
                    chunk_handle,
                    locations,
                };

                chunk_metadata
            }
            None => {
                //Error, file not created, so it is missing in lookup table
                todo!()
            }
        }
    }

    fn generate_chunk_handle(&self, file_path: &str, chunk_id: u64) -> u64 {
        let user_id = 1;

        let mut hasher = DefaultHasher::new();
        user_id.hash(&mut hasher);
        file_path.hash(&mut hasher);
        chunk_id.hash(&mut hasher);

        hasher.finish()
    }

    fn get_locations_for_chunk(&self) -> Vec<String> {
        // For now will take 3 servers with greatest available space
        let servers = self.chunk_servers.lock().unwrap();

        let mut entries: Vec<_> = servers.iter().collect();
        entries.sort_by(|a, b| b.1.available.cmp(&a.1.available));

        // Map the top three entries (if available) to their addresses
        let servers = entries
            .iter()
            .take(3) // Take only the top three entries
            .map(|(_key, status)| status.address.clone())
            .collect();

        servers
    }

    pub fn heartbeat_update(&self, request: HeartbeatRequest) -> Vec<String> {
        // This also acts as chunk server registration

        let mut servers = self.chunk_servers.lock().unwrap();
        let chunk_server_handles: HashSet<String> = request.chunk_handles.iter().cloned().collect();

        // Update server status map
        match servers.get_mut(&request.server_address) {
            Some(status) => {
                status.available = request.available;
                status.used = request.used;
                status.last_heartbeat = Instant::now();
                // Save state, will be updated to correct values in next heartbeat
                // Do i even need this ?
                status.chunk_handles = chunk_server_handles.clone();
            }
            None => {
                // Registration
                let server_status = ChunkServerStatus {
                    address: request.server_address.clone(),
                    used: request.used,
                    available: request.available,
                    chunk_handles: chunk_server_handles.clone(),
                    last_heartbeat: Instant::now(),
                };

                servers.insert(request.server_address.clone(), server_status);
            }
        }

        let mut locations_map = self.chunk_handle_to_chunk_servers.lock().unwrap();

        // Update chunk_handle to locations map
        for handle in chunk_server_handles.iter() {
            match locations_map.get_mut(handle) {
                Some(locations_set) => {
                    locations_set.insert(request.server_address.clone());
                }
                None => {
                    // If handle not presend here it means that it was allocated and upload was finished
                    let mut new_set = HashSet::new();
                    new_set.insert(request.server_address.clone());
                    locations_map.insert(handle.to_string(), new_set);
                }
            }
        }

        // do not have corresponding file or file marked as to_delete
        let to_delete = self.get_outdated_chunks(&chunk_server_handles);

        to_delete
    }

    fn get_outdated_chunks(&self, set_to_verify: &HashSet<String>) -> Vec<String> {
        // Check file 2 chunks map if file present

        let map = self.filepath_to_chunk_handles.lock().unwrap();

        let mut to_delete = Vec::new();

        for handle in set_to_verify {
            match map.iter().find(|(file_path, file_chunks)| {
                if file_chunks.contains(&handle.parse().unwrap()) {
                    return true;
                }
                false
            }) {
                Some((file_path, _)) => {
                    if !self.namespace.lock().unwrap().is_active(file_path) {
                        to_delete.push(handle.to_owned());
                    }
                }
                None => {
                    // File deleted, not present in file_path to chunks map
                    // File should be deleted after chunks have been deleted, so this scenario is
                    // possible if chunk server was not operational but get back from the dead and has stale chunks
                    to_delete.push(handle.to_owned());
                }
            }
        }

        to_delete
    }
}
