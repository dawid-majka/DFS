use std::{
    collections::{HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    io::{Error, ErrorKind},
    sync::Mutex,
    time::Instant,
};

use common::master_server::{ChunkMetadata, HeartbeatRequest};

use crate::operation_log::OperationLog;

#[derive(Debug)]
struct ChunkServerStatus {
    address: String,
    used: u64,
    available: u64,
    chunk_handles: HashSet<String>,
    last_heartbeat: Instant,
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
    chunk_servers: Mutex<HashMap<String, ChunkServerStatus>>,
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

#[derive(Debug, Eq, PartialEq)]
enum Status {
    // Rethink this
    Active,
    Deleted,
}

#[derive(Debug)]
enum Node {
    Directory {
        name: String,
        nodes: HashMap<String, Node>,
    },
    File {
        name: String, // Do i need chunks stored here or in separate map<file_path/file_name, chunks>
        status: Status,
    },
}

impl Node {
    fn mkdir(&mut self, name: &str) -> Result<&mut Node, Error> {
        match self {
            Node::Directory { nodes, .. } => {
                let node = nodes.entry(name.to_string()).or_insert(Node::Directory {
                    name: name.to_string(),
                    nodes: HashMap::new(),
                });

                Ok(node)
            }
            Node::File { .. } => {
                // TODO: Error(Its not directory)
                Err(Error::new(
                    ErrorKind::Other,
                    "Path invalid. File not a directory.",
                ))
            }
        }
    }

    fn ls(&self) -> Vec<&str> {
        match self {
            Node::Directory { name, nodes } => nodes
                .values()
                .filter_map(|node| match node {
                    Node::Directory { name, nodes } => Some(name.as_str()),
                    Node::File { name, status } => match status {
                        Status::Deleted => None,
                        Status::Active => Some(name.as_str()),
                    },
                })
                .collect(),
            Node::File { .. } => {
                //TODO: Error
                Vec::new()
            }
        }
    }

    fn create_file(&mut self, file_name: &str) {
        match self {
            Node::Directory { nodes, .. } => {
                nodes.entry(file_name.to_string()).or_insert(Node::File {
                    name: file_name.to_string(),
                    status: Status::Active,
                });
            }
            Node::File { .. } => {
                //TODO: Error
            }
        }
    }

    fn mark_as_deleted(&mut self) {
        match self {
            Node::Directory { nodes, .. } => {
                // Error for now, what about deleting dirs?
                todo!()
            }
            Node::File { status, .. } => *status = Status::Deleted,
        }
    }

    fn get_node(&mut self, name: &str) -> &mut Node {
        match self {
            Node::Directory { nodes, .. } => {
                // Error handling
                nodes.get_mut(name).unwrap()
            }
            Node::File { .. } => {
                // Error:
                todo!()
            }
        }
    }
}

#[derive(Debug)]
struct Namespace {
    root: Node,
}

impl Namespace {
    pub fn new() -> Self {
        Namespace {
            root: Node::Directory {
                name: "".to_string(),
                nodes: HashMap::new(),
            },
        }
    }

    //TODO: fix &mut self unnecessary
    pub fn is_active(&mut self, file_path: &str) -> bool {
        let mut node = &mut self.root;
        let file_path = file_path.strip_prefix('/').unwrap();

        for part in file_path.split('/') {
            // Add validation
            if part.is_empty() {
                // Error(Invalid dir name)
            }

            node = node.get_node(part);
        }

        match node {
            Node::Directory { name, nodes } => {
                // ignore for now, rethink folder deletion later
                todo!()
            }
            Node::File { name, status } => match status {
                Status::Active => true,
                Status::Deleted => false,
            },
        }
    }

    pub fn create_file(&mut self, file_path: &str) {
        let mut node = &mut self.root;
        let file_path = file_path.strip_prefix('/').unwrap();
        if let Some((path, name)) = file_path.rsplit_once('/') {
            for part in path.split('/') {
                // Add validation
                if part.is_empty() {
                    // Error(Invalid dir name)
                }

                // mkdir if not exists else traverse
                node = node.mkdir(part).unwrap();
            }

            node.create_file(name);
        }
    }

    pub fn delete_file(&mut self, file_path: &str) {
        let mut node = &mut self.root;
        let file_path = file_path.strip_prefix('/').unwrap();

        for part in file_path.split('/') {
            // Add validation
            if part.is_empty() {
                // Error(Invalid dir name)
            }

            node = node.get_node(part);
        }
        node.mark_as_deleted();
    }

    // Path should always start with root
    pub fn mkdir(&mut self, path: &str) {
        let mut node = &mut self.root;
        let path = path.strip_prefix('/').unwrap();

        for part in path.split('/') {
            // Add validation
            if part.is_empty() {
                // Error(Invalid dir name)
            }
            node = node.mkdir(part).unwrap();
        }
    }

    pub fn ls(&self, path: &str) -> Vec<&str> {
        // TODO: Add separate method to validate path

        let path = path.strip_prefix('/').unwrap();

        let mut node = &self.root;
        for part in path.split('/') {
            // Add validation
            if part.is_empty() {
                // Error(Invalid dir name)
            }

            node = match node {
                Node::Directory { name, nodes } => {
                    match nodes.get(part) {
                        Some(node) => node,
                        None => {
                            // TODO: Error
                            todo!()
                        }
                    }
                }
                Node::File { .. } => {
                    // Error
                    todo!()
                }
            }
        }

        node.ls()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mkdir_should_create_dir() {
        let mut namespace = Namespace::new();
        namespace.mkdir("/path/to/new/directory");

        let path_dir = namespace.ls("/path");
        let to_dir = namespace.ls("/path/to");
        let new_dir = namespace.ls("/path/to/new");
        let directory_dir = namespace.ls("/path/to/new/directory");

        assert_eq!(path_dir.len(), 1);
        assert_eq!(path_dir[0], "to");
        assert_eq!(to_dir.len(), 1);
        assert_eq!(to_dir[0], "new");
        assert_eq!(new_dir.len(), 1);
        assert_eq!(new_dir[0], "directory");
        assert_eq!(directory_dir.len(), 0);
    }

    #[test]
    fn crate_file_should_create_file() {
        let mut namespace = Namespace::new();
        namespace.mkdir("/path/to");
        namespace.create_file("/path/to/new/directory/new_file");

        let path_dir = namespace.ls("/path");
        let to_dir = namespace.ls("/path/to");
        let new_dir = namespace.ls("/path/to/new");
        let directory_dir = namespace.ls("/path/to/new/directory");

        assert_eq!(path_dir.len(), 1);
        assert_eq!(path_dir[0], "to");
        assert_eq!(to_dir.len(), 1);
        assert_eq!(to_dir[0], "new");
        assert_eq!(new_dir.len(), 1);
        assert_eq!(new_dir[0], "directory");
        assert_eq!(directory_dir.len(), 1);
        assert_eq!(directory_dir[0], "new_file");
    }

    #[test]
    fn delete_file_should_mark_file_as_deleted() {
        let mut namespace = Namespace::new();
        namespace.create_file("/dir/new_file");

        let path_dir = namespace.ls("/dir");

        assert_eq!(path_dir.len(), 1);
        assert_eq!(path_dir[0], "new_file");

        namespace.delete_file("/dir/new_file");

        let path_dir = namespace.ls("/dir");
        assert_eq!(path_dir.len(), 0);

        match namespace.root.get_node("dir").get_node("new_file") {
            Node::Directory { name, nodes } => {
                panic!("Should be file not directort");
            }
            Node::File { name, status } => {
                assert_eq!(Status::Deleted, *status)
            }
        }
    }

    #[test]
    fn allocate_chunk_should_update_lookup_table() {
        let metadata = Metadata::new();
        let mut servers = metadata.chunk_servers.lock().unwrap();

        let server1 = ChunkServerStatus {
            address: "123".to_string(),
            used: 1000000,
            available: 1000000,
            chunk_handles: HashSet::new(),
            last_heartbeat: Instant::now(),
        };

        let server2 = ChunkServerStatus {
            address: "1234".to_string(),
            used: 1000000,
            available: 2000000,
            chunk_handles: HashSet::new(),
            last_heartbeat: Instant::now(),
        };

        let server3 = ChunkServerStatus {
            address: "12345".to_string(),
            used: 1000000,
            available: 3000000,
            chunk_handles: HashSet::new(),
            last_heartbeat: Instant::now(),
        };

        servers.insert(server1.address.clone(), server1);
        servers.insert(server2.address.clone(), server2);
        servers.insert(server3.address.clone(), server3);

        drop(servers);

        let file_path = "/test/directory/test_file.txt";

        metadata.create_file(file_path.to_string());
        let chunk_metadata = metadata.allocate_chunk(file_path, 1);

        assert_eq!(chunk_metadata.locations.len(), 3);
    }
}
