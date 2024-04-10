use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    sync::Mutex,
};

#[derive(Debug)]
pub struct Metadata {
    namespace: Mutex<Namespace>,
    //TODO: Operation log
    // stores filename to chunk handles list mapping
    filepath_to_chunk_handles: Mutex<HashMap<String, Vec<String>>>,
    // stores chunk handles locations on chunk servers,
    chunk_handle_to_chunk_servers: Mutex<HashMap<String, Vec<String>>>,
}

impl Metadata {
    pub fn new() -> Self {
        let namespace = Mutex::new(Namespace::new());
        let filepath_to_chunk_handles = Mutex::new(HashMap::new());
        let chunk_handle_to_chunk_servers = Mutex::new(HashMap::new());

        Metadata {
            namespace,
            filepath_to_chunk_handles,
            chunk_handle_to_chunk_servers,
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

        self.filepath_to_chunk_handles
            .lock()
            .unwrap()
            .insert(file_path, Vec::new());
    }
}

#[derive(Debug)]
enum Node {
    Directory {
        name: String,
        nodes: HashMap<String, Node>,
    },
    File {
        name: String, // Do i need chunks stored here or in separate map<file_path/file_name, chunks>
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
            Node::File { name } => {
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
                .map(|node| match node {
                    Node::Directory { name, nodes } => name.as_str(),
                    Node::File { name } => name.as_str(),
                })
                .collect(),
            Node::File { name } => {
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
                });
            }
            Node::File { name } => {
                //TODO: Error
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
                Node::File { name } => {
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
}
