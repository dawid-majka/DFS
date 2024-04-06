use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    sync::Mutex,
    vec,
};

#[derive(Debug)]
pub struct Metadata {
    // TODO: Namespace
    namespace: Mutex<Namespace>,
    //TODO: Edit log
    // stores filename to chunk handles list mapping
    chunk_handles: Mutex<HashMap<String, Vec<String>>>,
    // stores chunk handles locations on chunk servers,
    pub locations: Mutex<HashMap<String, Vec<String>>>,
}

impl Metadata {
    pub fn new() -> Self {
        let namespace = Mutex::new(Namespace::new());
        let chunk_handles = Mutex::new(HashMap::new());
        let locations = Mutex::new(HashMap::new());

        Metadata {
            namespace,
            chunk_handles,
            locations,
        }
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
            Node::Directory { name, nodes } => {
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

    // TODO: Only from root ? Or should i check if root
    pub fn mkdir(&mut self, path: &str) {
        let mut node = &mut self.root;
        for part in path.split('/') {
            // Add validation
            if part.is_empty() {
                // Error(Invalid dir name)
            }
            node = node.mkdir(part).unwrap();
        }
    }

    pub fn ls(&self, path: &str) -> Vec<String> {
        vec![]
    }

    pub fn create_file(&mut self, path: &str, filename: &str) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mkdir_should_create_dir() {
        let mut namespace = Namespace::new();

        namespace.mkdir("/path/to/new/directory");
    }
}
