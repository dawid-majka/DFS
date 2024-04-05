use std::{collections::HashMap, sync::Mutex, vec};

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

    pub fn mkdir(&mut self, path: &str) {}

    pub fn ls(&self, path: &str) -> Vec<String> {
        vec![]
    }

    pub fn create_file(&mut self, path: &str, filename: &str) {}
}
