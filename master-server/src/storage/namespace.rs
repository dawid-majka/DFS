use std::collections::HashMap;

#[derive(Debug)]
pub struct Namespace {
    pub root: Node,
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

#[derive(Debug, Eq, PartialEq)]
pub enum Status {
    // Rethink this
    Active,
    Deleted,
}

#[derive(Debug)]
pub enum Node {
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

    pub fn get_node(&mut self, name: &str) -> &mut Node {
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
