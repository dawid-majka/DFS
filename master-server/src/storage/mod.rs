pub mod metadata;
mod namespace;
mod operation_log;

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tests::{
        metadata::{ChunkServerStatus, Metadata},
        namespace::{Namespace, Node, Status},
    };

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

        let server1 = ChunkServerStatus::new("123".to_string(), 1000000, 1000000, HashSet::new());

        let server2 = ChunkServerStatus::new("1234".to_string(), 1000000, 2000000, HashSet::new());

        let server3 = ChunkServerStatus::new("12345".to_string(), 1000000, 3000000, HashSet::new());

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
