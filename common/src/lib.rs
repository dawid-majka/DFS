pub mod master_server {
    tonic::include_proto!("dfs.master_server");
}

pub mod chunk_server {
    tonic::include_proto!("dfs.chunk_server");
}

pub mod client {
    tonic::include_proto!("dfs.client");
}
pub mod shared {
    tonic::include_proto!("dfs.shared");
}
