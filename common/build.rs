fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().build_server(true).compile(
        &[
            "proto/master_server.proto",
            "proto/chunk_server.proto",
            "proto/shared.proto",
        ],
        &["proto"],
    )?;
    Ok(())
}
