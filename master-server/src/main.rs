use std::sync::Arc;

use config::get_configuration;
use server::run;

use tracing_subscriber::{EnvFilter, FmtSubscriber};

use crate::medatada::Metadata;
use crate::server::MasterServer;

mod config;
mod medatada;
mod operation_log;
mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    FmtSubscriber::builder().with_env_filter(filter).init();

    let configuration = get_configuration().expect("Failed to read conifguration");
    let address = format!("{}:{}", configuration.host, configuration.port);

    let metadata = Arc::new(Metadata::new());

    let master = MasterServer::new(metadata);

    let server = run(master, address)?;

    server.await?;

    Ok(())
}
