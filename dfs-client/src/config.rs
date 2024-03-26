use config::Config;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Settings {
    pub master_port: u16,
    pub master_host: String,
}

pub fn get_configuration() -> Result<Settings, config::ConfigError> {
    let base_path = std::env::current_dir().expect("Failed to determine current directory.");
    let configuration_directory = base_path.join("dfs-client/configuration");

    let settings = Config::builder()
        .add_source(config::File::from(configuration_directory.join("base")).required(true))
        .build()
        .unwrap();

    settings.try_deserialize()
}
