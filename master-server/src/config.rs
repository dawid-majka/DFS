use config::Config;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Settings {
    pub port: u16,
    pub host: String,
}

pub fn get_configuration() -> Result<Settings, config::ConfigError> {
    let base_path = std::env::current_dir().expect("Failed to determine current directory.");
    let configuration_directory = base_path.join("master-server/configuration");

    let settings = Config::builder()
        .add_source(config::File::from(configuration_directory.join("base")).required(true))
        .build()
        .unwrap();

    settings.try_deserialize()
}
