use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
};

use tracing::{error, info};

#[derive(Debug, Default)]
pub struct Storage {
    used: u64,
    available: u64,
    data_path: PathBuf,
    chunk_handles: Vec<String>,
}

impl Storage {
    #[tracing::instrument]
    pub fn new(data_path: &str) -> Self {
        let path: Vec<&str> = data_path.split('/').collect();

        let home = env::var_os("HOME").unwrap();
        let mut full_path = PathBuf::from(home);

        for part in path {
            full_path.push(part);
        }

        info!("Creating directory: {:?}", full_path);

        match fs::create_dir_all(&full_path) {
            Ok(_) => info!("Successfully created directory: {:?}", full_path),
            Err(e) => error!(
                "Failed to create directory: {:?}, because: {}",
                full_path, e
            ),
        }

        // Get disc_usage
        let (used, available) = get_disc_usage();

        // Get stored chunks
        let chunk_handles = get_stored_chunk_handles(full_path.to_str().unwrap());

        Storage {
            used,
            available,
            data_path: PathBuf::from_str(data_path).expect("data_path should be created"),
            chunk_handles,
        }
    }

    pub fn get_used_storage(&self) -> u64 {
        self.used
    }

    pub fn get_available_storage(&self) -> u64 {
        self.available
    }

    pub fn get_chunk_handles(&self) -> Vec<String> {
        self.chunk_handles.clone()
    }
}

fn get_disc_usage() -> (u64, u64) {
    let disc_usage = Command::new("df")
        .arg("-k")
        .arg("--output=used,avail")
        .arg("/")
        .output()
        .expect("Failed to execute df command");

    info!("disc_usage: {:?}", disc_usage);

    let output_str = String::from_utf8_lossy(&disc_usage.stdout);

    let data: Vec<&str> = output_str
        .split_once("\n")
        .unwrap()
        .1
        .split_ascii_whitespace()
        .collect();

    info!("{:?}", data);

    let used = data.first().unwrap().parse().unwrap();
    let available = data.get(1).unwrap().parse().unwrap();

    info!("disc_usage: used:{:?}, available: {:?}", used, available);

    (used, available)
}

fn get_stored_chunk_handles(data_path: &str) -> Vec<String> {
    let files = Command::new("find")
        .arg(data_path)
        .arg("-type")
        .arg("f")
        .output()
        .expect("Failed to execute find command");

    info!("stored files: {:?}", files);

    let files = String::from_utf8(files.stdout).expect("Failed to convert output to string");

    let filenames: Vec<String> = files
        .lines()
        .filter_map(|line| {
            Path::new(line)
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.to_string())
        })
        .collect();

    info!("stored file names: {:?}", filenames);

    filenames
}
