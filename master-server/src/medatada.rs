use std::{collections::HashMap, sync::Mutex};

#[derive(Debug, Default)]
pub struct Metadata {
    // TODO: Namespace
    // stores filename to chunk handles list mapping
    chunk_handles: Mutex<HashMap<String, Vec<String>>>,
    // stores chunk handles locations on chunk servers,
    pub locations: Mutex<HashMap<String, Vec<String>>>,
}

impl Metadata {
    pub fn new() -> Self {
        let chunk_handles = Mutex::new(HashMap::new());
        let locations = Mutex::new(HashMap::new());

        Metadata {
            chunk_handles,
            locations,
        }
    }
}
