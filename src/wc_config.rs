use serde::{Deserialize, Serialize};
use serde_json as json;
use std::fs;

#[derive(Serialize, Deserialize, Debug)]
pub struct WCConfig {
    pub indir: String,
    pub out_by_a: String,
    pub out_by_n: String,
    pub index_threads: u32,
    pub max_q_size: u32,
}

impl WCConfig {
    pub fn new(path: String) -> WCConfig {
        let content = fs::read_to_string(path).expect("Unable to read file");
        json::from_str(&content).unwrap_or_else(|err| {
            panic!("Json parse error: {:?}", err);
        })
    }
}