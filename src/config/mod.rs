use once_cell::sync::Lazy;
use serde::Deserialize;

// TODO:
// may need to be modified...
pub const DATA_DIR: &str = env!("CARGO_MANIFEST_DIR");

#[derive(Deserialize)]
pub struct ConfigData {
    pub page_size: u32,
    pub page_num: u32,
}
