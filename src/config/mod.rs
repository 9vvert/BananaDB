use once_cell::sync::Lazy;
use serde::Deserialize;
use toml;

#[derive(Deserialize)]
pub struct ConfigData {
    pub page_size: u32,
    pub page_num: u32,
}

pub static CONFIG: Lazy<ConfigData> = Lazy::new(|| {
    let toml_content = std::fs::read_to_string("config.toml").unwrap();
    toml::from_str(&toml_content).unwrap()
});
