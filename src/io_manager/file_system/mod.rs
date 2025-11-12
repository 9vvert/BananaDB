// global下面存储顶层信息   现阶段假设文件为 ./global/map.json
// base下存储不同的表

use serde_json;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::hash::Hash;
use std::io::{ErrorKind, Read, Seek, Write};
use std::path::Path;
use uuid::Uuid;

const PAGE_SIZE: u16 = 4096;

pub struct FileManager {
    global_path: String,
    base_path: String,
    table_map_data: HashMap<String, TableItem>, // record  the meta info of a table
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct TableItem {
    name: String, // TODO:
                  // 新的记录形式
}

#[allow(non_snake_case)]
impl TableItem {
    pub fn new(name: &str) -> Self {
        TableItem {
            name: name.to_string(),
        }
    }
}
impl FileManager {
    pub fn new() -> Self {
        // TODO:
        // 读取配置文件
        //
        // mkdir, if not exist
        std::fs::create_dir_all("./global").expect("Error: cannot create directory:  ./global");
        std::fs::create_dir_all("./base").expect("Error: cannot create directory:  ./base");
        // touch file and write empty json
        //
        let map_path = Path::new("./global/TableMap.json");
        if !map_path.exists() {
            let mut file = fs::File::create(map_path).unwrap();
            file.write_all(b"{}").unwrap();
        }

        let global_map_string = fs::read_to_string("./global/TableMap.json")
            .expect("TableMap.json file format incorrect!");

        FileManager {
            global_path: "./global/TableMap.json".to_string(),
            base_path: "./base".to_string(),
            table_map_data: serde_json::from_str(&global_map_string).unwrap(),
        }
    }

    // TODO:
    // clear data

    fn update_table_map(&self, table_map_data: &HashMap<String, TableItem>) {
        let mapjson_path = Path::new(&self.global_path);
        let mut mapjson_file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(mapjson_path)
            .expect("Cannot open TableMap.json for writing!");
        let json_str = serde_json::to_string_pretty(table_map_data)
            .expect("Cannot convert current map to string.");
        mapjson_file
            .write_all(json_str.as_bytes())
            .expect("Failed in writing to TableMap.json!");
        println!("{}", json_str);
    }

    // create a new table (allocate UUID, touch file, update mapfile)
    // if success, return table uuid; otherwise return Error
    pub fn new_table(&mut self, table_name: &str) -> Result<String, Box<dyn Error>> {
        // TODO:
        // 1. if global directory doesn't exist, then try to create one,.
        // 2. add config path to .toml file

        if self.table_map_data.contains_key(table_name) {
            return Err(format!("Table {} exists", table_name).into());
        } else {
            // update table map and write to map file
            self.table_map_data
                .insert(table_name.to_string(), TableItem::new(table_name));
            self.update_table_map(&self.table_map_data);
            // touch file

            let table_path = self.base_path.to_owned() + table_name;
            let table_data_path = Path::new(&table_path);
            fs::File::create_new(table_data_path);
            return Ok("Create success".to_string());
        }
    }

    pub fn delete_table(&mut self, table_name: &str) -> Result<String, Box<dyn Error>> {
        if self.table_map_data.contains_key(table_name) {
            self.table_map_data.remove(table_name);
            self.update_table_map(&self.table_map_data);
            // TODO:
            // 删除具体的表数据文件
            return Ok("Delete successfully.".into());
        } else {
            return Err(format!("table {} doesn't exist.", table_name).into());
        }
    }
    pub fn open_file(&self, path_str: &str) -> Result<fs::File, Box<dyn Error>> {
        let file_path = Path::new(path_str);

        if file_path.exists() {
            return Ok(fs::File::open(file_path).unwrap());
        } else {
            return Err(format!("Cannot open file {}", path_str).into());
        }
    }

    pub fn read_page(
        &mut self,
        file: &mut fs::File,
        page_index: u32,
        buffer: &mut [u8; PAGE_SIZE as usize],
    ) {
        let offset: u64 = (page_index * PAGE_SIZE as u32).into();
        file.seek(std::io::SeekFrom::Start(offset));
        file.read(buffer);
    }

    pub fn write_page(
        &mut self,
        file: &mut fs::File,
        page_index: u32,
        buffer: &[u8; PAGE_SIZE as usize],
    ) {
        let offset: u64 = (page_index * PAGE_SIZE as u32).into();
        file.seek(std::io::SeekFrom::Start(offset));
        file.write(buffer);
    }
}
