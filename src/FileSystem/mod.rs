// global下面存储顶层信息   现阶段假设文件为 ./global/map.json
// base下存储不同的表

use serde_json;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::hash::Hash;
use std::io::{ErrorKind, Write};
use std::path::Path;
use uuid::Uuid;

pub struct FileManager<'a> {
    map_file_path: &'a str,
    funcID: HashMap<String, fs::FileType>,
}

#[allow(non_snake_case)]
impl<'a> FileManager<'a> {
    pub fn new() -> Self {
        // mkdir, if not exist
        std::fs::create_dir_all("./global").expect("Error: cannot create directory:  ./global");
        // touch file and write empty json
        let mapjson_path = Path::new("./global/TableMap.json");
        if !mapjson_path.exists() {
            let mut mapjson_file = fs::File::create(mapjson_path).unwrap();
            mapjson_file.write_all("{}".as_bytes()).unwrap();
        }

        FileManager {
            map_file_path: "./global/TableMap.json",
            funcID: HashMap::new(),
        }
    }
    // create a new table (allocate UUID, touch file, update mapfile)
    // if success, return table uuid; otherwise return Error
    pub fn new_table(&mut self, tableName: &str) -> Result<String, Box<dyn Error>> {
        // TODO:
        // 1. if global directory doesn't exist, then try to create one,.
        // 2. add config path to .toml file

        // 1. read meta file  [FileIOError, TableExist]
        // if the TableMap.json doesn't exist, create new one.
        let mut tableMapData: HashMap<String, String> = match fs::read_to_string(self.map_file_path)
        {
            Ok(mapString) => {
                // open normally, read content
                serde_json::from_str(&mapString)
                    .expect("Failed to read global/TableMap.json. Probably format error!")
            }
            Err(e) => {
                // other error, like permission denied, report an error and return.
                return Err(e.into());
            }
        };
        // 2. allocate an UUID
        // if such name exists, then throw an error
        let new_uuid = Uuid::new_v4().to_string();
        if tableMapData.contains_key(tableName) {
            return Err(format!("Table {} exists", tableName).into());
        } else {
            tableMapData.insert(tableName.to_string(), new_uuid.to_string());
        }
        // 3. mkdir [DirectoryExist]
        let table_dir_path = format!("./base/{}", new_uuid);
        if Path::new(&table_dir_path).exists() {
            return Err(format!(" {} directory exist", table_dir_path).into());
        }
        fs::create_dir_all(table_dir_path)?;

        // 4. write to mapfile [FILEIOError]
        let mapjson_path = Path::new(self.map_file_path);
        let mut mapjson_file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(mapjson_path)
            .expect("Cannot open TableMap.json for writing!");
        let json_str = serde_json::to_string_pretty(&tableMapData)
            .expect("Cannot convert current map to string.");
        mapjson_file
            .write_all(json_str.as_bytes())
            .expect("Failed in writing to TableMap.json!");

        // return uuid, if all success
        return Ok(new_uuid);
    }

    pub fn open_table(&self, tableName: &str) -> Result<String, Box<dyn Error>> {
        let tableMapData: HashMap<String, String> = match fs::read_to_string(self.map_file_path) {
            Ok(mapString) => {
                // open normally, read content
                serde_json::from_str(&mapString)
                    .expect("Failed to read global/TableMap.json. Probably format error!")
            }
            Err(ref e) if e.kind() == ErrorKind::NotFound => {
                // file not found, then create a new one.
                fs::File::create_new(self.map_file_path).expect("Failed to create new file");
                let emptyJson: HashMap<String, String> = HashMap::new();
                emptyJson
            }
            Err(e) => {
                // other error, like permission denied, report an error and return.
                return Err(e.into());
            }
        };

        return match tableMapData.get(tableName) {
            Some(table_uuid) => Ok(table_uuid.to_string()),
            None => return Err(format!("table {} doesn't exist.", tableName).into()),
        };
    }
}
