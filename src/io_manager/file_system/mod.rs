// global下面存储顶层信息   现阶段假设文件为 ./global/map.json
// base下存储不同的表

use serde_json;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::hash::Hash;
use std::io::{ErrorKind, Read, Write};
use std::path::Path;
use uuid::Uuid;

const PAGE_SIZE: u16 = 4096;

pub struct FileManager<'a> {
    map_file_path: &'a str,
    base_path: &'a str,
    table_map_data: HashMap<String, String>,
}

#[allow(non_snake_case)]
impl<'a> FileManager<'a> {
    pub fn new() -> Self {
        // mkdir, if not exist
        // TODO:在已经有文件夹的情况下是否会覆盖？
        std::fs::create_dir_all("./global").expect("Error: cannot create directory:  ./global");
        // touch file and write empty json
        let mapjson_path = Path::new("./global/TableMap.json");
        if !mapjson_path.exists() {
            let mut mapjson_file = fs::File::create(mapjson_path).unwrap();
            mapjson_file.write_all("{}".as_bytes()).unwrap();
        }

        let global_map_string = match fs::read_to_string("./global/TableMap.json")?;
        

        FileManager {
            map_file_path: "./global/TableMap.json",
            base_path: "./base",
            table_map_data: serde_json::from_str(&global_map_string).unwrap(),
        }
    }

    pub fn clear(&self) {
        let base_path = Path::new(self.base_path);
        let global_path = Path::new(self.map_file_path);

        if base_path.exists() {
            fs::remove_dir_all(base_path).unwrap();
        }

        if global_path.exists() {
            fs::remove_dir_all(global_path).unwrap();
        }
    }

    fn write_table_map(&mut self, table_map_data: &HashMap<String, String>) {
        let mapjson_path = Path::new(self.map_file_path);
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
    pub fn new_table(&mut self, tableName: &str) -> Result<String, Box<dyn Error>> {
        // TODO:
        // 1. if global directory doesn't exist, then try to create one,.
        // 2. add config path to .toml file

        // 1. read meta file  [FileIOError, TableExist]
        // 2. allocate an UUID
        // if such name exists, then throw an error
        let new_uuid = Uuid::new_v4().to_string();
        if self.table_map_data.contains_key(tableName) {
            return Err(format!("Table {} exists", tableName).into());
        } else {
            self.table_map_data.insert(tableName.to_string(), new_uuid.to_string());
            let json_str = serde_json::to_string_pretty(&self.table_map_data).expect("f");
            println!("{}", json_str);
        }
        // 3. mkdir [DirectoryExist]
        let table_dir_path = format!("./base/{}", new_uuid);
        if Path::new(&table_dir_path).exists() {
            return Err(format!(" {} directory exist", table_dir_path).into());
        }
        fs::create_dir_all(table_dir_path)?;

        // 4. write to mapfile [FILEIOError]
        self.write_table_map(self.table_map_data);

        // return uuid, if all success
        return Ok(new_uuid);
    }

    pub fn delete_table(&mut self, tableName: &str) -> Result<String, Box<dyn Error>> {

        if self.table_map_data.contains_key(tableName) {
            self.table_map_data.remove(tableName);
            self.write_table_map(self.table_map_data);
            return Ok("Delete successfully.".into());
        } else {
            return Err(format!("table {} doesn't exist.", tableName).into());
        }
    }

    pub fn open_table(&self, tableName: &str) -> Result<String, Box<dyn Error>> {
        let tableMapData: HashMap<String, String> = self.read_table_map()?;

        return match tableMapData.get(tableName) {
            Some(table_uuid) => Ok(table_uuid.to_string()),
            None => return Err(format!("table {} doesn't exist.", tableName).into()),
        };
    }

    // NOTE:
    // current implementation: create new page when needed but dont delete them
    // TODO:
    // add a json file for each table, recording their page, and allocate a new page without page
    // argument
    pub fn new_page(&mut self, table_uuid: &str, page_index: u32) -> Result<(), Box<dyn Error>> {
        let page_path =
            self.base_path.to_string() + "/" + table_uuid + "/" + &page_index.to_string();
        let empty_buffer: [u8; PAGE_SIZE as usize] = [0; PAGE_SIZE as usize];

        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&page_path)
            .expect(&format!("Error in opening {}", page_path));

        let n = file.write(&empty_buffer);

        return match n {
            Ok(write_size) => {
                if write_size == PAGE_SIZE as usize {
                    Ok(())
                } else {
                    Err("Cannot memset new page with 0. Maybe it is due to IO Error.".into())
                }
            }
            Err(e) => Err(e.into()),
        };
    }

    pub fn read_page(
        &mut self,
        table_uuid: &str,
        page_index: u32,
        buffer: &mut [u8; PAGE_SIZE as usize],
    ) -> Result<(), Box<dyn Error>> {
        let page_path =
            self.base_path.to_string() + "/" + table_uuid + "/" + &page_index.to_string();

        let mut file = fs::OpenOptions::new().read(true).open(&page_path)?;

        let n = file.read(buffer);

        return match n {
            Ok(read_size) => {
                if read_size == PAGE_SIZE as usize {
                    Ok(())
                } else {
                    Err("Cannot fill a page. Maybe the data is coruppted!".into())
                }
            }
            Err(e) => Err(e.into()),
        };
    }

    pub fn write_page(
        &mut self,
        table_uuid: &str,
        page_index: u32,
        buffer: &[u8; PAGE_SIZE as usize],
    ) -> Result<(), Box<dyn Error>> {
        let page_path =
            self.base_path.to_string() + "/" + table_uuid + "/" + &page_index.to_string();

        let mut file = fs::OpenOptions::new().write(true).open(&page_path)?;
        let n = file.write(buffer);

        return match n {
            Ok(write_size) => {
                if write_size == PAGE_SIZE as usize {
                    Ok(())
                } else {
                    Err("Cannot write buffer to page. Maybe it is due to IO Error.".into())
                }
            }
            Err(e) => Err(e.into()),
        };
    }
}
