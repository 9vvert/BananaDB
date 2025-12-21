// global下面存储顶层信息   现阶段假设文件为 ./global/map.json
// base下存储不同的表

use serde_json;
use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Seek, Write};
use std::path::Path;

use crate::config::DATA_DIR;
use crate::dbms::resource::PageType;
use crate::dbms::resource::ResId;
use crate::error_type::IOManagerError;

const PAGE_SIZE: u16 = 4096;

pub struct FileManager {
    global_path: String,
    base_path: String,
    map_data: HashMap<String, TableItem>, // record  the meta info of a table
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct TableItem {
    // TODO: 新的记录形式
    // 将区分文件的工作放在上层
    name: String,
    file_type: PageType,
}

#[allow(non_snake_case)]
impl TableItem {
    pub fn new(name: &str, file_type: &PageType) -> Self {
        TableItem {
            name: name.to_string(),
            file_type: file_type.clone(),
        }
    }
}
impl FileManager {
    pub fn new() -> Self {
        // TODO:
        // 读取配置文件
        //
        //
        // ---------- create global map json
        // mkdir, if doesn't exist
        std::fs::create_dir_all(DATA_DIR.to_string() + "/global")
            .expect("Error: cannot create directory:  global");
        std::fs::create_dir_all(DATA_DIR.to_string() + "/base")
            .expect("Error: cannot create directory:  base");
        // touch file and write empty json
        let map_path = DATA_DIR.to_string() + "/global/TableMap.json";
        if !Path::new(&map_path).exists() {
            let mut file = fs::File::create(map_path).unwrap();
            file.write_all(b"{}").unwrap();
        }

        let global_map_string = fs::read_to_string(DATA_DIR.to_string() + "/global/TableMap.json")
            .expect("TableMap.json file format incorrect!");

        FileManager {
            global_path: DATA_DIR.to_string() + "/global/TableMap.json",
            base_path: DATA_DIR.to_string() + "/base/",
            map_data: serde_json::from_str(&global_map_string).unwrap(),
        }
    }

    // TODO:
    // clear data

    fn update_file_map(&self, map_data: &HashMap<String, TableItem>) {
        let mapjson_path = Path::new(&self.global_path);
        let mut mapjson_file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(mapjson_path)
            .expect("Cannot open TableMap.json for writing!");
        let json_str =
            serde_json::to_string_pretty(map_data).expect("Cannot convert current map to string.");
        mapjson_file
            .write_all(json_str.as_bytes())
            .expect("Failed in writing to TableMap.json!");
        println!("{}", json_str);
    }

    // TODO:
    // add character blacklist

    // each file name is bind with a directory
    // then using index to distinguish different type
    pub fn create_file(
        &mut self,
        file_name: &str,
        file_type: &PageType,
    ) -> Result<String, IOManagerError> {
        let dir_path = DATA_DIR.to_string() + "/base/" + file_name;
        let file_path = ResId::gen_file_path(file_name, file_type);
        std::fs::create_dir_all(dir_path)
            .expect(format!("Error: cannot create directory for {}", file_name).as_str());
        // INFO: as_str: String -> &str
        //       to_string: &str -> String

        if self.map_data.contains_key(&file_path) {
            Err(IOManagerError::AlreadyExistError(format!(
                "File {} already exists.",
                file_path
            )))
        } else {
            // update table map and write to map file
            self.map_data
                .insert(file_path.to_string(), TableItem::new(file_name, file_type));
            self.update_file_map(&self.map_data);
            // touch file

            fs::File::create_new(Path::new(&file_path))?;

            // TEST:
            //
            println!("create file:{file_path}");

            return Ok(format!("Create file: {}", file_path));
        }
    }

    pub fn delete_file(
        &mut self,
        file_name: &str,
        file_type: &PageType,
    ) -> Result<String, IOManagerError> {
        let file_path = ResId::gen_file_path(file_name, &file_type);
        if self.map_data.contains_key(&file_path) {
            self.map_data.remove(&file_path);
            self.update_file_map(&self.map_data);
            // TODO:
            // 删除具体的表数据文件
            Ok("Delete successfully.".into())
        } else {
            Err(IOManagerError::NotFoundError(format!(
                "Trying to delete a file that doesn't exist : \"{}\" !",
                file_path
            )))
        }
    }
    pub fn open_file(
        &self,
        file_name: &str,
        file_type: &PageType,
    ) -> Result<fs::File, IOManagerError> {
        let file_path = ResId::gen_file_path(file_name, file_type);
        // TEST:
        eprintln!("open file_path = {:?}", file_path);

        match std::fs::metadata(&file_path) {
            Ok(m) => eprintln!("metadata OK: len={}", m.len()),
            Err(e) => eprintln!("metadata ERR: {:?} ({})", e.kind(), e),
        }
        // END
        match OpenOptions::new().read(true).write(true).open(file_path) {
            Ok(f) => Ok(f),
            Err(e) => Err(IOManagerError::IOError(e)), // INFO:IOManagerError是自定义错误类型，还是需要用Err包装
        }
    }

    // 文件读写可能出现 io::Error
    pub fn read_page(
        &mut self,
        file: &mut fs::File,
        page_index: usize,
        buffer: &mut [u8; PAGE_SIZE as usize],
    ) -> Result<(), IOManagerError> {
        let offset: u64 = (page_index * PAGE_SIZE as usize) as u64;

        file.seek(std::io::SeekFrom::Start(offset))?; // INFO: '?' 在发生错误的时候向上传递，可以自动类型转换
        // 而上面的open_file不能直接 '?' 的原因是接受
        // fs::File类型
        file.read(buffer)?;
        Ok(())
    }

    pub fn write_page(
        &mut self,
        file: &mut fs::File,
        page_index: usize,
        buffer: &[u8; PAGE_SIZE as usize],
    ) -> Result<(), IOManagerError> {
        let offset: u64 = (page_index * PAGE_SIZE as usize) as u64;
        file.seek(std::io::SeekFrom::Start(offset))?;
        file.write(buffer)?;
        Ok(())
    }
}
