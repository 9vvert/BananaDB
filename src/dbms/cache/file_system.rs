// global下面存储顶层信息   现阶段假设文件为 ./global/map.json
// base下存储不同的表

use serde_json;
use std::collections::HashMap;
use std::fs::{self, remove_file};
use std::fs::{OpenOptions, create_dir};
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
}

impl FileManager {
    pub fn new(global_path: &str, base_path: &str) -> Self {
        FileManager {
            global_path: global_path.to_string(),
            base_path: base_path.to_string(),
        }
    }

    // TODO:
    // clear data

    // TODO:
    // add character blacklist

    // each file name is bind with a directory
    // then using index to distinguish different type
    pub fn create_file(
        &mut self,
        file_name: &str,
        file_type: &PageType,
        extra_info: &str,
    ) -> Result<String, IOManagerError> {
        let file_path_str = &ResId::gen_file_path(file_name, file_type, extra_info);
        let file_path = Path::new(file_path_str);
        // INFO: as_str: String -> &str
        //       to_string: &str -> String

        // touch file
        if let Some(parent_dir) = file_path.parent() {
            fs::create_dir_all(parent_dir)?;
        }
        fs::File::create_new(Path::new(&file_path))?;

        // TEST:
        //
        println!("create file:{file_path_str}");
        return Ok(format!("Create file: {}", file_path_str));
    }

    pub fn delete_file(&mut self, file_name: &str, file_type: &PageType, extra_info: &str) {
        // TODO:
        // delete file
        let file_path = ResId::gen_file_path(file_name, file_type, extra_info);
        remove_file(file_path).unwrap();
    }
    pub fn open_file(
        &self,
        file_name: &str,
        file_type: &PageType,
        extra_info: &str,
    ) -> Result<fs::File, IOManagerError> {
        let file_path = ResId::gen_file_path(file_name, file_type, extra_info);
        // TEST:
        // match std::fs::metadata(&file_path) {
        //     Ok(m) => eprintln!("metadata OK: len={}", m.len()),
        //     Err(e) => eprintln!("metadata ERR: {:?} ({})", e.kind(), e),
        // }

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
