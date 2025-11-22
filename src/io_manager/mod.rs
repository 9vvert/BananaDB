use cache_system::resource::*;
use std::collections::HashMap;
use std::fs::File;
use std::{error::Error, fs};

pub mod cache_system;
pub mod file_system;

#[allow(non_camel_case_types)]
struct IO_Manager<const PAGE_NUM: usize> {
    cache_sys: cache_system::CacheBuf<PAGE_NUM>,
    file_sys: file_system::FileManager,
    opened_file: HashMap<String, File>,
}

impl<'a, const PAGE_NUM: usize> IO_Manager<PAGE_NUM> {
    pub fn new() -> Self {
        IO_Manager {
            cache_sys: cache_system::CacheBuf::<PAGE_NUM>::new(),
            file_sys: file_system::FileManager::new(),
            opened_file: HashMap::new(),
        }
    }
    pub fn create_table(&mut self, table_name: &str) {
        // WARN:
        // add exception detect
        // DON'T unwrap directly!!!
        self.file_sys.new_table(table_name);
    }

    // get data
    // FIX:
    // add check in self.cache_sys.add_cache_resource, for it may replace an dirty page!
    pub fn get_page(
        &mut self,
        page_type: PageType,
        page_id: usize,
        file_name: &str,
        base_path: Option<&str>,
    ) -> &mut [u8; 4096] {
        let res_id = ResId::new(page_type, file_name, page_id);

        let file_path = base_path.unwrap_or("./").to_string() + file_name;
        // first: query if there is cache.
        // assume that the borrowd cache is dropped at once.
        match self.cache_sys.query_cache_index(&res_id) {
            // cache hit
            Some(cache_id) => self.cache_sys.get_cache_resource(cache_id),
            // cache miss
            None => {
                // request for data by file_sys
                if !self.opened_file.contains_key(&file_path) {
                    let new_fd = self.file_sys.open_file(&file_path).unwrap();
                    self.opened_file.insert(file_path.to_string(), new_fd);
                }

                // get the file descriptor of target file.
                let fd: &mut File = self.opened_file.get_mut(&file_path).unwrap();

                // read data to buffer
                let mut data_buf: [u8; 4096] = [0; 4096];
                self.file_sys.read_page(fd, page_id, &mut data_buf);

                // fill the cache, move the ownership(avoid copy)
                self.cache_sys.add_cache_resource(&res_id, data_buf);

                // read from cache
                let new_cache_id = self.cache_sys.query_cache_index(&res_id).unwrap();
                self.cache_sys.get_cache_resource(new_cache_id)
            }
        }
    }
}
