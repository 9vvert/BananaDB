// cache system
// 接受table_id, page_id, 封装所有和cache page有关的细节
mod lru_list;
pub mod resource;

use bitvec::{order::Lsb0, vec::BitVec};
use resource::ResId;
use std::{collections::HashMap, fs::File};

use crate::io_manager::{
    cache_system::{lru_list::LruList, resource::PageType},
    file_system::{self, FileManager},
};

// ==================== Page ======================

pub struct Page<const PAGE_SIZE: usize> {
    pub data: [u8; 4096],
    dirty: bool,
}

impl<const PAGE_SIZE: usize> Page<PAGE_SIZE> {
    pub fn new() -> Self {
        // initialize: fake cache
        Self {
            data: [0u8; 4096],
            dirty: false,
        }
    }

    pub fn set_dirty(&mut self) {
        self.dirty = true;
    }
    pub fn set_clean(&mut self) {
        self.dirty = false;
    }
}

// TODO:
// file system内部管理opened_file
// ==================== Cache ======================
pub struct CacheBuf<const PAGE_NUM: usize, const PAGE_SIZE: usize> {
    // io
    file_sys: FileManager,
    opened_file: HashMap<String, File>,
    // cache
    cache_map: HashMap<ResId, usize>,   // ResId -> cache page index
    reverse_map: HashMap<usize, ResId>, // cache id -> ResId
    pages: [Page<PAGE_SIZE>; PAGE_NUM],
    // pages: Vec<Page<PAGE_SIZE>>,
    lru_list: LruList<PAGE_NUM>,
}

impl<const PAGE_NUM: usize, const PAGE_SIZE: usize> CacheBuf<PAGE_NUM, PAGE_SIZE> {
    pub fn new() -> Self {
        CacheBuf {
            // io
            file_sys: FileManager::new(),
            opened_file: HashMap::new(),
            // cache
            cache_map: HashMap::new(),
            reverse_map: HashMap::new(),
            pages: std::array::from_fn(|_| Page::<PAGE_SIZE>::new()),
            lru_list: LruList::<PAGE_NUM>::new(),
        }
    }
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
        match self.query_cache_index(&res_id) {
            // cache hit
            Some(cache_id) => self.get_cache_resource(cache_id),
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
                self.file_sys.read_page(fd, page_id, &mut data_buf).unwrap();

                // fill the cache, move the ownership(avoid copy)
                self.add_cache_resource(&res_id, data_buf);

                // read from cache
                let new_cache_id = self.query_cache_index(&res_id).unwrap();

                // return the page mut-ref
                self.get_cache_resource(new_cache_id)
            }
        }
    }

    // TODO:
    // change to private
    //
    // to identify if current cache buffer has such resource
    // if has, then return cache id; otherwise return None
    pub fn query_cache_index(&self, res_id: &ResId) -> Option<usize> {
        if self.cache_map.contains_key(res_id) {
            Some(self.cache_map[res_id])
        } else {
            None
        }
    }

    // get cache by id
    pub fn get_cache_resource(&mut self, cache_id: usize) -> &mut [u8; 4096] {
        if cache_id >= PAGE_NUM {
            panic!("Invalid cache id {}, current max is {}", cache_id, PAGE_NUM);
        }
        // TODO:
        // set the used cahce page to list head
        self.lru_list.lift_page(cache_id).unwrap();
        &mut self.pages[cache_id].data
    }

    // input: buffer
    // then add data to cache
    // NOTE:
    // the buffer here is NOT ref, this is just a test
    // trying to move directly, aiming to reduce the cost of copy
    pub fn add_cache_resource(&mut self, res_id: &ResId, buffer: [u8; 4096]) {
        if self.query_cache_index(res_id).is_some() {
            panic!("Cache leak: trying load a page data twice!");
        }
        let cache_id: usize;
        // query if the cache is full
        if self.lru_list.have_free_page() {
            cache_id = self.lru_list.new_page().unwrap();
        } else {
            cache_id = self.lru_list.get_drop_page().unwrap();

            let old_res_id = self
                .reverse_map
                .get(&cache_id)
                .expect("Fatal! Mismatch cache_map and reverse_map!");
            // delete old map item
            // FIX:
            // write back, if dirty
            self.cache_map.remove(old_res_id);
            self.reverse_map.remove(&cache_id);

            self.lru_list.lift_page(cache_id).unwrap();
        }
        // load data, and clear the dirty signal
        self.pages[cache_id].data = buffer;
        self.pages[cache_id].set_clean();

        // add new map item
        // NOTE: first derive Clone for ResId, then clone it.
        self.cache_map.insert(res_id.clone(), cache_id);
        self.reverse_map.insert(cache_id, res_id.clone());
        //
    }
    // TEST:
    pub fn debug_cache(&self) {
        let mut result = String::new();
        result.push_str(&format!(
            "CacheBuf HashMap (size: {}):\n",
            self.cache_map.len()
        ));

        if self.cache_map.is_empty() {
            result.push_str("  <empty>\n");
        } else {
            for (res_id, cache_index) in &self.cache_map {
                result.push_str(&format!("  {:?} -> cache_index: {}\n", res_id, cache_index));
            }
        }
        println!("{}", result);
        self.lru_list.dump_lru_order();
    }
}
