// cache system
// 接受table_id, page_id, 封装所有和cache page有关的细节
mod lru_list;
pub mod resource;

use bitvec::{order::Lsb0, vec::BitVec};
use resource::ResId;
use std::collections::HashMap;

use crate::io_manager::cache_system::lru_list::LruList;

pub struct CacheBuf<const PAGE_NUM: usize> {
    cache_map: HashMap<ResId, usize>, // ResId -> cache page index
    valid: BitVec<usize, Lsb0>,
    dirty: BitVec<usize, Lsb0>,
    data: Vec<[u8; 4096]>,
    lru_list: LruList<PAGE_NUM>,
}

impl<const PAGE_NUM: usize> CacheBuf<PAGE_NUM> {
    pub fn new() -> Self {
        CacheBuf {
            cache_map: HashMap::new(),
            data: vec![[0u8; 4096]; PAGE_NUM],
            valid: BitVec::<usize, Lsb0>::repeat(false, PAGE_NUM),
            dirty: BitVec::<usize, Lsb0>::repeat(false, PAGE_NUM),
            lru_list: LruList::<PAGE_NUM>::new(),
        }
    }

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
        if cache_id >= self.data.len() {
            panic!(
                "Invalid cache id {}, current max is {}",
                cache_id,
                self.data.len()
            );
        }
        &mut self.data[cache_id]
    }

    // input: buffer
    // then add data to cache
    // NOTE:
    // the buffer here is NOT ref, this is just a test
    // trying to move directly, aiming to reduce the cost of copy
    pub fn add_cache_resource(&mut self, res_id: &ResId, buffer: [u8; 4096]) {
        if self.query_cache_index(res_id).is_none() {
            panic!("Cache leak: trying load a page data twice!");
        }
        let cache_id: usize;
        // query if the cache is full
        if self.lru_list.have_free_page() {
            cache_id = self.lru_list.new_page().unwrap();
        } else {
            cache_id = self.lru_list.get_drop_page().unwrap();
            // TODO:
            // check if the lift_page is used correctly
            self.lru_list.lift_page(cache_id);
        }
        // load data
        self.data[cache_id] = buffer;

        let res_id_key = res_id.clone(); // NOTE: first derive Clone for ResId, then clone it.
        self.cache_map.insert(res_id_key, cache_id);
    }

    // old interface
    pub fn find_free(&self) -> Option<usize> {
        self.valid.iter().position(|bit| !bit)
    }
    pub fn set_free(&mut self, index: usize) {
        self.valid.set(index, false);
    }
    pub fn set_busy(&mut self, index: usize) {
        self.valid.set(index, true);
    }
    pub fn is_dirty(&self, index: usize) -> bool {
        return self.dirty[index];
    }
    pub fn set_dirty(&mut self, index: usize) {
        self.dirty.set(index, true);
    }
    pub fn set_clean(&mut self, index: usize) {
        self.dirty.set(index, false);
    }
}
