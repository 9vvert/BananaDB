// cache system
// 接受table_id, page_id, 封装所有和cache page有关的细节
mod lru_list;
pub mod resource;

use bitvec::{order::Lsb0, vec::BitVec};
use resource::ResId;
use std::collections::HashMap;

use crate::io_manager::cache_system::lru_list::LruList;

pub struct CacheBuf<const PAGE_NUM: usize> {
    cache_map: HashMap<ResId, usize>,   // ResId -> cache page index
    reverse_map: HashMap<usize, ResId>, // cache id -> ResId
    valid: BitVec<usize, Lsb0>,
    dirty: BitVec<usize, Lsb0>,
    data: Vec<[u8; 4096]>,
    lru_list: LruList<PAGE_NUM>,
}

impl<const PAGE_NUM: usize> CacheBuf<PAGE_NUM> {
    pub fn new() -> Self {
        CacheBuf {
            cache_map: HashMap::new(),
            reverse_map: HashMap::new(),
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
        // TODO:
        // set the used cahce page to list head
        self.lru_list.lift_page(cache_id).unwrap();
        &mut self.data[cache_id]
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
            self.cache_map.remove(old_res_id);
            self.reverse_map.remove(&cache_id);

            self.lru_list.lift_page(cache_id);
        }
        // load data
        self.data[cache_id] = buffer;

        // add new map item
        // NOTE: first derive Clone for ResId, then clone it.
        self.cache_map.insert(res_id.clone(), cache_id);
        self.reverse_map.insert(cache_id, res_id.clone());
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
