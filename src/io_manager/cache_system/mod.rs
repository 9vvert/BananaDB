// cache system
// 接受table_id, page_id, 封装所有和cache page有关的细节
mod lru_list;
use bitvec::{order::Lsb0, vec::BitVec};
use std::collections::HashMap;

use crate::io_manager::cache_system::lru_list::LruList;

pub struct CacheBuf<const PAGE_NUM: usize> {
    page_map: HashMap<String, Vec<i64>>,
    valid: BitVec<usize, Lsb0>,
    dirty: BitVec<usize, Lsb0>,
    data: Vec<[u8; 4096]>,
    lru_list: LruList<PAGE_NUM>,
}

impl<const PAGE_NUM: usize> CacheBuf<PAGE_NUM> {
    pub fn new() -> Self {
        CacheBuf {
            page_map: HashMap::new(),
            data: vec![[0u8; 4096]; PAGE_NUM],
            valid: BitVec::<usize, Lsb0>::repeat(false, PAGE_NUM),
            dirty: BitVec::<usize, Lsb0>::repeat(false, PAGE_NUM),
            lru_list: LruList::<PAGE_NUM>::new(),
        }
    }
    // [input] table_id, page_id
    // [output] Option(cache_index)
    pub fn get_cache_index(&mut self, table_id: &str, page_id: usize) -> Option<usize> {
        match self.page_map.get_mut(table_id) {
            Some(table_index_vec) => {
                if table_index_vec.len() <= page_id {
                    None
                } else {
                    let cache_index = table_index_vec[page_id];
                    if cache_index >= 0 && cache_index < PAGE_NUM as i64 {
                        Some(cache_index as usize)
                    } else {
                        None
                    }
                }
            }
            None => None,
        }
    }
    pub fn set_cache_index(&mut self, table_id: &str, page_id: usize, cache_id: usize) {
        if !self.page_map.contains_key(table_id) {
            //
            self.page_map
                .insert(table_id.to_string(), vec![-1 as i64; page_id + 1]);
        }
        match self.page_map.get_mut(table_id) {
            Some(table_index_vec) => {
                if table_index_vec.len() <= page_id {
                    table_index_vec.resize(page_id + 1, -1);
                }
                table_index_vec[page_id] = cache_id as i64;
            }
            None => {
                panic!("Error in cache page_map operation!")
            }
        }
    }

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
    pub fn get_cache_by_id(&mut self, cache_id: i64) -> Option<&mut [u8; 4096]> {
        if cache_id >= 0 && (cache_id as usize) < PAGE_NUM {
            // valid cache
            Some(&mut self.data[cache_id as usize])
        } else {
            None
        }
    }
}
