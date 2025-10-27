use std::collections::HashMap;

use crate::config::CONFIG;
use bitvec::{order::Lsb0, vec::BitVec};

struct CacheBuf<const PAGE_NUM: usize> {
    page_map: HashMap<String, Vec<i64>>,
    valid: BitVec<usize, Lsb0>,
    dirty: BitVec<usize, Lsb0>,
    data: Vec<[u8; 4096]>,
}

impl<const PAGE_NUM: usize> CacheBuf<PAGE_NUM> {
    pub fn new() -> Self {
        CacheBuf {
            page_map: HashMap::new(),
            data: vec![[0u8; 4096]; PAGE_NUM],
            valid: BitVec::<usize, Lsb0>::repeat(false, PAGE_NUM),
            dirty: BitVec::<usize, Lsb0>::repeat(false, PAGE_NUM),
        }
    }
    fn get_cache_index(&mut self, table_id: &str, page_id: usize) -> Option<usize> {
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
    fn set_cache_index(&mut self, table_id: &str, page_id: usize, cache_id: usize) {
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

    fn find_free(&self) -> Option<usize> {
        self.valid.iter().position(|bit| !bit)
    }
    fn set_free(&mut self, index: usize) {
        self.valid.set(index, false);
    }
    fn set_busy(&mut self, index: usize) {
        self.valid.set(index, true);
    }
    fn is_dirty(&self, index: usize) -> bool {
        return self.dirty[index];
    }
    fn set_dirty(&mut self, index: usize) {
        self.dirty.set(index, true);
    }
    fn set_clean(&mut self, index: usize) {
        self.dirty.set(index, false);
    }

    fn get_page_cache(&mut self, table_id: &str, page_id: i64) -> &mut [u8; 4096] {
        // if in cache, then return the ref of it

        return match self.get_cache_index(table_id, page_id as usize) {
            Some(cache_index) => &mut self.data[cache_index],
            None => {
                match self.find_free() {
                    // free cache page
                    Some(index) => {
                        self.set_busy(index);
                        self.set_clean(index);
                        self.set_cache_index(table_id, page_id as usize, index);
                        // load file into cache page
                    }
                    // replace an existing page
                    None => {}
                }
            }
        };
    }
}
