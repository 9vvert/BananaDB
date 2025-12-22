// cache system
// 接受table_id, page_id, 封装所有和cache page有关的细节
mod file_system;
mod lru_list;

use bitvec::{order::Lsb0, vec::BitVec};
use std::{collections::HashMap, fs::File};

use file_system::FileManager;
use lru_list::LruList;

use crate::dbms::{
    PAGE_NUM,
    resource::{PageType, ResId},
};

// ==================== Page ======================
// NOTE:
// 最初的做法中，没有Page这一层抽象，将dirty的控制交给cache system，导致封装不够优雅
// 现在将带有dirty标记的Page返回,方便控制
#[derive(Clone)]
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
    // pages: [Page<PAGE_SIZE>; PAGE_NUM],
    pages: Vec<Page<PAGE_SIZE>>,
    lru_list: LruList<PAGE_NUM>,
}

impl<const PAGE_NUM: usize, const PAGE_SIZE: usize> CacheBuf<PAGE_NUM, PAGE_SIZE> {
    pub fn new(global_path: &str, base_path: &str) -> Self {
        CacheBuf {
            // io
            file_sys: FileManager::new(global_path, base_path),
            opened_file: HashMap::new(),
            // cache
            cache_map: HashMap::new(),
            reverse_map: HashMap::new(),
            pages: vec![Page::<PAGE_SIZE>::new(); PAGE_NUM],
            lru_list: LruList::<PAGE_NUM>::new(),
        }
    }

    //============ Public IO ================
    pub fn get_page(
        &mut self,
        file_name: &str,
        page_id: usize,
        page_type: &PageType,
        extra_info: &str,
    ) -> &mut Page<PAGE_SIZE> {
        let res_id = ResId::new(page_type, file_name, page_id, extra_info);

        let file_path = ResId::gen_file_path(file_name, page_type, extra_info);
        // first: query if there is cache.
        // assume that the borrowd cache is dropped at once.
        match self.query_cache_index(&res_id) {
            // cache hit
            Some(cache_id) => self.get_cache_resource(cache_id),
            // cache miss
            None => {
                // request for data by ftile_sys
                if !self.opened_file.contains_key(&file_path) {
                    // NOTE:
                    // the key of opened_file is path, not base name
                    println!("file:");
                    println!("{}", file_path);
                    let new_fd = self
                        .file_sys
                        .open_file(&file_name, page_type, extra_info)
                        .unwrap();
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

    // ================ Public Create ==================
    // pass
    pub fn create_file(&mut self, file_name: &str, page_type: &PageType, extra_info: &str) {
        // TODO:
        // detect error
        self.file_sys
            .create_file(file_name, page_type, extra_info)
            .unwrap();
    }
    pub fn delete_file(&mut self, file_name: &str, page_type: &PageType, extra_info: &str) {
        // TODO:
        // detect error
        self.file_sys.delete_file(file_name, page_type, extra_info);
    }

    // ================ Private function ===============
    // to identify if current cache buffer has such resource
    // if yes, then return cache id; otherwise return None
    fn query_cache_index(&self, res_id: &ResId) -> Option<usize> {
        if self.cache_map.contains_key(res_id) {
            Some(self.cache_map[res_id])
        } else {
            None
        }
    }

    // get cache by id
    fn get_cache_resource(&mut self, cache_id: usize) -> &mut Page<PAGE_SIZE> {
        if cache_id >= PAGE_NUM {
            panic!("Invalid cache id {}, current max is {}", cache_id, PAGE_NUM);
        }
        // TODO:
        // set the used cahce page to list head
        self.lru_list.lift_page(cache_id).unwrap();
        &mut self.pages[cache_id]
    }

    // input: buffer
    // then add data to cache
    // NOTE:
    // the buffer here is NOT ref, this is just a test
    // trying to move directly, aiming to reduce the cost of copy
    fn add_cache_resource(&mut self, res_id: &ResId, buffer: [u8; 4096]) {
        if self.query_cache_index(res_id).is_some() {
            panic!("Cache leak: trying load a page data twice!");
        }
        let cache_id: usize;
        // query if the cache is full
        if self.lru_list.have_free_page() {
            cache_id = self.lru_list.new_page().unwrap();
        } else {
            cache_id = self.lru_list.get_drop_page().unwrap();

            // INFO:
            // 如果交换write_back和old_res_id的位置，会出现错误

            // write back, if dirty
            self.write_back_page(cache_id);
            // delete old map item
            let old_res_id = self
                .reverse_map
                .get(&cache_id)
                .expect("Fatal! Mismatch cache_map and reverse_map!");
            // get file by ResId
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

    // drop a certain page
    fn write_back_page(&mut self, cache_id: usize) {
        let res_id = self
            .reverse_map
            .get(&cache_id)
            .expect("Fatal! Mismatch cache_map and reverse_map!");

        let drop_page: &Page<PAGE_SIZE> = &self.pages[cache_id];
        if drop_page.dirty {
            let resid_parts = res_id.break_resid();
            let wb_file_type = resid_parts.0;
            let wb_file_name = resid_parts.1;
            let wb_page_id = resid_parts.2;
            let wb_extra_info = resid_parts.3;
            let mut wb_fd = self
                .file_sys
                .open_file(&wb_file_name, &wb_file_type, &wb_extra_info)
                .unwrap();
            self.file_sys
                .write_page(&mut wb_fd, wb_page_id, &drop_page.data)
                .unwrap();
            // INFO:
            // 如果一个变量是mut类型，想要使用可变引用，必须显示用&mut x，而不是仅仅 &x
            // 但是如果变量本身是 &mut类型，则可以直接用 x
        }
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

// ============== write back ==========
impl<const PAGE_NUM: usize, const PAGE_SIZE: usize> Drop for CacheBuf<PAGE_NUM, PAGE_SIZE> {
    fn drop(&mut self) {
        // Vec<Page> doesn't implement "Copy", so cannot move directly
        // use reference instead

        // INFO:
        // 如果使用for dirty_page in self.pages.iter().enumerate(),
        // 会导致所有权的引用问题（迭代的时候获得&mut self, 而下面self.write_back_page还需要）
        // 一个优雅的解决方法是：将迭代和write_back的时序分离开,这样就不会出现交错所有权引用的冲突
        // 不过，其实还是没有完全发挥“安全”的空间，如果要使用可变引用的两个成员可以保证互不干扰，但是编译器无法知晓，还是会报错
        // 如果时序上也无法做到完全不重叠，该怎么办？出了改变has接口，有没有简洁的实现方法？
        let dirty_page_id: Vec<usize> = self
            .pages
            .iter()
            .enumerate()
            .filter_map(|(i, p)| if p.dirty { Some(i) } else { None })
            .collect();

        for cache_id in dirty_page_id {
            self.write_back_page(cache_id);
        }
    }
}
