use std::error::Error;

pub mod cache_system;
pub mod file_system;

#[allow(non_camel_case_types)]
struct IO_Manager<const PAGE_NUM: usize> {
    cache_sys: cache_system::CacheBuf<PAGE_NUM>,
    file_sys: file_system::FileManager,
}

impl<'a, const PAGE_NUM: usize> IO_Manager<PAGE_NUM> {
    pub fn new() -> Self {
        IO_Manager {
            cache_sys: cache_system::CacheBuf::<PAGE_NUM>::new(),
            file_sys: file_system::FileManager::new(),
        }
    }
    pub fn create_table(&mut self, table_name: &str) {
        self.file_sys.new_table(table_name);
    }
    pub fn get_page(
        &mut self,
        table_id: &str,
        page_id: i64,
    ) -> Result<&mut [u8; 4096], Box<dyn Error>> {
        // if in cache, then return the ref of it
        match self.cache_sys.get_cache_index(table_id, page_id as usize) {
            // if there is an existing cache page for it
            Some(cache_id) => match self.cache_sys.get_cache_by_id(cache_id as i64) {
                Some(cache_page) => Ok(&mut cache_page),
                None => Err(format!("Invalid cache id {}", cache_id).into()),
            },
            // no cache page, allocate one
            None => {
                // find free page
                match self.cache_sys.find_free() {
                    // free cache page, then allocate at once
                    Some(index) => {
                        // TODO:
                        // load file into cache page

                        // set flag
                        self.cache_sys.set_busy(index);
                        self.cache_sys.set_clean(index);
                        self.cache_sys
                            .set_cache_index(table_id, page_id as usize, index as usize);

                        let mut buffer = match self.cache_sys.get_cache_by_id(index as i64) {
                            Some(cache_page) => &mut cache_page,
                            None => return Err("Failed to access a free page".into()),
                        };
                        self.file_sys.read_page(table_id, page_id as u32, buffer);
                        Ok(buffer)
                    }
                    None => {
                        // TODO:
                        // use LRU.  select an existing busy page.
                    }
                }
            }
        }
    }
}
