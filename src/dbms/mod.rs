use crate::dbms::cache::CacheBuf;

pub mod cache;
pub mod resource;

const PAGE_NUM: usize = 3;
const PAGE_SIZE: usize = 4096;

pub struct DBMS<const PAGE_NUM: usize, const PAGE_SIZE: usize> {
    pub db_io: CacheBuf<PAGE_NUM, PAGE_SIZE>,
}

impl<const PAGE_NUM: usize, const PAGE_SIZE: usize> DBMS<PAGE_NUM, PAGE_SIZE> {
    pub fn new() -> Self {
        DBMS {
            db_io: CacheBuf::new(),
        }
    }
}
