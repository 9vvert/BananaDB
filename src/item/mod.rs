pub mod page;
pub mod table;

use crate::item::page::{TablePage, record::ColumnType};

pub struct MetaTable {
    column_count: usize,
    item_size: usize,
    col_type_list: Vec<ColumnType>,
    col_offset_list: Vec<usize>,
}

impl MetaTable {
    pub fn new(col_type_list: &Vec<ColumnType>) -> Self {
        // TODO:
        // read meta data from global.json
        let mut col_offset_list: Vec<usize> = Vec::new();
        let mut item_size: usize = 0;

        let mut offset: usize = 0;
        for col_type in col_type_list {
            col_offset_list.push(offset);
            offset += col_type.size(); // define in record.rs
            // calculate the length of a item
            item_size += col_type.size();
        }
        MetaTable {
            column_count: col_type_list.len(),
            item_size: item_size,
            col_type_list: col_type_list.clone(),
            col_offset_list: col_offset_list,
        }
    }
}

pub struct TableManager<const PAGE_SIZE: usize> {
    metainfo: MetaTable,
}

impl<const PAGE_SIZE: usize> TableManager<PAGE_SIZE> {
    pub fn new(&mut self, col_type_list: &Vec<ColumnType>) -> Self {
        TableManager {
            metainfo: MetaTable::new(col_type_list),
        }
    }
    pub fn get_item(table_page: TablePage<PAGE_SIZE>) -> Vec<u8> {}
}
