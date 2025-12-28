pub mod page;

use std::usize;

use crate::table::page::{
    TablePage,
    record::{ColumnType, RecordId},
};

// NOTE:
// MetaTable代表一个Table文件的元信息，通常从配置文件中读取
#[derive(serde::Deserialize, serde::Serialize)]
pub struct TableMetaData {
    pub table_name: String,
    pub data_page_count: usize,
    pub next_free_slot: RecordId,
    pub item_size: usize,    // size of each item
    pub column_count: usize, // column count
    pub column_name: Vec<String>,
    pub column_type: Vec<ColumnType>,
    pub column_offset: Vec<usize>, // offset of each column
    pub column_index: Vec<usize>,  // vector of built index
}

impl TableMetaData {
    pub fn new(
        table_name: &str,
        data_page_count: usize,
        next_free_slot: RecordId,
        //
        item_size: usize,
        column_count: usize,
        column_name: Vec<&str>,
        column_type: Vec<&ColumnType>,
        column_index: Vec<usize>,
    ) -> Self {
        // TODO:
        // read meta data from global.json
        let mut col_offset_list: Vec<usize> = Vec::new();
        let mut real_size: usize = 0;

        let mut offset: usize = 0;
        // SOME: borrow here
        for col_type in &column_type {
            col_offset_list.push(offset);
            offset += col_type.size(); // define in record.rs
            // calculate the length of a item
            real_size += col_type.size();
        }
        // check
        // assert_eq!(item_size, real_size);
        assert_eq!(column_count, column_type.len());
        assert_eq!(column_name.len(), column_type.len());
        //
        //NOTE:
        //实际的item_size可以大于各个column size之和，方便后续增加null等信息
        TableMetaData {
            table_name: table_name.to_string(),
            data_page_count: data_page_count,
            next_free_slot: next_free_slot,
            item_size: item_size,
            column_count: column_count,
            column_name: column_name.iter().map(|x| x.to_string()).collect(),
            column_type: column_type.into_iter().cloned().collect(),
            column_offset: col_offset_list,
            column_index: column_index,
        }
    }
}

// pub struct TableManager<const PAGE_SIZE: usize> {
//     metainfo: MetaTable,
// }
//
// // NOTE:
// // CacheBuf/dbms中提供函数：接受pagetype, filename, pageid, 得到page资源
// // 现在需要对得到的page进行新的操作：解释成DataPage,然后根据rid获得资源
//
// impl<const PAGE_SIZE: usize> TableManager<PAGE_SIZE> {
//     pub fn new(&mut self, col_type_list: &Vec<ColumnType>) -> Self {
//         TableManager {
//             metainfo: MetaTable::new(col_type_list),
//         }
//     }
//     // TODO:
//     //
//     pub fn get_item(table_page: TablePage<PAGE_SIZE>) -> Vec<u8> {}
// }
