// read meta-data from global.json

use crate::item::page::record::ColumnType;

pub struct MetaTable {
    col_type_list: Vec<ColumnType>,
    col_offset_list: Vec<usize>,
}

impl MetaTable {
    pub fn new(&mut self, col_type_list: &Vec<ColumnType>) -> Self {
        // TODO:
        // read meta data from global.json
        let mut col_offset_list: Vec<usize> = Vec::new();

        let mut offset: usize = 0;
        for col_type in col_type_list {
            col_offset_list.push(offset);
            offset += col_type.size(); // define in record.rs
        }
        MetaTable {
            col_type_list: col_type_list.clone(),
            col_offset_list: col_offset_list,
        }
    }
}
