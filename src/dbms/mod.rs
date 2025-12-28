// NOTE:
// dmbs的抽象层级：
// create table/index, insert item, delete item
use std::{
    collections::HashMap,
    fs::{File, OpenOptions, read_to_string},
    io::Write,
    path::Path,
};

use crate::{
    config::DATA_DIR,
    dbms::cache::CacheBuf,
    table::{
        TableMetaData,
        page::{
            TablePage,
            record::{ColumnType, RecordId, RecordItem},
        },
    },
};

pub mod cache;
pub mod resource;

const PAGE_NUM: usize = 3;
const PAGE_SIZE: usize = 4096;

pub struct DBMS<const PAGE_NUM: usize, const PAGE_SIZE: usize> {
    pub db_io: CacheBuf<PAGE_NUM, PAGE_SIZE>,
    metadata_map: HashMap<String, TableMetaData>, // record  the meta info of a table
    global_path: String,
    base_path: String,
}

impl<const PAGE_NUM: usize, const PAGE_SIZE: usize> DBMS<PAGE_NUM, PAGE_SIZE> {
    pub fn new() -> Self {
        // 1. create ./base ./global, if doesn't exist
        std::fs::create_dir_all(DATA_DIR.to_string() + "/global")
            .expect("Error: cannot create directory:  global");
        std::fs::create_dir_all(DATA_DIR.to_string() + "/base")
            .expect("Error: cannot create directory:  base");
        // global metactl.json
        let map_path = DATA_DIR.to_string() + "/global/metactl.json";
        if !Path::new(&map_path).exists() {
            let mut file = File::create(map_path).unwrap();
            file.write_all(b"{}").unwrap();
        }

        let metadata = serde_json::from_str(
            &read_to_string(DATA_DIR.to_string() + "/global/metactl.json").unwrap(),
        )
        .expect("metactl.json file format incorrect!");

        let global_path = DATA_DIR.to_string() + "/global/";
        let base_path = DATA_DIR.to_string() + "/base/";

        DBMS {
            db_io: CacheBuf::new(&global_path, &base_path),
            metadata_map: metadata,
            global_path: global_path,
            base_path: base_path,
            //
        }
    }
    // update global/metactl.json
    fn update_meta_json(&self, metactl_data: &HashMap<String, TableMetaData>) {
        let metajson_path = self.global_path.clone() + "metactl.json";
        let mut metajson_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(metajson_path)
            .expect("Cannot open metactl.json for writing!");
        let json_str = serde_json::to_string_pretty(metactl_data)
            .expect("Cannot convert current map to string.");
        metajson_file
            .write_all(json_str.as_bytes())
            .expect("Failed in writing to TableMap.json!");
        println!("{}", json_str);
    }

    pub fn create_table(
        &mut self,
        name: &str,
        column_type: Vec<&ColumnType>,
        column_name: Vec<&str>,
    ) -> Result<(), String> {
        // if file already exist, then report an error
        if self.metadata_map.contains_key(name) {
            return Err(format!("Table {} already exist.", name));
        }
        // calculate the item size
        // NOTE: 在这里需要加上末尾的辅助信息大小
        let mut item_size: usize = 4; // plus the size of RecordId
        for col_type in &column_type {
            item_size += col_type.size();
        }
        // crate file, no extra info
        self.db_io.create_file(name, &resource::PageType::TABLE, "");
        let new_table_metadata = TableMetaData::new(
            name,
            0,             // data page count: 0 at start
            RecordId::NIL, // first free page: NULL at start
            item_size,
            column_type.len(),
            column_name,
            column_type,
            Vec::new(),
        );
        self.metadata_map
            .insert(name.to_string(), new_table_metadata);
        self.update_meta_json(&self.metadata_map);
        return Ok(());
    }

    pub fn create_index(&mut self, name: &str, index_of_col: usize) -> Result<(), String> {
        // check if the table exist
        if !self.metadata_map.contains_key(name) {
            return Err(format!("Table {} doesn't exist.", name));
        }

        let table_metadata = self.metadata_map.get_mut(name).unwrap();
        // check if the index is illeagal (doesn't exist / exceed bound)
        let column_size = table_metadata.column_type.len();
        if index_of_col >= column_size {
            return Err(format!(
                "Cannot create index {} on a table with only {} columns",
                index_of_col + 1,
                column_size
            ));
        }
        // check if that index have been created.
        if table_metadata.column_index.contains(&index_of_col) {
            return Err(format!("Index on that column already exist"));
        }

        // create index
        // extra info: column index
        let extra_info = &index_of_col.to_string();
        self.db_io
            .create_file(name, &resource::PageType::INDEX, extra_info);
        table_metadata.column_index.push(index_of_col);
        self.update_meta_json(&self.metadata_map);
        return Ok(());
    }
    // delete a table file
    pub fn delete_table(&mut self, name: &str) -> Result<(), String> {
        // check if there is exist file
        if !self.metadata_map.contains_key(name) {
            return Err(format!("Table {} doesn't exist.", name));
        }
        // crate file, no extra info
        self.db_io.delete_file(name, &resource::PageType::TABLE, "");
        self.metadata_map.remove(name);
        self.update_meta_json(&self.metadata_map);
        return Ok(());
    }

    pub fn delete_index(&mut self, name: &str, index_of_col: usize) -> Result<(), String> {
        // check if there is exist file
        if !self.metadata_map.contains_key(name) {
            return Err(format!("Table {} doesn't exist.", name));
        }
        // check if the index exist
        let table_metadata = self.metadata_map.get_mut(name).unwrap();

        // check if that index have been created.
        if !table_metadata.column_index.contains(&index_of_col) {
            return Err(format!("Index on that column doesn't exist"));
        }

        // extra info: column index
        let extra_info = &index_of_col.to_string();
        self.db_io
            .delete_file(name, &resource::PageType::INDEX, extra_info);
        table_metadata.column_index.remove(index_of_col);
        self.update_meta_json(&self.metadata_map);
        return Ok(());
    }

    // =============== table item =================
    pub fn insert_item(&mut self, name: &str, data: Vec<u8>) {
        // NOTE:
        // if no enough free slots, allocate new page
        // current page count: x, then allocate page(x+1) (page 0 is reserved)

        // TODO: may need to load if not exist
        let table_metadata = self.metadata_map.get_mut(name).unwrap();

        // if no free space, then allocate new page
        if table_metadata.next_free_slot == RecordId::NIL {
            let new_page_id = table_metadata.data_page_count + 1;
            let new_page = self
                .db_io
                .get_page(name, new_page_id, &resource::PageType::TABLE, "");
            // always set new page as dirty
            new_page.set_dirty();

            let new_data_page = &mut TablePage::new(table_metadata.item_size, &mut new_page.data);

            // init page
            let page_item_capacity = new_data_page.item_num;
            let page_rid_start = table_metadata.data_page_count * page_item_capacity;
            // SOME:
            // [1..10].iter() ---> 1..10   [1..5] <----> [1..=4]
            for single_item_id in 0..page_item_capacity {
                let next_free_slot: RecordId;
                if single_item_id == page_item_capacity - 1 {
                    next_free_slot = RecordId::NIL;
                } else {
                    next_free_slot = RecordId((page_rid_start + single_item_id + 1) as u32);
                }
                // append returns (). it only modify the value
                // NOTE:
                // col1 | col2 | ... | item_info(is_null, next_free_slot)

                let init_record_item =
                    RecordItem::new(vec![0u8; table_metadata.item_size], next_free_slot);

                new_data_page.write_item(single_item_id, init_record_item);
            }

            // 将table_metadata中的第一个free_slot设置为新页的开头位置
            table_metadata.next_free_slot = RecordId(page_rid_start as u32);
        }

        // ensure that current table has free_slot
        assert_ne!(table_metadata.next_free_slot, RecordId::NIL);

        // get the according page by table_metadata.next_free_slot
        let rid: u32 = table_metadata.next_free_slot.get().unwrap();

        let page_id = rid / table_metadata.page_item_capacity as u32;
        let item_id = rid % table_metadata.page_item_capacity as u32;

        let page_with_free_slot =
            self.db_io
                .get_page(name, page_id as usize, &resource::PageType::TABLE, "");
        let data_page_with_free_slot =
            &mut TablePage::new(table_metadata.item_size, &mut page_with_free_slot.data);
        let free_item = data_page_with_free_slot.read_item(item_id as usize);
        // set new "next_free_slot"
        table_metadata.next_free_slot = free_item.next_free_slot;

        let new_item = RecordItem::new(data, RecordId::NIL);
        data_page_with_free_slot.write_item(item_id as usize, new_item);
    }
}
