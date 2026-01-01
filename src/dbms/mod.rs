// NOTE:
// dmbs的抽象层级：
// create table/index, insert item, delete item
use colored::Colorize;
use hex::ToHex;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions, read_to_string},
    io::Write,
    path::Path,
};

use crate::{
    config::DATA_DIR,
    dbms::cache::{CacheBuf, Page},
    table::{
        TableMetaData,
        page::{
            TablePage,
            record::{ColumnType, ColumnValue, RecordId, RecordItem},
        },
    },
};

pub mod cache;
pub mod resource;

const PAGE_NUM: usize = 3;
const PAGE_SIZE: usize = 4096;

pub struct RecordItemGuard<'a> {
    page: TablePage<'a>,
    pub item: RecordItem<'a>,
}

pub struct DBMS<const PAGE_NUM: usize> {
    pub db_io: CacheBuf<PAGE_NUM>,
    metadata_map: HashMap<String, TableMetaData>, // record  the meta info of a table
    global_path: String,
    base_path: String,
}

impl<const PAGE_NUM: usize> DBMS<PAGE_NUM> {
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
        let column_size = table_metadata.const_info.column_type.len();
        if index_of_col >= column_size {
            return Err(format!(
                "Cannot create index {} on a table with only {} columns",
                index_of_col + 1,
                column_size
            ));
        }
        // check if that index have been created.
        if table_metadata.mut_info.column_index.contains(&index_of_col) {
            return Err(format!("Index on that column already exist"));
        }

        // create index
        // extra info: column index
        let extra_info = &index_of_col.to_string();
        self.db_io
            .create_file(name, &resource::PageType::INDEX, extra_info);
        table_metadata.mut_info.column_index.push(index_of_col);
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
        if !table_metadata.mut_info.column_index.contains(&index_of_col) {
            return Err(format!("Index on that column doesn't exist"));
        }

        // extra info: column index
        let extra_info = &index_of_col.to_string();
        self.db_io
            .delete_file(name, &resource::PageType::INDEX, extra_info);
        table_metadata.mut_info.column_index.remove(index_of_col);
        self.update_meta_json(&self.metadata_map);
        return Ok(());
    }

    // =============== table item =================
    pub fn insert_item(&mut self, name: &str, data: Vec<u8>) -> Result<RecordId, String> {
        // if no enough free slots, allocate new page

        // TODO: may need to load if not exist
        let table_metadata = self.metadata_map.get_mut(name).unwrap();

        // if no free space, then allocate new page
        if table_metadata.mut_info.next_free_slot == RecordId::NIL {
            let new_page_id = table_metadata.mut_info.data_page_count;
            let new_page = self
                .db_io
                .get_page(name, new_page_id, &resource::PageType::TABLE, "");
            // always set new page as dirty
            new_page.set_dirty();

            let new_data_page = &mut TablePage::new(&table_metadata.const_info, &mut new_page.data);

            // init page
            let page_item_capacity = new_data_page.const_metadata.page_item_capacity;
            let page_rid_start = table_metadata.mut_info.data_page_count * page_item_capacity;
            // NOTE: bump data_page_count after above two steps
            table_metadata.mut_info.data_page_count += 1;
            // TIP:
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

                let mut init_item = new_data_page.get_item(single_item_id);
                init_item.item_data.fill(0);
                init_item.set_next_free_slot(next_free_slot);
            }

            // 将table_metadata中的第一个free_slot设置为新页的开头位置
            table_metadata.mut_info.next_free_slot = RecordId(page_rid_start as u32);
        }

        // ensure that current table has free_slot
        assert_ne!(table_metadata.mut_info.next_free_slot, RecordId::NIL);

        // get the according page by table_metadata.next_free_slot
        let rid = table_metadata.mut_info.next_free_slot;
        let rid_value: u32 = rid.get();

        // NOTE:
        // xxx.table只包含页数据，目前的计划是把metadata存放在global下的json中
        // 如果后续信息较多，可以考虑增加新的文件
        let page_id = rid_value / table_metadata.const_info.page_item_capacity as u32;
        let item_id = rid_value % table_metadata.const_info.page_item_capacity as u32;

        // get the page with free slot, and set it as dirty
        let page_with_free_slot =
            self.db_io
                .get_page(name, page_id as usize, &resource::PageType::TABLE, "");
        page_with_free_slot.set_dirty();
        // TODO: 检查 DataPage类型中关于dirty中是否有重复的设置
        let data_page_with_free_slot =
            &mut TablePage::new(&table_metadata.const_info, &mut page_with_free_slot.data);

        // get the free slot, read its "next_free_slot"
        // update the "next_free_slot" in table header
        let mut free_item = data_page_with_free_slot.get_item(item_id as usize);
        table_metadata.mut_info.next_free_slot = free_item.get_next_free_slot();
        free_item.set_next_free_slot(RecordId::NIL);
        free_item.item_data.copy_from_slice(&data);

        // mark the slot as busy
        data_page_with_free_slot.set_slot_busy(item_id as usize);

        Ok(rid)
    }
    pub fn delete_item(&mut self, name: &str, rid: RecordId) -> Result<(), String> {
        // check if the rid is out-of-range
        let table_metadata = self.metadata_map.get_mut(name).unwrap();
        assert!(
            table_metadata.mut_info.data_page_count * table_metadata.const_info.page_item_capacity
                > rid.get() as usize
        );

        // get target page
        let rid_value: u32 = rid.get();
        let page_id = rid_value / table_metadata.const_info.page_item_capacity as u32;
        let item_id = rid_value % table_metadata.const_info.page_item_capacity as u32;
        // TODO: 后续将一些usize接口转换成u32
        let target_page =
            self.db_io
                .get_page(name, page_id as usize, &resource::PageType::TABLE, "");

        let target_data_page =
            &mut TablePage::new(&table_metadata.const_info, &mut target_page.data);

        // now set table_metadata.next_free_slot point to this new free slot.
        let mut target_item = target_data_page.get_item(item_id as usize);
        target_item.set_next_free_slot(table_metadata.mut_info.next_free_slot);
        table_metadata.mut_info.next_free_slot = rid;

        Ok(())
    }

    // item read/modify
    pub fn read_item_all(&mut self, name: &str, rid: RecordId) -> Result<Vec<ColumnValue>, String> {
        let f = |item: &mut RecordItem, metadata: &TableMetaData| {
            let mut item_all_value: Vec<ColumnValue> = Vec::new();
            for i in 0..metadata.const_info.column_count {
                let col_value = item.get_column_val(i).unwrap();
                item_all_value.push(col_value);
            }
            Ok(item_all_value)
        };
        self.with_item(name, rid, false, f)
    }
    pub fn read_item_col(
        &mut self,
        name: &str,
        rid: RecordId,
        col_name: &str,
    ) -> Result<ColumnValue, String> {
        let f = |item: &mut RecordItem, metadata: &TableMetaData| {
            let col_index = match metadata
                .const_info
                .column_name
                .iter()
                .position(|s| s == col_name)
            {
                Some(x) => x,
                None => {
                    return Err("Invalid Colume name".to_string());
                }
            };

            item.get_column_val(col_index)
        };
        self.with_item(name, rid, false, f)
    }

    pub fn write_item_col(
        &mut self,
        name: &str,
        rid: RecordId,
        col_name: &str,
        col_val: ColumnValue,
    ) -> Result<(), String> {
        let f = |item: &mut RecordItem, metadata: &TableMetaData| {
            let col_index = match metadata
                .const_info
                .column_name
                .iter()
                .position(|s| s == col_name)
            {
                Some(x) => x,
                None => {
                    return Err("Invalid Colume name".to_string());
                }
            };

            item.set_column_val(col_index, col_val)
        };
        self.with_item(name, rid, false, f)
    }

    // NOTE:
    // 用闭包来实现代码复用（有些情况如果用普通函数来实现代码复用，会引入生命周期和所有权的问题，但是闭包不会引入函数“return”带来的生命周期问题)
    // with_item仅用于对item的读写操作，不会修改bitmap
    fn with_item<R>(
        &mut self,
        name: &str,
        rid: RecordId,
        set_dirty: bool,
        f: impl FnOnce(&mut RecordItem, &TableMetaData) -> Result<R, String>,
    ) -> Result<R, String> {
        let table_metadata = self.metadata_map.get_mut(name).unwrap();

        let rid_value = rid.get();
        let page_id = rid_value / table_metadata.const_info.page_item_capacity as u32;
        let item_id = rid_value % table_metadata.const_info.page_item_capacity as u32;

        let target_page =
            self.db_io
                .get_page(name, page_id as usize, &resource::PageType::TABLE, "");

        if set_dirty {
            target_page.set_dirty();
        }

        let mut table_page = TablePage::new(&table_metadata.const_info, &mut target_page.data);

        let mut item = table_page.get_item(item_id as usize);
        f(&mut item, table_metadata)
    }

    // DEBUG:
    // print debug info
    // show the metadata of a table
    pub fn show_table_metadata(&self, name: &str) {
        let table_metadata = self.metadata_map.get(name).unwrap();
        println!("--------------------------");
        println!("{}: {}", "TableName".blue().bold(), name);
        println!(
            "{}: {}",
            "data_page_count".purple(),
            table_metadata.mut_info.data_page_count
        );
        println!(
            "{}: {}",
            "item_size".yellow(),
            table_metadata.const_info.item_size
        );
        println!(
            "{}: {}",
            "page_item_capacity".green(),
            table_metadata.const_info.page_item_capacity
        );
        println!(
            "{}: {}",
            "next_free_slot".red(),
            table_metadata.mut_info.next_free_slot.get()
        );
        println!("--------------------------");
    }

    pub fn show_next_free_slot(&self, name: &str) {
        let table_metadata = self.metadata_map.get(name).unwrap();
        println!(
            "{}: {}",
            "next_free_slot".red(),
            table_metadata.mut_info.next_free_slot.get()
        );
        println!("--------------------------");
    }
    pub fn show_table_page(&mut self, name: &str, page_id: usize) {
        let table_metadata = self.metadata_map.get(name).unwrap();
        let page_capacity = table_metadata.const_info.page_item_capacity;

        let target_page =
            self.db_io
                .get_page(name, page_id as usize, &resource::PageType::TABLE, "");

        // lazy delete
        let target_data_page =
            &mut TablePage::new(&table_metadata.const_info, &mut target_page.data);

        println!("<###############################");
        println!("{}: {}", "TableName".blue().bold(), name);
        println!("{}: {}", "Page".yellow().bold(), page_id);
        for i in 0..page_capacity {
            println!("-------- slot {} --------", i.to_string().purple());
            let validate = if target_data_page.check_slot_stat(i) {
                1
            } else {
                0
            };
            println!("validate: {}", validate);

            let item = target_data_page.get_item(i);
            let item_data = &item.item_data;
            println!("data: {}", item_data.encode_hex::<String>());
            let item_info = &item.get_next_free_slot().get();
            println!("next_free_slot: {:x}", item_info);
        }
        println!("###############################>");
    }
}
