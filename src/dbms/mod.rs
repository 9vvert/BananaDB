use std::{
    collections::HashMap,
    fs::{File, OpenOptions, read_to_string},
    io::Write,
    path::Path,
};

use crate::{config::DATA_DIR, dbms::cache::CacheBuf, item::record::ColumnType};

pub mod cache;
pub mod resource;

const PAGE_NUM: usize = 3;
const PAGE_SIZE: usize = 4096;

#[derive(serde::Deserialize, serde::Serialize)]
pub struct TableMetaData {
    name: String,
    column_type: Vec<ColumnType>,
    column_name: Vec<String>,
    index: Vec<usize>, // use which column
}

impl TableMetaData {
    pub fn new(
        name: &str,
        column_type: Vec<&ColumnType>,
        column_name: Vec<&str>,
        column_index: Vec<usize>,
    ) -> Self {
        assert_eq!(column_type.len(), column_name.len());

        Self {
            name: name.to_string(),
            // XXX:
            // why the map will cause lsp error? shouldn't these two ways be the same?
            // column_type: column_type.iter().map(|x| x.clone()).collect(),
            column_type: column_type.into_iter().cloned().collect(),
            column_name: column_name.iter().map(|x| x.to_string()).collect(),
            index: column_index,
        }
    }
}

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
        // crate file, no extra info
        self.db_io.create_file(name, &resource::PageType::TABLE, "");
        let new_table_metadata = TableMetaData::new(name, column_type, column_name, Vec::new());
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
        if table_metadata.index.contains(&index_of_col) {
            return Err(format!("Index on that column already exist"));
        }

        // create index
        // extra info: column index
        let extra_info = &index_of_col.to_string();
        self.db_io
            .create_file(name, &resource::PageType::INDEX, extra_info);
        table_metadata.index.push(index_of_col);
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
        if !table_metadata.index.contains(&index_of_col) {
            return Err(format!("Index on that column doesn't exist"));
        }

        // extra info: column index
        let extra_info = &index_of_col.to_string();
        self.db_io
            .delete_file(name, &resource::PageType::INDEX, extra_info);
        table_metadata.index.remove(index_of_col);
        self.update_meta_json(&self.metadata_map);
        return Ok(());
    }
}
