#[derive(PartialEq, Eq, Hash)]
pub enum FileType {
    TABLE = 0,
    INDEX = 1,
}

#[derive(Hash, PartialEq, Eq)]
pub struct ResId {
    resource_id: String,
}

impl ResId {
    pub fn new(file_type: FileType, file_name: &str, page_id: usize) -> String {
        let res_id: String = match file_type {
            FileType::TABLE => "T-".to_string() + file_name + "-" + page_id.to_string().as_str(),
            FileType::INDEX => "I-".to_string() + file_name + "-" + page_id.to_string().as_str(),
        };
        res_id
    }
}
