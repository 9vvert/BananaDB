#[derive(PartialEq, Eq, Hash)]
pub enum PageType {
    TABLE = 0,
    INDEX = 1,
}

#[derive(Hash, PartialEq, Eq, Clone)]
pub struct ResId {
    resource_id: String,
}

impl ResId {
    pub fn new(page_type: PageType, file_name: &str, page_id: usize) -> Self {
        let res_id: String = match page_type {
            PageType::TABLE => "T-".to_string() + file_name + "-" + page_id.to_string().as_str(),
            PageType::INDEX => "I-".to_string() + file_name + "-" + page_id.to_string().as_str(),
        };

        ResId {
            resource_id: res_id,
        }
    }
}
