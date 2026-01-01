use std::fmt::{self, Display};

use serde::{Deserialize, Serialize};

use crate::config::DATA_DIR;

#[derive(PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub enum PageType {
    TABLE = 0,
    INDEX = 1,
}

impl Display for PageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::TABLE => "table",
            Self::INDEX => "index",
        };
        write!(f, "{s}")
    }
}

#[derive(Hash, PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct ResId {
    resource_id: String,
}

impl ResId {
    pub fn new(page_type: &PageType, file_name: &str, page_id: usize, extra: &str) -> Self {
        let extra_str: &str = if extra.is_empty() {
            "@"
        } else {
            &("@".to_string() + extra)
        };
        // INFO: 这里使用 & 可以，但是 .as_str() 无法正常编译

        let res_id: String = match page_type {
            PageType::TABLE => {
                "T-".to_string() + file_name + "-" + page_id.to_string().as_str() + extra_str
            }
            PageType::INDEX => {
                "I-".to_string() + file_name + "-" + page_id.to_string().as_str() + extra_str
            }
        };

        ResId {
            resource_id: res_id,
        }
    }
    pub fn gen_file_path(file_name: &str, file_type: &PageType, extra: &str) -> String {
        // XXX:
        // @ 在测试机器上是否是合法文件字符?
        let extra_str: &str = if extra.is_empty() {
            "@"
        } else {
            &("@".to_string() + extra)
        };
        // INFO:
        // impl Display for enum type, then using to_string
        let dir_path = DATA_DIR.to_string() + "/base/" + file_name;
        let file_path = dir_path + "/" + file_name + extra_str + "." + &file_type.to_string();
        file_path
    }

    // PageType, filename, pageid, extra
    pub fn break_resid(&self) -> (PageType, String, usize, String) {
        let mut level1_parts = self.resource_id.split('@');

        let prefix_part: String = level1_parts.next().unwrap().to_string();
        let extra_part: String = level1_parts.next().unwrap().to_string();

        let mut parts = prefix_part.split('-');
        let file_type = match parts.next() {
            Some("T") => PageType::TABLE,
            Some("I") => PageType::INDEX,
            _ => {
                panic!("Unknown ResId prefix")
            }
        };

        let file_name = parts.next().unwrap().to_string();

        let page_id = parts.next().unwrap().parse().unwrap();

        (file_type, file_name, page_id, extra_part)
    }
}
