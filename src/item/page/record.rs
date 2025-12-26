// define all possible columns

use std::task::ready;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Deserialize, Serialize)]
pub enum ColumnType {
    INT,
    CHAR(usize),
}

impl ColumnType {
    pub fn size(&self) -> usize {
        match self {
            Self::INT => 4,
            Self::CHAR(x) => *x, // INFO: need dereference
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub enum ColumnValue {
    INT(i32),
    CAHR(Vec<u8>),
}

// impl ColumnValue {
//     pub fn new(&mut self) -> Self {
//         // match self {}
//     }
// }

// TODO:
// record item
pub struct Record {
    type_list

}
