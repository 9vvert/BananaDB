// define all possible columns

use std::task::ready;

use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
// SOME:
// need "pub" here. or cannot
pub struct RecordId(pub u32);

impl RecordId {
    pub const NIL: Self = Self(u32::MAX);

    pub fn new(v: u32) -> Self {
        Self(v)
    }

    pub fn is_nil(self) -> bool {
        self.0 == u32::MAX
    }

    pub fn get(self) -> Option<u32> {
        if self.is_nil() { None } else { Some(self.0) }
    }
    pub fn from_le_bytes(bytes: [u8; 4]) -> Self {
        let v = u32::from_le_bytes(bytes);
        Self(v)
    }

    pub fn to_le_bytes(self) -> [u8; 4] {
        self.0.to_le_bytes()
    }
}

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
