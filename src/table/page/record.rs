// define all possible columns

use std::{task::ready, usize};

use serde::{Deserialize, Serialize};

pub const ITEM_INFO_LEN: usize = 4;

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
pub struct RecordItem {
    pub item_data: Vec<u8>,
    pub next_free_slot: RecordId,
}

impl RecordItem {
    pub fn new(item_data: Vec<u8>, next_free_slot: RecordId) -> Self {
        RecordItem {
            item_data: item_data,
            next_free_slot: next_free_slot,
        }
    }

    // directly from vector<u8>
    // SOME: 需要在变量前面声明mut，而不是type里
    pub fn from_raw(mut total_data: Vec<u8>) -> Self {
        let total_len = total_data.len();
        // SOME: Move the last ITEM_INFO_LEN bytes out into a new Vec<u8> (no element copy of the prefix)
        let info_bytes: Vec<u8> = total_data.split_off(total_len - ITEM_INFO_LEN);
        // Now total_data is the prefix (item_data), moved/owned
        let item_data = total_data;

        // Convert &[u8] -> [u8; ITEM_INFO_LEN]
        let info_arr: [u8; ITEM_INFO_LEN] =
            info_bytes.as_slice().try_into().expect("wrong info length");

        // SOME: [u8; 4]是在[u8]的基础上进行了大小的强制要求,使用try_into()进行转换
        let rid_info: &[u8; 4] = &info_arr[..4].try_into().unwrap();
        let next_free_slot = RecordId::from_le_bytes(*rid_info);

        Self {
            item_data,
            next_free_slot,
        }
    }

    // SOME: 参数使用 self 来将自身移动,防止拷贝。但是这回导致自身被销毁
    pub fn move_to_bytes(self) -> Vec<u8> {
        let item_info = self.next_free_slot.to_le_bytes().to_vec();
        let mut item_data = self.item_data;
        item_data.extend(item_info);
        item_data
    }
}
