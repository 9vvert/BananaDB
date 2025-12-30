// define all possible columns

use std::{ops::Index, task::ready, usize};

use serde::{Deserialize, Serialize};

pub const ITEM_INFO_LEN: usize = 4;

#[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
// TIP:
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

    pub fn get(self) -> u32 {
        self.0
    }
    pub fn from_le_bytes(bytes: [u8; 4]) -> Self {
        let v = u32::from_le_bytes(bytes);
        Self(v)
    }

    pub fn to_le_bytes(self) -> [u8; 4] {
        self.0.to_le_bytes()
    }
}

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
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
    CAHR(String),
}

// TODO: Constraint
#[derive(Clone, Debug)]
pub enum Constraint {
    NotNull,
    IntRange { min: i32, max: i32 },
    // 未来：CharCharset、Regex、Unique、...
}

pub struct RecordItem<'a> {
    pub item_data: &'a mut [u8],
    next_free_slot: &'a mut [u8; 4],
    column_type: &'a Vec<ColumnType>,
    column_offset: &'a Vec<usize>,
}

impl<'a> RecordItem<'a> {
    // TIP: 需要在变量前面声明mut，而不是type里
    pub fn new(
        total_data: &'a mut [u8],
        column_type: &'a Vec<ColumnType>,
        column_offset: &'a Vec<usize>,
    ) -> Self {
        let total_len = total_data.len();

        // TIP: total_data是一块字节引用，如果想要将其中两个引用分开，就需要使用split_at_mut
        let (item_data, item_info) = total_data.split_at_mut(total_len - ITEM_INFO_LEN);
        // TIP: use RecordId::xxx instead of RecordId.xxx
        // and there need "(&mut item_info[..4])", otherwise there will be priority issue
        let next_free_slot: &mut [u8; 4] = (&mut item_info[..4]).try_into().unwrap();

        // TIP: [u8; 4]是在[u8]的基础上进行了大小的强制要求,使用try_into()进行转换
        // let rid_info: &[u8; 4] = &info_arr[..4].try_into().unwrap();
        // let next_free_slot = RecordId::from_le_bytes(*rid_info);

        Self {
            item_data,
            next_free_slot,
            column_type,
            column_offset,
        }
    }

    pub fn set_next_free_slot(&mut self, nfs: RecordId) {
        // TIP: rust默认的 "=" 行为是移动，如果仅仅想要对其引用赋值，可以解引用
        *self.next_free_slot = nfs.to_le_bytes();
    }

    pub fn get_next_free_slot(&self) -> RecordId {
        // TIP: 这里如果直接使用 from_le_bytes(self.next_free_slot),
        // 也会有类型不匹配的问题(因为参数要求的不是引用，而是副本)，依旧需要解引用,将 &mut [u8; 4]转换成[u8; 4]
        RecordId::from_le_bytes(*self.next_free_slot)
    }

    pub fn get_column_val(&self, col_index: usize) -> Result<ColumnValue, String> {
        if col_index >= self.column_type.len() {
            return Err("Column out of range".to_string());
        }

        let target_offset = self.column_offset[col_index];
        let target_type = self.column_type[col_index];
        let target_size = target_type.size();

        // NOTE: impl a dynamic adapter for ColumnValue is hard.
        // so here use an easy way
        match target_type {
            ColumnType::INT => {
                let int_val: i32 = i32::from_le_bytes(
                    self.item_data[target_offset..target_offset + target_size]
                        .try_into()
                        .unwrap(),
                );
                Ok(ColumnValue::INT(int_val))
            }
            ColumnType::CHAR(_) => {
                let str_val: String = str::from_utf8(
                    self.item_data[target_offset..target_offset + target_size]
                        .try_into()
                        .unwrap(),
                )
                .unwrap()
                .to_string();
                Ok(ColumnValue::CAHR(str_val))
            }
        }
    }

    pub fn set_column_val(&mut self, col_index: usize, col_val: ColumnValue) -> Result<(), String> {
        if col_index >= self.column_type.len() {
            return Err("Column out of range".to_string());
        }

        let target_offset = self.column_offset[col_index];
        let target_type = self.column_type[col_index];
        let target_size = target_type.size();

        match col_val {
            ColumnValue::INT(x) => {
                if target_type != ColumnType::INT {
                    return Err("Column type mismatch".to_string());
                }
                let int_bytes: [u8; 4] = x.to_le_bytes();
                self.item_data[target_offset..target_offset + target_size]
                    .copy_from_slice(&int_bytes);

                Ok(())
            }
            ColumnValue::CAHR(s) => {
                // NOTE: must specify the char length of CHAR (also a part of "enum type")
                if target_type != ColumnType::CHAR(target_size) {
                    return Err("Column type mismatch".to_string());
                }
                let str_bytes: &[u8] = s.as_bytes();
                self.item_data[target_offset..target_offset + target_size]
                    .copy_from_slice(str_bytes);
                Ok(())
            }
        }
    }
}
