use std::{io, ops::BitAndAssign};

use bitvec::prelude::*;
use bytemuck::cast_slice;

use crate::table::page::record::RecordItem;
// 从IO_manager中申请到的页面缓存
// 初始化成为一个PageData结构体

pub mod record;

// TODO:
// 规划各个常量
pub const PAGE_SIZE: usize = 4096;
pub const TAIL_SIZE: usize = 64;
pub const BITMAP_SIZE: usize = 32;
pub const BITMAP_BIT_SIZE: usize = BITMAP_SIZE * 8;
// NOTE:
// Bitmap::from要求传入固定的[u128; 2]
pub struct TablePage<'a> {
    item_size: usize,
    pub item_num: usize, // table item capacity
    // data
    pub data: &'a mut [u8; PAGE_SIZE - TAIL_SIZE],
    // metadata of a data page
    slot_bitmap: &'a mut BitSlice<u8, Lsb0>,
}

impl<'a> TablePage<'a> {
    // TODO:
    // 目前这里存在拷贝
    pub fn new(item_size: usize, page_data: &'a mut [u8; PAGE_SIZE]) -> Self {
        let (items_data, bitmap_data) = page_data.split_at_mut(PAGE_SIZE - TAIL_SIZE);

        TablePage {
            item_size: item_size,
            item_num: (PAGE_SIZE - TAIL_SIZE) / item_size,
            data: items_data.try_into().unwrap(),
            slot_bitmap: BitSlice::<u8, Lsb0>::from_slice_mut(bitmap_data),
        }
    }
    fn check_index_violent(&self, index: usize) {
        if index >= self.item_num {
            panic!(
                "Trying to use an illegal slot index {}, max is {}",
                index,
                self.item_num - 1
            );
        }
    }

    // clear the slot bits
    // used in first init
    pub fn set_slot_free(&mut self, index: usize) {
        self.check_index_violent(index);
        self.slot_bitmap.set(index, false);
    }
    pub fn set_slot_busy(&mut self, index: usize) {
        self.check_index_violent(index);
        self.slot_bitmap.set(index, true);
    }
    pub fn check_slot_stat(&self, index: usize) -> bool {
        assert!(index < self.item_num);
        self.slot_bitmap[index]
    }

    // TIP: didn't return &mut RecordItem, for the "ref" needs its owner live longer. but here it
    // will be destructed.
    // Instead, RecordItem<'_> just return the "value". but since it is just constructed by "ref",
    // so no copy will be introduced.
    // NOTE: 想要达到“指针”的功能，未必要返回 &mut, 因为引用的前提是其Owner的声明周期安全
    // 如果需要更长的声明周期，就必须返回值。这和“指针”并不矛盾，因为值本身也可以用引用来构造
    pub fn get_item(&mut self, index: usize) -> RecordItem<'_> {
        // TIP: to_vec() will cause copy. avoid it!

        //protect
        self.check_index_violent(index);

        let item_start: usize = self.item_size * index;

        RecordItem::new(&mut self.data[item_start..(item_start + self.item_size)])
    }

    // NOTE:
    // ensure the size of vector equals "item_size"
    //
    // pub fn read_item(&self, index: usize) -> RecordItem {
    //     //protect
    //     self.check_index_violent(index);
    //
    //     let item_start: usize = self.item_size * index;
    //     RecordItem::from_raw(self.data[item_start..(item_start + self.item_size)].to_vec())
    // }
    // pub fn write_item(&mut self, index: usize, record_item: RecordItem) {
    //     let item_data: Vec<u8> = record_item.move_to_bytes();
    //     //protect
    //     assert!(item_data.len() == self.item_size);
    //     self.check_index_violent(index);
    //
    //     let item_start: usize = self.item_size * index;
    //     self.data[item_start..(item_start + self.item_size)].copy_from_slice(&item_data);
    // }
}
