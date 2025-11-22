use std::ops::BitAndAssign;

use bitmaps::Bitmap;
use bytemuck::cast_slice;
// 从IO_manager中申请到的页面缓存
// 初始化成为一个PageData结构体
pub const TAIL_SIZE: usize = 64;
pub const BITMAP_SIZE: usize = 32;
pub const BITMAP_BIT_SIZE: usize = BITMAP_SIZE * 8;
// NOTE:
// Bitmap::from要求传入固定的[u128; 2]
pub struct DataPage<'a, const PAGE_SIZE: usize> {
    // fix part
    bitmap_offset: usize,
    meta_data: usize,
    slot_bitmap: Bitmap<BITMAP_BIT_SIZE>,
    //
    is_dirty: bool,
    item_size: usize,
    item_num: usize, // table item capacity
    data: &'a [u8; PAGE_SIZE],
}

impl<'a, const PAGE_SIZE: usize> DataPage<'a, PAGE_SIZE> {
    pub fn new(item_size: usize, page_data: &'a [u8; PAGE_SIZE]) -> Self {
        // from bytes to bitmap
        let bitmap_offset = PAGE_SIZE - TAIL_SIZE;
        let bitmap_data_u8: &[u8] = &page_data[bitmap_offset..(bitmap_offset + BITMAP_SIZE)];
        let bitmap_data_u128_vec: Vec<u128> = cast_slice(bitmap_data_u8).to_vec();
        let bitmap_data_u128_arr: [u128; 2] = bitmap_data_u128_vec.try_into().unwrap();
        // TODO:
        // metadata
        let meta_data = PAGE_SIZE - TAIL_SIZE + BITMAP_SIZE;

        DataPage {
            bitmap_offset: bitmap_offset,
            meta_data: meta_data,
            slot_bitmap: Bitmap::from(bitmap_data_u128_arr),
            is_dirty: false,
            item_size: item_size,
            item_num: (PAGE_SIZE - TAIL_SIZE) / item_size,
            data: page_data,
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

    // return a free slot
    pub fn find_free_slot(&self) -> Option<usize> {
        return match self.slot_bitmap.first_false_index() {
            Some(x) => {
                if x < self.item_num {
                    Some(x)
                } else {
                    None
                }
            }
            None => None,
        };
    }
    // clear the slot bits
    // used in first init
    pub fn clear_slot(&mut self) {
        self.slot_bitmap.bitand_assign(Bitmap::new());
    }
    pub fn set_slot_free(&mut self, index: usize) {
        self.check_index_violent(index);
        self.slot_bitmap.set(index, false);
    }
    pub fn set_slot_busy(&mut self, index: usize) {
        self.check_index_violent(index);
        self.slot_bitmap.set(index, true);
    }

    pub fn read_item(&self, index: usize) -> Vec<u8> {
        self.check_index_violent(index);

        let item_start: usize = self.item_size * index;
        return self.data[item_start..(item_start + self.item_size)].to_vec();
    }
}
