use std::usize;

use crate::{dbms::cache::Page, table::page::record::RecordId};

pub type IndexNodeId = u32;

// TODO:
struct TreeNode<'a> {
    page: &'a mut [u8; 4096],
}

impl<'a> TreeNode<'a> {
    //
    pub fn new(page: &'a mut [u8; 4096]) -> Self {
        Self { page: page }
    }
}

struct LeafNode<const LEAF_SLOT_CAPACITY: usize, const PAGE_SIZE: usize> {
    // TODO::
    // restrict LEAF_SLOT_CAPACITY
    prev_leaf_page_id: IndexNodeId,
    next_leaf_page_id: IndexNodeId,
    leaf_slots: Vec<RecordId>,
}

impl<const LEAF_SLOT_CAPACITY: usize, const PAGE_SIZE: usize>
    LeafNode<LEAF_SLOT_CAPACITY, PAGE_SIZE>
{
    pub fn new(page: &mut Page) -> Self {
        //
        let page_data = page.data;

        Self {
            prev_leaf_page_id: IndexNodeId::from_le_bytes(page_data[0..4].try_into().unwrap()), // SOME: need borrowing when get a slice of array
            next_leaf_page_id: IndexNodeId::from_le_bytes(page_data[4..8].try_into().unwrap()),
            // SOME: from raw bytes to u32 vec
            leaf_slots: page_data[8..8 + 4 * LEAF_SLOT_CAPACITY]
                .chunks_exact(4)
                .map(|c| RecordId::from_le_bytes(c.try_into().unwrap()))
                .collect(),
        }
    }
}

struct InnerNode {}
