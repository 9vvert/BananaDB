use crate::{
    dbms::cache::Page,
    table::page::{
        record::{ColumnValue, RecordId},
        PAGE_SIZE,
    },
};

pub const INDEX_MAGIC: u32 = 0x424c_5054; // "BLPT"
pub const INVALID_PAGE: u32 = u32::MAX;
pub const NODE_HEADER_SIZE: usize = 16;

// ====================== Header Page =========================
pub struct HeaderPage<'a> {
    pub page: &'a mut Page,
}

impl<'a> HeaderPage<'a> {
    pub fn new(page: &'a mut Page) -> Self {
        HeaderPage { page }
    }

    // TODO: 是否应该清除magic number?
    pub fn init_if_needed(&mut self, col_idx: usize) {
        let magic = u32::from_le_bytes(self.page.data[0..4].try_into().unwrap());
        if magic != INDEX_MAGIC {
            self.page.data.fill(0);
            self.page.data[0..4].copy_from_slice(&INDEX_MAGIC.to_le_bytes());
            self.set_root(INVALID_PAGE);
            self.set_next_page_id(1);
            self.set_col_idx(col_idx as u32);
            self.page.set_dirty();
        }
    }

    pub fn root(&self) -> u32 {
        u32::from_le_bytes(self.page.data[4..8].try_into().unwrap())
    }

    pub fn set_root(&mut self, page_id: u32) {
        self.page.data[4..8].copy_from_slice(&page_id.to_le_bytes());
        self.page.set_dirty();
    }

    pub fn next_page_id(&self) -> u32 {
        u32::from_le_bytes(self.page.data[8..12].try_into().unwrap())
    }

    pub fn set_next_page_id(&mut self, val: u32) {
        self.page.data[8..12].copy_from_slice(&val.to_le_bytes());
        self.page.set_dirty();
    }

    pub fn col_idx(&self) -> usize {
        u32::from_le_bytes(self.page.data[12..16].try_into().unwrap()) as usize
    }

    pub fn set_col_idx(&mut self, val: u32) {
        self.page.data[12..16].copy_from_slice(&val.to_le_bytes());
        self.page.set_dirty();
    }

    pub fn alloc_page(&mut self) -> u32 {
        let next = self.next_page_id();
        self.set_next_page_id(next + 1);
        next
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NodeType {
    Leaf = 0,
    Internal = 1,
}

#[derive(Clone)]
pub enum Bound {
    Unbounded,
    Inclusive(ColumnValue),
    Exclusive(ColumnValue),
}
// ====================== InnerNode =========================
pub struct InnerNode<'a> {
    pub data: &'a mut [u8; PAGE_SIZE],
    pub key_size: usize,
}

impl<'a> InnerNode<'a> {
    pub fn new(index_node: IndexNode<'a>) -> Self {
        InnerNode {
            data: index_node.data,
            key_size: index_node.key_size,
        }
    }

    pub fn node_type(&self) -> NodeType {
        NodeType::Internal
    }

    pub fn key_count(&self) -> usize {
        u32::from_le_bytes(self.data[4..8].try_into().unwrap()) as usize
    }

    pub fn set_key_count(&mut self, cnt: usize) {
        self.data[4..8].copy_from_slice(&(cnt as u32).to_le_bytes());
    }

    pub fn internal_entry_size(&self) -> usize {
        self.key_size + 4
    }

    pub fn internal_capacity(&self) -> usize {
        (PAGE_SIZE - NODE_HEADER_SIZE - 4) / self.internal_entry_size()
    }

    pub fn internal_first_child_offset(&self) -> usize {
        NODE_HEADER_SIZE
    }

    pub fn internal_entry_offset(&self, index: usize) -> usize {
        NODE_HEADER_SIZE + 4 + index * self.internal_entry_size()
    }

    pub fn internal_child_at(&self, idx: usize) -> u32 {
        // idx in [0, key_count]
        let key_cnt = self.key_count();
        assert!(idx <= key_cnt);
        if idx == 0 {
            let off = self.internal_first_child_offset();
            u32::from_le_bytes(self.data[off..off + 4].try_into().unwrap())
        } else {
            let entry_off = self.internal_entry_offset(idx - 1);
            let child_off = entry_off + self.key_size;
            u32::from_le_bytes(self.data[child_off..child_off + 4].try_into().unwrap())
        }
    }

    pub fn set_internal_child0(&mut self, child: u32) {
        let off = self.internal_first_child_offset();
        self.data[off..off + 4].copy_from_slice(&child.to_le_bytes());
    }

    pub fn internal_key_bytes(&self, idx: usize) -> &[u8] {
        let off = self.internal_entry_offset(idx);
        &self.data[off..off + self.key_size]
    }

    pub fn write_internal_entry(&mut self, idx: usize, key: &[u8], child: u32) {
        let off = self.internal_entry_offset(idx);
        self.data[off..off + self.key_size].copy_from_slice(key);
        self.data[off + self.key_size..off + self.key_size + 4]
            .copy_from_slice(&child.to_le_bytes());
    }

    pub fn shift_internal_entries(&mut self, start: usize, end: usize) {
        if end <= start {
            return;
        }
        let entry_size = self.internal_entry_size();
        let base = NODE_HEADER_SIZE + 4;
        let total_bytes = (end - start) * entry_size;
        self.data.copy_within(
            base + start * entry_size..base + start * entry_size + total_bytes,
            base + (start + 1) * entry_size,
        );
    }

    pub fn shift_internal_entries_left(&mut self, start: usize, end: usize) {
        if end <= start {
            return;
        }
        let entry_size = self.internal_entry_size();
        let base = NODE_HEADER_SIZE + 4;
        let total_bytes = (end - start) * entry_size;
        self.data.copy_within(
            base + (start + 1) * entry_size..base + (start + 1) * entry_size + total_bytes,
            base + start * entry_size,
        );
    }
}

// ====================== LeafNode =========================

pub struct LeafNode<'a> {
    pub data: &'a mut [u8; PAGE_SIZE],
    pub key_size: usize,
}

impl<'a> LeafNode<'a> {
    pub fn new(index_node: IndexNode<'a>) -> Self {
        LeafNode {
            data: index_node.data,
            key_size: index_node.key_size,
        }
    }
    pub fn node_type(&self) -> NodeType {
        NodeType::Leaf
    }
    pub fn key_count(&self) -> usize {
        u32::from_le_bytes(self.data[4..8].try_into().unwrap()) as usize
    }

    pub fn set_key_count(&mut self, cnt: usize) {
        self.data[4..8].copy_from_slice(&(cnt as u32).to_le_bytes());
    }

    pub fn next_leaf(&self) -> u32 {
        u32::from_le_bytes(self.data[8..12].try_into().unwrap())
    }

    pub fn set_next_leaf(&mut self, next: u32) {
        self.data[8..12].copy_from_slice(&next.to_le_bytes());
    }

    pub fn leaf_entry_size(&self) -> usize {
        self.key_size + 4
    }

    // leaf
    pub fn leaf_capacity(&self) -> usize {
        (PAGE_SIZE - NODE_HEADER_SIZE) / self.leaf_entry_size()
    }

    pub fn leaf_entry_offset(&self, index: usize) -> usize {
        NODE_HEADER_SIZE + index * self.leaf_entry_size()
    }

    pub fn leaf_key_bytes(&self, idx: usize) -> &[u8] {
        let off = self.leaf_entry_offset(idx);
        &self.data[off..off + self.key_size]
    }

    pub fn leaf_record_id(&self, idx: usize) -> RecordId {
        let off = self.leaf_entry_offset(idx) + self.key_size;
        let bytes: [u8; 4] = self.data[off..off + 4].try_into().unwrap();
        RecordId::from_le_bytes(bytes)
    }

    pub fn write_leaf_entry(&mut self, idx: usize, key: &[u8], rid: RecordId) {
        let off = self.leaf_entry_offset(idx);
        self.data[off..off + self.key_size].copy_from_slice(key);
        self.data[off + self.key_size..off + self.key_size + 4].copy_from_slice(&rid.to_le_bytes());
    }

    pub fn shift_leaf_entries(&mut self, start: usize, end: usize) {
        if end <= start {
            return;
        }
        let entry_size = self.leaf_entry_size();
        let base = NODE_HEADER_SIZE;
        let total_bytes = (end - start) * entry_size;
        self.data.copy_within(
            base + start * entry_size..base + start * entry_size + total_bytes,
            base + (start + 1) * entry_size,
        );
    }

    pub fn shift_leaf_entries_left(&mut self, start: usize, end: usize) {
        if end <= start {
            return;
        }
        let entry_size = self.leaf_entry_size();
        let base = NODE_HEADER_SIZE;
        let total_bytes = (end - start) * entry_size;
        self.data.copy_within(
            base + (start + 1) * entry_size..base + (start + 1) * entry_size + total_bytes,
            base + start * entry_size,
        );
    }
}

// ====================== Node Page ===========================
// NOTE: 基类IndexNode
pub struct IndexNode<'a> {
    pub data: &'a mut [u8; PAGE_SIZE],
    pub key_size: usize,
}

impl<'a> IndexNode<'a> {
    pub fn new(data: &'a mut [u8; PAGE_SIZE], key_size: usize) -> Self {
        IndexNode { data, key_size }
    }

    pub fn node_type(&self) -> NodeType {
        match u32::from_le_bytes(self.data[0..4].try_into().unwrap()) {
            0 => NodeType::Leaf,
            1 => NodeType::Internal,
            v => panic!("Unknown node type {}", v),
        }
    }

    // inner & leaf
    pub fn set_node_type(&mut self, ty: NodeType) {
        self.data[0..4].copy_from_slice(&(ty as u32).to_le_bytes());
    }
}
