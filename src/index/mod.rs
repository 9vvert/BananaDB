use std::{cmp::Ordering, mem::size_of};

use crate::{
    dbms::{
        cache::{CacheBuf, Page},
        resource::PageType,
    },
    table::page::{
        PAGE_SIZE,
        record::{ColumnType, ColumnValue, RecordId},
    },
};

pub mod node;

const INDEX_MAGIC: u32 = 0x424c_5054; // "BLPT"
const INVALID_PAGE: u32 = u32::MAX;
const NODE_HEADER_SIZE: usize = 16;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum NodeType {
    Leaf = 0,
    Internal = 1,
}

#[derive(Clone)]
pub enum Bound {
    Unbounded,
    Inclusive(ColumnValue),
    Exclusive(ColumnValue),
}

// ====================== Header Page =========================
struct HeaderPage<'a> {
    page: &'a mut Page,
}

impl<'a> HeaderPage<'a> {
    fn new(page: &'a mut Page) -> Self {
        HeaderPage { page }
    }

    fn init_if_needed(&mut self, col_idx: usize) {
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

    fn root(&self) -> u32 {
        u32::from_le_bytes(self.page.data[4..8].try_into().unwrap())
    }

    fn set_root(&mut self, page_id: u32) {
        self.page.data[4..8].copy_from_slice(&page_id.to_le_bytes());
        self.page.set_dirty();
    }

    fn next_page_id(&self) -> u32 {
        u32::from_le_bytes(self.page.data[8..12].try_into().unwrap())
    }

    fn set_next_page_id(&mut self, val: u32) {
        self.page.data[8..12].copy_from_slice(&val.to_le_bytes());
        self.page.set_dirty();
    }

    fn col_idx(&self) -> usize {
        u32::from_le_bytes(self.page.data[12..16].try_into().unwrap()) as usize
    }

    fn set_col_idx(&mut self, val: u32) {
        self.page.data[12..16].copy_from_slice(&val.to_le_bytes());
        self.page.set_dirty();
    }

    fn alloc_page(&mut self) -> u32 {
        let next = self.next_page_id();
        self.set_next_page_id(next + 1);
        next
    }
}

// ====================== Node Page ===========================
struct IndexNodePage<'a> {
    data: &'a mut [u8; PAGE_SIZE],
    key_size: usize,
}

impl<'a> IndexNodePage<'a> {
    fn new(data: &'a mut [u8; PAGE_SIZE], key_size: usize) -> Self {
        IndexNodePage { data, key_size }
    }

    fn node_type(&self) -> NodeType {
        match u32::from_le_bytes(self.data[0..4].try_into().unwrap()) {
            0 => NodeType::Leaf,
            1 => NodeType::Internal,
            v => panic!("Unknown node type {}", v),
        }
    }

    fn set_node_type(&mut self, ty: NodeType) {
        self.data[0..4].copy_from_slice(&(ty as u32).to_le_bytes());
    }

    fn key_count(&self) -> usize {
        u32::from_le_bytes(self.data[4..8].try_into().unwrap()) as usize
    }

    fn set_key_count(&mut self, cnt: usize) {
        self.data[4..8].copy_from_slice(&(cnt as u32).to_le_bytes());
    }

    fn next_leaf(&self) -> u32 {
        u32::from_le_bytes(self.data[8..12].try_into().unwrap())
    }

    fn set_next_leaf(&mut self, next: u32) {
        self.data[8..12].copy_from_slice(&next.to_le_bytes());
    }

    fn leaf_entry_size(&self) -> usize {
        self.key_size + size_of::<u32>()
    }

    fn internal_entry_size(&self) -> usize {
        self.key_size + size_of::<u32>()
    }

    fn leaf_capacity(&self) -> usize {
        (PAGE_SIZE - NODE_HEADER_SIZE) / self.leaf_entry_size()
    }

    fn internal_capacity(&self) -> usize {
        (PAGE_SIZE - NODE_HEADER_SIZE - size_of::<u32>()) / self.internal_entry_size()
    }

    fn leaf_entry_offset(&self, index: usize) -> usize {
        NODE_HEADER_SIZE + index * self.leaf_entry_size()
    }

    fn internal_first_child_offset(&self) -> usize {
        NODE_HEADER_SIZE
    }

    fn internal_entry_offset(&self, index: usize) -> usize {
        NODE_HEADER_SIZE + size_of::<u32>() + index * self.internal_entry_size()
    }

    fn leaf_key_bytes(&self, idx: usize) -> &[u8] {
        let off = self.leaf_entry_offset(idx);
        &self.data[off..off + self.key_size]
    }

    fn leaf_record_id(&self, idx: usize) -> RecordId {
        let off = self.leaf_entry_offset(idx) + self.key_size;
        let bytes: [u8; 4] = self.data[off..off + 4].try_into().unwrap();
        RecordId::from_le_bytes(bytes)
    }

    fn write_leaf_entry(&mut self, idx: usize, key: &[u8], rid: RecordId) {
        let off = self.leaf_entry_offset(idx);
        self.data[off..off + self.key_size].copy_from_slice(key);
        self.data[off + self.key_size..off + self.key_size + 4].copy_from_slice(&rid.to_le_bytes());
    }

    fn shift_leaf_entries(&mut self, start: usize, end: usize) {
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

    fn shift_leaf_entries_left(&mut self, start: usize, end: usize) {
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

    fn internal_child_at(&self, idx: usize) -> u32 {
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

    fn set_internal_child0(&mut self, child: u32) {
        let off = self.internal_first_child_offset();
        self.data[off..off + 4].copy_from_slice(&child.to_le_bytes());
    }

    fn internal_key_bytes(&self, idx: usize) -> &[u8] {
        let off = self.internal_entry_offset(idx);
        &self.data[off..off + self.key_size]
    }

    fn write_internal_entry(&mut self, idx: usize, key: &[u8], child: u32) {
        let off = self.internal_entry_offset(idx);
        self.data[off..off + self.key_size].copy_from_slice(key);
        self.data[off + self.key_size..off + self.key_size + 4]
            .copy_from_slice(&child.to_le_bytes());
    }

    fn shift_internal_entries(&mut self, start: usize, end: usize) {
        if end <= start {
            return;
        }
        let entry_size = self.internal_entry_size();
        let base = NODE_HEADER_SIZE + size_of::<u32>();
        let total_bytes = (end - start) * entry_size;
        self.data.copy_within(
            base + start * entry_size..base + start * entry_size + total_bytes,
            base + (start + 1) * entry_size,
        );
    }

    fn shift_internal_entries_left(&mut self, start: usize, end: usize) {
        if end <= start {
            return;
        }
        let entry_size = self.internal_entry_size();
        let base = NODE_HEADER_SIZE + size_of::<u32>();
        let total_bytes = (end - start) * entry_size;
        self.data.copy_within(
            base + (start + 1) * entry_size..base + (start + 1) * entry_size + total_bytes,
            base + start * entry_size,
        );
    }
}

// ====================== B+ Tree ===========================
pub struct BPlusTree<'a, const PAGE_NUM: usize> {
    buf: &'a mut CacheBuf<PAGE_NUM>,
    table_name: &'a str,
    col_idx: usize,
    col_type: ColumnType,
    key_size: usize,
    extra_info: String,
}

impl<'a, const PAGE_NUM: usize> BPlusTree<'a, PAGE_NUM> {
    pub fn new(
        buf: &'a mut CacheBuf<PAGE_NUM>,
        table_name: &'a str,
        col_idx: usize,
        col_type: ColumnType,
    ) -> Self {
        let key_size = col_type.size();
        let extra_info = col_idx.to_string();
        BPlusTree {
            buf,
            table_name,
            col_idx,
            col_type,
            key_size,
            extra_info,
        }
    }

    fn header_page(&mut self) -> HeaderPage<'_> {
        let header = self
            .buf
            .get_page(self.table_name, 0, &PageType::INDEX, &self.extra_info);
        HeaderPage::new(header)
    }

    fn ensure_header(&mut self) {
        let col_idx = self.col_idx;
        let mut header = self.header_page();
        header.init_if_needed(col_idx);
    }

    fn root_page_id(&mut self) -> u32 {
        let col_idx = self.col_idx;
        let mut header = self.header_page();
        header.init_if_needed(col_idx);
        header.root()
    }

    fn set_root_page(&mut self, root: u32) {
        let col_idx = self.col_idx;
        let mut header = self.header_page();
        header.init_if_needed(col_idx);
        header.set_root(root);
    }

    fn alloc_page(&mut self) -> u32 {
        let col_idx = self.col_idx;
        let mut header = self.header_page();
        header.init_if_needed(col_idx);
        header.alloc_page()
    }

    fn compare_key(col_type: ColumnType, a: &[u8], b: &[u8]) -> Ordering {
        match col_type {
            ColumnType::INT => {
                let va = i32::from_le_bytes(a[..4].try_into().unwrap());
                let vb = i32::from_le_bytes(b[..4].try_into().unwrap());
                va.cmp(&vb)
            }
            ColumnType::CHAR(_) => {
                let sa = std::str::from_utf8(a).unwrap().trim_end_matches('\0');
                let sb = std::str::from_utf8(b).unwrap().trim_end_matches('\0');
                sa.cmp(sb)
            }
        }
    }

    fn encode_key(&self, v: &ColumnValue) -> Result<Vec<u8>, String> {
        match (v, &self.col_type) {
            (ColumnValue::INT(x), ColumnType::INT) => Ok(x.to_le_bytes().to_vec()),
            (ColumnValue::CAHR(s), ColumnType::CHAR(len)) => {
                let mut buf = vec![0u8; *len];
                let copy_len = buf.len().min(s.len());
                buf[..copy_len].copy_from_slice(&s.as_bytes()[..copy_len]);
                Ok(buf)
            }
            _ => Err("Column type mismatch for index key".to_string()),
        }
    }

    fn leftmost_leaf(&mut self, root: u32) -> u32 {
        let mut current = root;
        loop {
            let node_page = self.buf.get_page(
                self.table_name,
                current as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            let node = IndexNodePage::new(&mut node_page.data, self.key_size);
            if node.node_type() == NodeType::Leaf {
                return current;
            }
            let child = node.internal_child_at(0);
            if child == INVALID_PAGE {
                return current;
            }
            current = child;
        }
    }

    fn find_leaf(&mut self, key: &[u8], root: u32) -> (u32, Vec<u32>) {
        let mut path = Vec::new();
        let mut current = root;
        let col_type = self.col_type;
        loop {
            path.push(current);
            let page = self.buf.get_page(
                self.table_name,
                current as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            let node = IndexNodePage::new(&mut page.data, self.key_size);
            match node.node_type() {
                NodeType::Leaf => return (current, path),
                NodeType::Internal => {
                    let key_cnt = node.key_count();
                    let mut idx = 0;
                    while idx < key_cnt {
                        if Self::compare_key(col_type, key, node.internal_key_bytes(idx))
                            == Ordering::Less
                        {
                            break;
                        }
                        idx += 1;
                    }
                    let child = node.internal_child_at(idx);
                    current = child;
                }
            }
        }
    }

    fn insert_into_leaf(
        &mut self,
        page_id: u32,
        key: &[u8],
        rid: RecordId,
    ) -> Result<Option<(Vec<u8>, u32)>, String> {
        let col_type = self.col_type;
        {
            let page = self.buf.get_page(
                self.table_name,
                page_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            page.set_dirty();
            let mut node = IndexNodePage::new(&mut page.data, self.key_size);
            if node.node_type() != NodeType::Leaf {
                return Err("Target node is not leaf".to_string());
            }
            let mut pos = 0;
            let key_cnt = node.key_count();
            while pos < key_cnt
                && Self::compare_key(col_type, node.leaf_key_bytes(pos), key) == Ordering::Less
            {
                pos += 1;
            }
            if pos < key_cnt {
                node.shift_leaf_entries(pos, key_cnt);
            }
            node.write_leaf_entry(pos, key, rid);
            node.set_key_count(key_cnt + 1);
            if node.key_count() <= node.leaf_capacity() {
                return Ok(None);
            }
        }

        let new_page_id = self.alloc_page();
        let (move_buf, move_count, new_first_key, next_leaf);
        {
            let page = self.buf.get_page(
                self.table_name,
                page_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            page.set_dirty();
            let mut node = IndexNodePage::new(&mut page.data, self.key_size);
            let total = node.key_count();
            let split = total / 2;
            let entry_size = node.leaf_entry_size();
            let move_bytes = (total - split) * entry_size;
            let mut buf = vec![0u8; move_bytes];
            let src_off = node.leaf_entry_offset(split);
            buf.copy_from_slice(&node.data[src_off..src_off + move_bytes]);
            next_leaf = node.next_leaf();
            node.set_key_count(split);
            node.set_next_leaf(new_page_id);
            move_count = total - split;
            new_first_key = buf[..self.key_size].to_vec();
            move_buf = buf;
        }

        {
            let new_page = self.buf.get_page(
                self.table_name,
                new_page_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            new_page.data.fill(0);
            new_page.set_dirty();
            let mut new_node = IndexNodePage::new(&mut new_page.data, self.key_size);
            new_node.set_node_type(NodeType::Leaf);
            new_node.set_key_count(move_count);
            new_node.set_next_leaf(next_leaf);
            let dst_off = new_node.leaf_entry_offset(0);
            new_node.data[dst_off..dst_off + move_buf.len()].copy_from_slice(&move_buf);
        }

        Ok(Some((new_first_key, new_page_id)))
    }

    fn insert_into_internal(
        &mut self,
        page_id: u32,
        left_child: u32,
        separator: &[u8],
        right_child: u32,
    ) -> Result<Option<(Vec<u8>, u32)>, String> {
        {
            let page = self.buf.get_page(
                self.table_name,
                page_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            page.set_dirty();
            let mut node = IndexNodePage::new(&mut page.data, self.key_size);
            if node.node_type() != NodeType::Internal {
                return Err("Target node is not internal".to_string());
            }
            let key_cnt = node.key_count();
            let mut pos = None;
            for i in 0..=key_cnt {
                if node.internal_child_at(i) == left_child {
                    pos = Some(i);
                    break;
                }
            }
            let insert_pos = pos.ok_or_else(|| "Parent-child link broken".to_string())?;
            if insert_pos < key_cnt {
                node.shift_internal_entries(insert_pos, key_cnt);
            }
            node.write_internal_entry(insert_pos, separator, right_child);
            node.set_key_count(key_cnt + 1);
            if node.key_count() <= node.internal_capacity() {
                return Ok(None);
            }
        }

        let new_page_id = self.alloc_page();
        let (promote_key, move_buf, move_count, first_child_right);
        {
            let page = self.buf.get_page(
                self.table_name,
                page_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            page.set_dirty();
            let mut node = IndexNodePage::new(&mut page.data, self.key_size);
            let total = node.key_count();
            let mid = total / 2;
            let entry_size = node.internal_entry_size();
            let move_count_tmp = total - mid - 1;
            let move_bytes = move_count_tmp * entry_size;
            let mut buf = vec![0u8; move_bytes];
            if move_bytes > 0 {
                let src_off = node.internal_entry_offset(mid + 1);
                buf.copy_from_slice(&node.data[src_off..src_off + move_bytes]);
            }
            promote_key = node.internal_key_bytes(mid).to_vec();
            first_child_right = node.internal_child_at(mid + 1);
            node.set_key_count(mid);
            move_buf = buf;
            move_count = move_count_tmp;
        }

        {
            let new_page = self.buf.get_page(
                self.table_name,
                new_page_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            new_page.data.fill(0);
            new_page.set_dirty();
            let mut new_node = IndexNodePage::new(&mut new_page.data, self.key_size);
            new_node.set_node_type(NodeType::Internal);
            new_node.set_key_count(move_count);
            new_node.set_internal_child0(first_child_right);
            if move_count > 0 {
                let dst_off = new_node.internal_entry_offset(0);
                new_node.data[dst_off..dst_off + move_buf.len()].copy_from_slice(&move_buf);
            }
        }

        Ok(Some((promote_key, new_page_id)))
    }

    pub fn insert(&mut self, key_val: &ColumnValue, rid: RecordId) -> Result<(), String> {
        let key = self.encode_key(key_val)?;
        self.ensure_header();
        let root = self.root_page_id();

        if root == INVALID_PAGE {
            // create first leaf
            let root_id = self.alloc_page();
            {
                let root_page = self.buf.get_page(
                    self.table_name,
                    root_id as usize,
                    &PageType::INDEX,
                    &self.extra_info,
                );
                root_page.data.fill(0);
                root_page.set_dirty();
                let mut root_node = IndexNodePage::new(&mut root_page.data, self.key_size);
                root_node.set_node_type(NodeType::Leaf);
                root_node.set_key_count(0);
                root_node.set_next_leaf(INVALID_PAGE);
            }
            self.insert_into_leaf(root_id, &key, rid)?;
            self.set_root_page(root_id);
            return Ok(());
        }

        let (leaf_id, path) = self.find_leaf(&key, root);
        let mut child_id = leaf_id;
        let mut split_info = self.insert_into_leaf(leaf_id, &key, rid)?;
        if split_info.is_none() {
            return Ok(());
        }

        let (mut sep_key, mut right_id) = split_info.take().unwrap();
        for parent_id in path.iter().rev().skip(1) {
            let res = self.insert_into_internal(*parent_id, child_id, &sep_key, right_id)?;
            if let Some((next_key, next_right)) = res {
                sep_key = next_key;
                right_id = next_right;
                child_id = *parent_id;
            } else {
                return Ok(());
            }
        }

        // create new root
        let new_root_id = self.alloc_page();
        {
            let new_root_page = self.buf.get_page(
                self.table_name,
                new_root_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            new_root_page.data.fill(0);
            new_root_page.set_dirty();
            let mut new_root = IndexNodePage::new(&mut new_root_page.data, self.key_size);
            new_root.set_node_type(NodeType::Internal);
            new_root.set_key_count(1);
            new_root.set_internal_child0(root);
            new_root.write_internal_entry(0, &sep_key, right_id);
        }
        self.set_root_page(new_root_id);
        Ok(())
    }

    fn search_leaf_rids(
        &mut self,
        lower: &Bound,
        upper: &Bound,
        root: u32,
    ) -> Result<Vec<RecordId>, String> {
        let col_type = self.col_type;
        let lower_key = match lower {
            Bound::Unbounded => None,
            Bound::Inclusive(v) | Bound::Exclusive(v) => Some(self.encode_key(v)?),
        };
        let upper_key = match upper {
            Bound::Unbounded => None,
            Bound::Inclusive(v) | Bound::Exclusive(v) => Some(self.encode_key(v)?),
        };

        let start_leaf = if let Some(ref key) = lower_key {
            let (leaf_id, _) = self.find_leaf(key, root);
            leaf_id
        } else {
            self.leftmost_leaf(root)
        };

        let mut result = Vec::new();
        let mut current = start_leaf;
        loop {
            let page = self.buf.get_page(
                self.table_name,
                current as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            let node = IndexNodePage::new(&mut page.data, self.key_size);
            assert_eq!(node.node_type(), NodeType::Leaf);
            let key_cnt = node.key_count();
            for i in 0..key_cnt {
                let key_bytes = node.leaf_key_bytes(i);
                if let Some(ref lk) = lower_key {
                    let ord = Self::compare_key(col_type, key_bytes, lk);
                    let ok = match lower {
                        Bound::Inclusive(_) => ord != Ordering::Less,
                        Bound::Exclusive(_) => ord == Ordering::Greater,
                        Bound::Unbounded => true,
                    };
                    if !ok {
                        continue;
                    }
                }
                if let Some(ref uk) = upper_key {
                    let ord = Self::compare_key(col_type, key_bytes, uk);
                    let ok = match upper {
                        Bound::Inclusive(_) => ord != Ordering::Greater,
                        Bound::Exclusive(_) => ord == Ordering::Less,
                        Bound::Unbounded => true,
                    };
                    if !ok {
                        return Ok(result);
                    }
                }
                result.push(node.leaf_record_id(i));
            }
            let next = node.next_leaf();
            if next == INVALID_PAGE {
                break;
            }
            current = next;
        }

        Ok(result)
    }

    pub fn search_range(&mut self, lower: Bound, upper: Bound) -> Result<Vec<RecordId>, String> {
        self.ensure_header();
        let root = self.root_page_id();
        if root == INVALID_PAGE {
            return Ok(Vec::new());
        }
        self.search_leaf_rids(&lower, &upper, root)
    }

    pub fn delete(&mut self, key_val: ColumnValue, rid: RecordId) -> Result<(), String> {
        let key = self.encode_key(&key_val)?;
        self.ensure_header();
        let root = self.root_page_id();
        if root == INVALID_PAGE {
            return Ok(());
        }
        let (leaf_id, _) = self.find_leaf(&key, root);
        let page = self.buf.get_page(
            self.table_name,
            leaf_id as usize,
            &PageType::INDEX,
            &self.extra_info,
        );
        page.set_dirty();
        let mut node = IndexNodePage::new(&mut page.data, self.key_size);
        if node.node_type() != NodeType::Leaf {
            return Err("Delete target is not a leaf".to_string());
        }
        let key_cnt = node.key_count();
        for i in 0..key_cnt {
            if Self::compare_key(self.col_type, node.leaf_key_bytes(i), &key) == Ordering::Equal
                && node.leaf_record_id(i) == rid
            {
                node.shift_leaf_entries_left(i, key_cnt - 1);
                node.set_key_count(key_cnt - 1);
                break;
            }
        }
        // TODO: handle underflow/merge if needed.
        Ok(())
    }
}
