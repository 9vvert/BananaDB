use std::{cmp::Ordering, mem::size_of};

use crate::{
    dbms::{
        cache::{CacheBuf, Page, PageId},
        resource::PageType,
    },
    index::node::{Bound, HeaderPage, INVALID_PAGE, IndexNode, InnerNode, LeafNode, NodeType},
    table::page::{
        PAGE_SIZE,
        record::{ColumnType, ColumnValue, RecordId},
    },
};

pub mod node;

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

    // NOTE:获得第一页作为header page
    // TIP: 返回一个借用，但是生命周期 <= self
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

    // TODO: 将cmp和encode属性交给record.rs中实现
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

    // find the left-most leaf node of this B+tree
    fn leftmost_leaf(&mut self, root: u32) -> u32 {
        let mut current = root;
        loop {
            let node_page = self.buf.get_page(
                self.table_name,
                current as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            let node = IndexNode::new(&mut node_page.data, self.key_size);

            // case1: leaf node, return directly
            if node.node_type() == NodeType::Leaf {
                return current;
            }

            // case2: inner node
            let inner_node = InnerNode::new(node);
            let child = inner_node.internal_child_at(0);
            if child == INVALID_PAGE {
                return current;
            }
            current = child;
        }
    }

    // NOTE: 根据key查找所在的leaf节点 (一个leaf节点对应一个key-range，包含多个key)
    // 根据一个key值，要寻找最终的leaf，需要在多层中依次寻找满足要求的“最左子节点”
    //
    // 返回值：page_id + 搜寻路径
    fn find_leaf(&mut self, key: &[u8], root: PageId) -> (PageId, Vec<PageId>) {
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
            let node = IndexNode::new(&mut page.data, self.key_size);
            match node.node_type() {
                NodeType::Leaf => return (current, path),
                NodeType::Internal => {
                    let inner_node = InnerNode::new(node);
                    let key_cnt = inner_node.key_count();
                    let mut idx = 0;
                    // 遍历当前的key array, 找到 key < inner_node的第一个key
                    while idx < key_cnt {
                        if Self::compare_key(col_type, key, inner_node.internal_key_bytes(idx))
                            == Ordering::Less
                        {
                            break;
                        }
                        idx += 1;
                    }
                    let child = inner_node.internal_child_at(idx);
                    current = child;
                }
            }
        }
    }

    // page_id指向的页面应该是leaf page
    fn insert_into_leaf(
        &mut self,
        page_id: PageId,
        key: &[u8],
        rid: RecordId,
    ) -> Result<Option<(Vec<u8>, u32)>, String> {
        let col_type = self.col_type;
        // TIP: 使用child block，将后续不会使用的变量释放，解决所有权问题
        {
            let page = self.buf.get_page(
                self.table_name,
                page_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            page.set_dirty();
            let mut node = IndexNode::new(&mut page.data, self.key_size);
            // check node type
            if node.node_type() != NodeType::Leaf {
                return Err("Target node is not leaf".to_string());
            }

            let mut leaf_node = LeafNode::new(node);
            let mut pos = 0;
            let key_cnt = leaf_node.key_count();
            // find the "fit" slot
            while pos < key_cnt
                && Self::compare_key(col_type, leaf_node.leaf_key_bytes(pos), key) == Ordering::Less
            {
                pos += 1;
            }
            if pos < key_cnt {
                leaf_node.shift_leaf_entries(pos, key_cnt);
            }
            leaf_node.write_leaf_entry(pos, key, rid);
            // NOTE: key_count记录当前LeafNode存储的RecordId条目数量
            leaf_node.set_key_count(key_cnt + 1);

            // 一般情况：容量重组，无须分裂
            if leaf_node.key_count() <= leaf_node.leaf_capacity() {
                return Ok(None);
            }
        }

        // 容量超出限制，进行B+树分离
        let new_page_id = self.alloc_page();
        // TIP: 使用let + ()定义多个变量
        let (move_buf, move_count, new_first_key, next_leaf);
        {
            let page = self.buf.get_page(
                self.table_name,
                page_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            page.set_dirty();
            let node = IndexNode::new(&mut page.data, self.key_size);
            let mut leaf_node = LeafNode::new(node);
            let total = leaf_node.key_count();
            let split = total / 2;
            let entry_size = leaf_node.leaf_entry_size();
            let move_bytes = (total - split) * entry_size;
            let mut buf = vec![0u8; move_bytes];
            let src_off = leaf_node.leaf_entry_offset(split);
            buf.copy_from_slice(&leaf_node.data[src_off..src_off + move_bytes]);
            next_leaf = leaf_node.next_leaf();
            leaf_node.set_key_count(split);
            leaf_node.set_next_leaf(new_page_id);
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
            // NOTE: 这里初始化LeafNode
            let mut new_node = IndexNode::new(&mut new_page.data, self.key_size);
            new_node.set_node_type(NodeType::Leaf);
            let mut leaf_new_node = LeafNode::new(new_node);
            leaf_new_node.set_key_count(move_count);
            leaf_new_node.set_next_leaf(next_leaf);
            let dst_off = leaf_new_node.leaf_entry_offset(0);
            leaf_new_node.data[dst_off..dst_off + move_buf.len()].copy_from_slice(&move_buf);
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
            let node = IndexNode::new(&mut page.data, self.key_size);
            if node.node_type() != NodeType::Internal {
                return Err("Target node is not internal".to_string());
            }

            let mut inner_node = InnerNode::new(node);
            let key_cnt = inner_node.key_count();
            let mut pos = None;
            for i in 0..=key_cnt {
                if inner_node.internal_child_at(i) == left_child {
                    pos = Some(i);
                    break;
                }
            }
            let insert_pos = pos.ok_or_else(|| "Parent-child link broken".to_string())?;
            if insert_pos < key_cnt {
                inner_node.shift_internal_entries(insert_pos, key_cnt);
            }
            inner_node.write_internal_entry(insert_pos, separator, right_child);
            inner_node.set_key_count(key_cnt + 1);
            if inner_node.key_count() <= inner_node.internal_capacity() {
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
            let mut node = IndexNode::new(&mut page.data, self.key_size);
            let mut inner_node = InnerNode::new(node);
            let total = inner_node.key_count();
            let mid = total / 2;
            let entry_size = inner_node.internal_entry_size();
            let move_count_tmp = total - mid - 1;
            let move_bytes = move_count_tmp * entry_size;
            let mut buf = vec![0u8; move_bytes];
            if move_bytes > 0 {
                let src_off = inner_node.internal_entry_offset(mid + 1);
                buf.copy_from_slice(&inner_node.data[src_off..src_off + move_bytes]);
            }
            promote_key = inner_node.internal_key_bytes(mid).to_vec();
            first_child_right = inner_node.internal_child_at(mid + 1);
            inner_node.set_key_count(mid);
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
            let mut new_node = IndexNode::new(&mut new_page.data, self.key_size);
            new_node.set_node_type(NodeType::Internal);

            let mut inner_new_node = InnerNode::new(new_node);
            inner_new_node.set_key_count(move_count);
            inner_new_node.set_internal_child0(first_child_right);
            if move_count > 0 {
                let dst_off = inner_new_node.internal_entry_offset(0);
                inner_new_node.data[dst_off..dst_off + move_buf.len()].copy_from_slice(&move_buf);
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
                let mut root_node = IndexNode::new(&mut root_page.data, self.key_size);
                root_node.set_node_type(NodeType::Leaf);

                let mut root_node_as_leaf = LeafNode::new(root_node);

                root_node_as_leaf.set_key_count(0);
                root_node_as_leaf.set_next_leaf(INVALID_PAGE);
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
            let mut new_root = IndexNode::new(&mut new_root_page.data, self.key_size);
            new_root.set_node_type(NodeType::Internal);

            let mut new_root_as_inner = InnerNode::new(new_root);
            new_root_as_inner.set_key_count(1);
            new_root_as_inner.set_internal_child0(root);
            new_root_as_inner.write_internal_entry(0, &sep_key, right_id);
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
        // get bound key by range
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
            let node = IndexNode::new(&mut page.data, self.key_size);
            assert_eq!(node.node_type(), NodeType::Leaf);
            let leaf_node = LeafNode::new(node);
            let key_cnt = leaf_node.key_count();
            for i in 0..key_cnt {
                let key_bytes = leaf_node.leaf_key_bytes(i);
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
                result.push(leaf_node.leaf_record_id(i));
            }
            let next = leaf_node.next_leaf();
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
        let node = IndexNode::new(&mut page.data, self.key_size);
        if node.node_type() != NodeType::Leaf {
            return Err("Delete target is not a leaf".to_string());
        }
        let mut leaf_node = LeafNode::new(node);
        let key_cnt = leaf_node.key_count();
        for i in 0..key_cnt {
            if Self::compare_key(self.col_type, leaf_node.leaf_key_bytes(i), &key)
                == Ordering::Equal
                && leaf_node.leaf_record_id(i) == rid
            {
                leaf_node.shift_leaf_entries_left(i, key_cnt - 1);
                leaf_node.set_key_count(key_cnt - 1);
                break;
            }
        }
        // TODO: handle underflow/merge if needed.
        Ok(())
    }
}
