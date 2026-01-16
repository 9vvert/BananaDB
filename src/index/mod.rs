use std::cmp::Ordering;

use crate::{
    dbms::{
        cache::{CacheBuf, PageId},
        resource::PageType,
    },
    index::node::{Bound, HeaderPage, INVALID_PAGE, IndexNode, InnerNode, LeafNode, NodeType},
    table::page::record::{ColumnType, ColumnValue, RecordId},
};

pub mod node;

// ====================== B+ Tree ===========================
pub struct BPlusTree<'a, const PAGE_NUM: usize> {
    buf: &'a mut CacheBuf<PAGE_NUM>,
    table_name: String,
    col_idx: usize,
    col_type: ColumnType,
    key_size: usize,
    extra_info: String,
}

impl<'a, const PAGE_NUM: usize> BPlusTree<'a, PAGE_NUM> {
    pub fn new(
        buf: &'a mut CacheBuf<PAGE_NUM>,
        table_name: &str,
        col_idx: usize,
        col_type: ColumnType,
    ) -> Self {
        let key_size = col_type.size();
        let extra_info = col_idx.to_string();
        BPlusTree {
            buf,
            table_name: table_name.to_string(),
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
            .get_page(self.table_name.as_str(), 0, &PageType::INDEX, &self.extra_info);
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
            ColumnType::FLOAT => {
                let va = f64::from_le_bytes(a[..8].try_into().unwrap());
                let vb = f64::from_le_bytes(b[..8].try_into().unwrap());
                va.partial_cmp(&vb).unwrap_or(Ordering::Equal)
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
            (ColumnValue::FLOAT(x), ColumnType::FLOAT) => Ok(x.to_le_bytes().to_vec()),
            _ => Err("Column type mismatch for index key".to_string()),
        }
    }

    // find the left-most leaf node of this B+tree
    fn leftmost_leaf(&mut self, root: u32) -> u32 {
        let mut current = root;
        loop {
            let node_page = self.buf.get_page(
                self.table_name.as_str(),
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
                self.table_name.as_str(),
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
                self.table_name.as_str(),
                page_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            page.set_dirty();
            let node = IndexNode::new(&mut page.data, self.key_size);
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
                self.table_name.as_str(),
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
                self.table_name.as_str(),
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
                self.table_name.as_str(),
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
                self.table_name.as_str(),
                page_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            page.set_dirty();
            let node = IndexNode::new(&mut page.data, self.key_size);
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
                self.table_name.as_str(),
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
                    self.table_name.as_str(),
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
                self.table_name.as_str(),
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
                self.table_name.as_str(),
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
            self.table_name.as_str(),
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
        let mut found = false;
        for i in 0..key_cnt {
            if Self::compare_key(self.col_type, leaf_node.leaf_key_bytes(i), &key)
                == Ordering::Equal
                && leaf_node.leaf_record_id(i) == rid
            {
                leaf_node.shift_leaf_entries_left(i, key_cnt - 1);
                leaf_node.set_key_count(key_cnt - 1);
                found = true;
                break;
            }
        }
        if !found {
            return Ok(());
        }

        let path = {
            let (_, path) = self.find_leaf(&key, root);
            path
        };
        self.rebalance_after_delete(path)?;
        Ok(())
    }

    fn rebalance_after_delete(&mut self, path: Vec<PageId>) -> Result<(), String> {
        if path.len() <= 1 {
            return self.collapse_root_if_needed();
        }

        for depth in (1..path.len()).rev() {
            let child_id = path[depth];
            let parent_id = path[depth - 1];
            self.fix_underflow(parent_id, child_id)?;
        }

        self.collapse_root_if_needed()
    }

    fn fix_underflow(&mut self, parent_id: u32, child_id: u32) -> Result<(), String> {
        let node_type = {
            let child_page = self.buf.get_page(
                self.table_name.as_str(),
                child_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            let node = IndexNode::new(&mut child_page.data, self.key_size);
            node.node_type()
        };

        match node_type {
            NodeType::Leaf => self.fix_leaf_underflow(parent_id, child_id),
            NodeType::Internal => self.fix_internal_underflow(parent_id, child_id),
        }
    }

    fn fix_leaf_underflow(&mut self, parent_id: u32, child_id: u32) -> Result<(), String> {
        let (child_key_cnt, leaf_capacity) = {
            let child_page = self.buf.get_page(
                self.table_name.as_str(),
                child_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            let node = IndexNode::new(&mut child_page.data, self.key_size);
            let leaf_node = LeafNode::new(node);
            (leaf_node.key_count(), leaf_node.leaf_capacity())
        };
        let min_keys = Self::min_keys(leaf_capacity);
        if child_key_cnt >= min_keys {
            return Ok(());
        }

        let (siblings, child_idx) = self.get_parent_children(parent_id, child_id)?;

        if child_idx > 0 {
            let left_id = siblings[child_idx - 1];
            let left_count = self.leaf_key_count(left_id)?;
            if left_count > min_keys {
                self.borrow_from_left_leaf(parent_id, child_idx, left_id, child_id)?;
                return Ok(());
            }
        }

        if child_idx + 1 < siblings.len() {
            let right_id = siblings[child_idx + 1];
            let right_count = self.leaf_key_count(right_id)?;
            if right_count > min_keys {
                self.borrow_from_right_leaf(parent_id, child_idx, right_id, child_id)?;
                return Ok(());
            }
        }

        if child_idx > 0 {
            let left_id = siblings[child_idx - 1];
            self.merge_leaf_into_left(parent_id, child_idx, left_id, child_id)?;
        } else {
            let right_id = siblings[child_idx + 1];
            self.merge_leaf_with_right(parent_id, child_idx, child_id, right_id)?;
        }
        Ok(())
    }

    fn fix_internal_underflow(&mut self, parent_id: u32, child_id: u32) -> Result<(), String> {
        let (child_keys_len, internal_capacity) = {
            let page = self.buf.get_page(
                self.table_name.as_str(),
                child_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            let node = IndexNode::new(&mut page.data, self.key_size);
            let inner = InnerNode::new(node);
            (inner.key_count(), inner.internal_capacity())
        };

        let min_keys = Self::min_keys(internal_capacity);
        if child_keys_len >= min_keys {
            return Ok(());
        }

        let (siblings, child_idx) = self.get_parent_children(parent_id, child_id)?;

        if child_idx > 0 {
            let left_id = siblings[child_idx - 1];
            let left_keys = self.internal_key_count(left_id)?;
            if left_keys > min_keys {
                self.borrow_from_left_internal(parent_id, child_idx, left_id, child_id)?;
                return Ok(());
            }
        }

        if child_idx + 1 < siblings.len() {
            let right_id = siblings[child_idx + 1];
            let right_keys = self.internal_key_count(right_id)?;
            if right_keys > min_keys {
                self.borrow_from_right_internal(parent_id, child_idx, right_id, child_id)?;
                return Ok(());
            }
        }

        if child_idx > 0 {
            let left_id = siblings[child_idx - 1];
            self.merge_internal_into_left(parent_id, child_idx, left_id, child_id)?;
        } else {
            let right_id = siblings[child_idx + 1];
            self.merge_internal_with_right(parent_id, child_idx, child_id, right_id)?;
        }

        Ok(())
    }

    fn min_keys(capacity: usize) -> usize {
        (capacity + 1) / 2
    }

    fn get_parent_children(
        &mut self,
        parent_id: u32,
        child_id: u32,
    ) -> Result<(Vec<u32>, usize), String> {
        let parent_page = self.buf.get_page(
            self.table_name.as_str(),
            parent_id as usize,
            &PageType::INDEX,
            &self.extra_info,
        );
        let node = IndexNode::new(&mut parent_page.data, self.key_size);
        let inner = InnerNode::new(node);
        let (keys, children) = Self::read_internal_layout(&inner);
        let pos = children
            .iter()
            .position(|&v| v == child_id)
            .ok_or_else(|| "Parent-child link broken".to_string())?;
        drop(keys);
        Ok((children, pos))
    }

    fn leaf_key_count(&mut self, page_id: u32) -> Result<usize, String> {
        let page = self.buf.get_page(
            self.table_name.as_str(),
            page_id as usize,
            &PageType::INDEX,
            &self.extra_info,
        );
        let node = IndexNode::new(&mut page.data, self.key_size);
        if node.node_type() != NodeType::Leaf {
            return Err("Sibling is not a leaf".to_string());
        }
        Ok(LeafNode::new(node).key_count())
    }

    fn internal_key_count(&mut self, page_id: u32) -> Result<usize, String> {
        let page = self.buf.get_page(
            self.table_name.as_str(),
            page_id as usize,
            &PageType::INDEX,
            &self.extra_info,
        );
        let node = IndexNode::new(&mut page.data, self.key_size);
        if node.node_type() != NodeType::Internal {
            return Err("Sibling is not internal".to_string());
        }
        Ok(InnerNode::new(node).key_count())
    }

    fn borrow_from_left_leaf(
        &mut self,
        parent_id: u32,
        child_idx: usize,
        left_id: u32,
        child_id: u32,
    ) -> Result<(), String> {
        let (borrow_key, borrow_rid) = {
            let page = self.buf.get_page(
                self.table_name.as_str(),
                left_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            page.set_dirty();
            let node = IndexNode::new(&mut page.data, self.key_size);
            let mut left = LeafNode::new(node);
            let count = left.key_count();
            let key = left.leaf_key_bytes(count - 1).to_vec();
            let rid_val = left.leaf_record_id(count - 1);
            left.set_key_count(count - 1);
            (key, rid_val)
        };

        {
            let page = self.buf.get_page(
                self.table_name.as_str(),
                child_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            page.set_dirty();
            let node = IndexNode::new(&mut page.data, self.key_size);
            let mut leaf = LeafNode::new(node);
            let count = leaf.key_count();
            leaf.shift_leaf_entries(0, count);
            leaf.write_leaf_entry(0, &borrow_key, borrow_rid);
            leaf.set_key_count(count + 1);
        }

        {
            let page = self.buf.get_page(
                self.table_name.as_str(),
                parent_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            page.set_dirty();
            let node = IndexNode::new(&mut page.data, self.key_size);
            let mut inner = InnerNode::new(node);
            let right_child = inner.internal_child_at(child_idx);
            inner.write_internal_entry(child_idx - 1, &borrow_key, right_child);
        }
        Ok(())
    }

    fn borrow_from_right_leaf(
        &mut self,
        parent_id: u32,
        child_idx: usize,
        right_id: u32,
        child_id: u32,
    ) -> Result<(), String> {
        let (borrow_key, borrow_rid, new_sep) = {
            let page = self.buf.get_page(
                self.table_name.as_str(),
                right_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            page.set_dirty();
            let node = IndexNode::new(&mut page.data, self.key_size);
            let mut right = LeafNode::new(node);
            let count = right.key_count();
            let key = right.leaf_key_bytes(0).to_vec();
            let rid_val = right.leaf_record_id(0);
            right.shift_leaf_entries_left(0, count - 1);
            right.set_key_count(count - 1);
            let sep = right.leaf_key_bytes(0).to_vec();
            (key, rid_val, sep)
        };

        {
            let page = self.buf.get_page(
                self.table_name.as_str(),
                child_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            page.set_dirty();
            let node = IndexNode::new(&mut page.data, self.key_size);
            let mut leaf = LeafNode::new(node);
            let count = leaf.key_count();
            leaf.write_leaf_entry(count, &borrow_key, borrow_rid);
            leaf.set_key_count(count + 1);
        }

        {
            let page = self.buf.get_page(
                self.table_name.as_str(),
                parent_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            page.set_dirty();
            let node = IndexNode::new(&mut page.data, self.key_size);
            let mut inner = InnerNode::new(node);
            let right_child = inner.internal_child_at(child_idx + 1);
            inner.write_internal_entry(child_idx, &new_sep, right_child);
        }
        Ok(())
    }

    fn merge_leaf_into_left(
        &mut self,
        parent_id: u32,
        child_idx: usize,
        left_id: u32,
        child_id: u32,
    ) -> Result<(), String> {
        let (move_buf, move_cnt, next_leaf) = {
            let page = self.buf.get_page(
                self.table_name.as_str(),
                child_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            let node = IndexNode::new(&mut page.data, self.key_size);
            let leaf = LeafNode::new(node);
            let count = leaf.key_count();
            let size = leaf.leaf_entry_size();
            let mut buf = vec![0u8; count * size];
            let src_off = leaf.leaf_entry_offset(0);
            let move_len = buf.len();
            buf.copy_from_slice(&leaf.data[src_off..src_off + move_len]);
            (buf, count, leaf.next_leaf())
        };

        {
            let page = self.buf.get_page(
                self.table_name.as_str(),
                left_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            page.set_dirty();
            let node = IndexNode::new(&mut page.data, self.key_size);
            let mut leaf = LeafNode::new(node);
            let left_cnt = leaf.key_count();
            let dst_off = leaf.leaf_entry_offset(left_cnt);
            leaf.data[dst_off..dst_off + move_buf.len()].copy_from_slice(&move_buf);
            leaf.set_key_count(left_cnt + move_cnt);
            leaf.set_next_leaf(next_leaf);
        }

        self.remove_parent_entry(parent_id, child_idx - 1)?;
        Ok(())
    }

    fn merge_leaf_with_right(
        &mut self,
        parent_id: u32,
        child_idx: usize,
        child_id: u32,
        right_id: u32,
    ) -> Result<(), String> {
        let (move_buf, move_cnt, next_leaf) = {
            let page = self.buf.get_page(
                self.table_name.as_str(),
                right_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            let node = IndexNode::new(&mut page.data, self.key_size);
            let leaf = LeafNode::new(node);
            let count = leaf.key_count();
            let size = leaf.leaf_entry_size();
            let mut buf = vec![0u8; count * size];
            let src_off = leaf.leaf_entry_offset(0);
            let move_len = buf.len();
            buf.copy_from_slice(&leaf.data[src_off..src_off + move_len]);
            (buf, count, leaf.next_leaf())
        };

        {
            let page = self.buf.get_page(
                self.table_name.as_str(),
                child_id as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            page.set_dirty();
            let node = IndexNode::new(&mut page.data, self.key_size);
            let mut leaf = LeafNode::new(node);
            let child_cnt = leaf.key_count();
            let dst_off = leaf.leaf_entry_offset(child_cnt);
            leaf.data[dst_off..dst_off + move_buf.len()].copy_from_slice(&move_buf);
            leaf.set_key_count(child_cnt + move_cnt);
            leaf.set_next_leaf(next_leaf);
        }

        self.remove_parent_entry(parent_id, child_idx)?;
        Ok(())
    }

    fn borrow_from_left_internal(
        &mut self,
        parent_id: u32,
        child_idx: usize,
        left_id: u32,
        child_id: u32,
    ) -> Result<(), String> {
        let separator = self.parent_key(parent_id, child_idx - 1)?;

        let (mut left_keys, mut left_children) = self.read_internal_page(left_id)?;
        let borrowed_key = left_keys.pop().unwrap();
        let borrowed_child = left_children.pop().unwrap();

        let (mut child_keys, mut child_children) = self.read_internal_page(child_id)?;
        child_keys.insert(0, separator);
        child_children.insert(0, borrowed_child);

        self.write_internal_page(left_id, left_keys, left_children)?;
        self.write_internal_page(child_id, child_keys, child_children)?;
        self.update_parent_key(parent_id, child_idx - 1, borrowed_key)
    }

    fn borrow_from_right_internal(
        &mut self,
        parent_id: u32,
        child_idx: usize,
        right_id: u32,
        child_id: u32,
    ) -> Result<(), String> {
        let separator = self.parent_key(parent_id, child_idx)?;

        let (mut right_keys, mut right_children) = self.read_internal_page(right_id)?;
        let borrowed_key = right_keys.remove(0);
        let borrowed_child = right_children.remove(0);

        let (mut child_keys, mut child_children) = self.read_internal_page(child_id)?;
        child_keys.push(separator);
        child_children.push(borrowed_child);

        self.write_internal_page(right_id, right_keys, right_children)?;
        self.write_internal_page(child_id, child_keys, child_children)?;
        self.update_parent_key(parent_id, child_idx, borrowed_key)
    }

    fn merge_internal_into_left(
        &mut self,
        parent_id: u32,
        child_idx: usize,
        left_id: u32,
        child_id: u32,
    ) -> Result<(), String> {
        let separator = self.parent_key(parent_id, child_idx - 1)?;
        let (mut left_keys, mut left_children) = self.read_internal_page(left_id)?;
        let (child_keys, child_children) = self.read_internal_page(child_id)?;

        left_keys.push(separator);
        left_keys.extend(child_keys);
        left_children.extend(child_children);

        self.write_internal_page(left_id, left_keys, left_children)?;
        self.remove_parent_entry(parent_id, child_idx - 1)
    }

    fn merge_internal_with_right(
        &mut self,
        parent_id: u32,
        child_idx: usize,
        child_id: u32,
        right_id: u32,
    ) -> Result<(), String> {
        let separator = self.parent_key(parent_id, child_idx)?;
        let (mut child_keys, mut child_children) = self.read_internal_page(child_id)?;
        let (right_keys, right_children) = self.read_internal_page(right_id)?;

        child_keys.push(separator);
        child_keys.extend(right_keys);
        child_children.extend(right_children);

        self.write_internal_page(child_id, child_keys, child_children)?;
        self.remove_parent_entry(parent_id, child_idx)
    }

    fn parent_key(&mut self, parent_id: u32, key_idx: usize) -> Result<Vec<u8>, String> {
        let page = self.buf.get_page(
            self.table_name.as_str(),
            parent_id as usize,
            &PageType::INDEX,
            &self.extra_info,
        );
        let node = IndexNode::new(&mut page.data, self.key_size);
        let inner = InnerNode::new(node);
        Ok(inner.internal_key_bytes(key_idx).to_vec())
    }

    fn remove_parent_entry(&mut self, parent_id: u32, remove_idx: usize) -> Result<(), String> {
        let page = self.buf.get_page(
            self.table_name.as_str(),
            parent_id as usize,
            &PageType::INDEX,
            &self.extra_info,
        );
        page.set_dirty();
        let node = IndexNode::new(&mut page.data, self.key_size);
        let mut inner = InnerNode::new(node);
        let (mut keys, mut children) = Self::read_internal_layout(&inner);
        keys.remove(remove_idx);
        children.remove(remove_idx + 1);
        Self::write_internal_layout(&mut inner, &keys, &children);
        Ok(())
    }

    fn update_parent_key(
        &mut self,
        parent_id: u32,
        key_idx: usize,
        new_key: Vec<u8>,
    ) -> Result<(), String> {
        let page = self.buf.get_page(
            self.table_name.as_str(),
            parent_id as usize,
            &PageType::INDEX,
            &self.extra_info,
        );
        page.set_dirty();
        let node = IndexNode::new(&mut page.data, self.key_size);
        let mut inner = InnerNode::new(node);
        let (mut keys, children) = Self::read_internal_layout(&inner);
        keys[key_idx] = new_key;
        Self::write_internal_layout(&mut inner, &keys, &children);
        Ok(())
    }

    fn read_internal_page(&mut self, page_id: u32) -> Result<(Vec<Vec<u8>>, Vec<u32>), String> {
        let page = self.buf.get_page(
            self.table_name.as_str(),
            page_id as usize,
            &PageType::INDEX,
            &self.extra_info,
        );
        let node = IndexNode::new(&mut page.data, self.key_size);
        if node.node_type() != NodeType::Internal {
            return Err("Node type mismatch".to_string());
        }
        let inner = InnerNode::new(node);
        Ok(Self::read_internal_layout(&inner))
    }

    fn write_internal_page(
        &mut self,
        page_id: u32,
        keys: Vec<Vec<u8>>,
        children: Vec<u32>,
    ) -> Result<(), String> {
        let page = self.buf.get_page(
            self.table_name.as_str(),
            page_id as usize,
            &PageType::INDEX,
            &self.extra_info,
        );
        page.set_dirty();
        let node = IndexNode::new(&mut page.data, self.key_size);
        if node.node_type() != NodeType::Internal {
            return Err("Node type mismatch".to_string());
        }
        let mut inner = InnerNode::new(node);
        Self::write_internal_layout(&mut inner, &keys, &children);
        Ok(())
    }

    fn read_internal_layout(inner: &InnerNode<'_>) -> (Vec<Vec<u8>>, Vec<u32>) {
        let key_cnt = inner.key_count();
        let mut keys = Vec::with_capacity(key_cnt);
        let mut children = Vec::with_capacity(key_cnt + 1);
        children.push(inner.internal_child_at(0));
        for i in 0..key_cnt {
            keys.push(inner.internal_key_bytes(i).to_vec());
            children.push(inner.internal_child_at(i + 1));
        }
        (keys, children)
    }

    fn write_internal_layout(inner: &mut InnerNode<'_>, keys: &[Vec<u8>], children: &[u32]) {
        assert_eq!(children.len(), keys.len() + 1);
        inner.set_key_count(keys.len());
        inner.set_internal_child0(children[0]);
        for (i, key) in keys.iter().enumerate() {
            inner.write_internal_entry(i, key, children[i + 1]);
        }
    }

    fn collapse_root_if_needed(&mut self) -> Result<(), String> {
        let root = self.root_page_id();
        if root == INVALID_PAGE {
            return Ok(());
        }

        let (root_type, key_count, child0) = {
            let page = self.buf.get_page(
                self.table_name.as_str(),
                root as usize,
                &PageType::INDEX,
                &self.extra_info,
            );
            let node = IndexNode::new(&mut page.data, self.key_size);
            match node.node_type() {
                NodeType::Leaf => return Ok(()),
                NodeType::Internal => {
                    let inner = InnerNode::new(node);
                    (
                        NodeType::Internal,
                        inner.key_count(),
                        inner.internal_child_at(0),
                    )
                }
            }
        };

        if root_type == NodeType::Internal && key_count == 0 {
            self.set_root_page(child0);
        }
        Ok(())
    }
}
