use crate::node::{Node, NodePtr, NodeType};
use crate::BPlusTreeConfig;
use std::cell::RefCell;
use std::rc::Rc;

/// B+ 树错误类型
#[derive(Debug, Clone, PartialEq)]
pub enum BPlusTreeError {
    KeyNotFound,
    DuplicateKey,
    InvalidOperation,
}

impl std::fmt::Display for BPlusTreeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BPlusTreeError::KeyNotFound => write!(f, "Key not found"),
            BPlusTreeError::DuplicateKey => write!(f, "Duplicate key"),
            BPlusTreeError::InvalidOperation => write!(f, "Invalid operation"),
        }
    }
}

impl std::error::Error for BPlusTreeError {}

/// B+ 树数据结构
pub struct BPlusTree<K, V> {
    root: Option<NodePtr<K, V>>,
    config: BPlusTreeConfig,
}

impl<K, V> BPlusTree<K, V>
where
    K: Ord + Clone + Debug,
    V: Clone + Debug,
{
    /// 创建新的 B+ 树
    pub fn new(config: BPlusTreeConfig) -> Self {
        Self {
            root: None,
            config,
        }
    }

    /// 创建默认配置的 B+ 树
    pub fn new_default() -> Self {
        Self::new(BPlusTreeConfig::default())
    }

    /// 查找键对应的值
    pub fn get(&self, key: &K) -> Option<V> {
        let root = self.root.as_ref()?;
        self.search_leaf(root, key)
            .and_then(|leaf| {
                let leaf_ref = leaf.borrow();
                if let NodeType::Leaf { entries, .. } = &leaf_ref.node_type {
                    entries
                        .binary_search_by(|(k, _)| k.cmp(key))
                        .ok()
                        .map(|idx| entries[idx].1.clone())
                } else {
                    None
                }
            })
    }

    /// 插入键值对
    pub fn insert(&mut self, key: K, value: V) -> Result<(), BPlusTreeError> {
        if self.root.is_none() {
            // 创建根节点（叶子节点）
            let root = Rc::new(RefCell::new(Node::new_leaf()));
            if let NodeType::Leaf { ref mut entries, .. } = root.borrow_mut().node_type {
                entries.push((key, value));
            }
            self.root = Some(root);
            return Ok(());
        }

        let root = self.root.as_ref().unwrap().clone();
        let leaf = self.search_leaf(&root, &key);

        if let Some(leaf) = leaf {
            let mut leaf_ref = leaf.borrow_mut();
            if let NodeType::Leaf { ref mut entries, .. } = leaf_ref.node_type {
                // 检查键是否已存在
                match entries.binary_search_by(|(k, _)| k.cmp(&key)) {
                    Ok(_) => return Err(BPlusTreeError::DuplicateKey),
                    Err(pos) => entries.insert(pos, (key, value)),
                }
            }

            // 检查是否需要分裂
            if leaf_ref.is_full(self.config.min_order) {
                drop(leaf_ref);
                self.split_leaf(leaf)?;
            }
        }

        Ok(())
    }

    /// 删除键值对
    pub fn delete(&mut self, key: &K) -> Result<(), BPlusTreeError> {
        let root = self.root.as_ref().ok_or(BPlusTreeError::KeyNotFound)?;
        let leaf = self.search_leaf(root, key).ok_or(BPlusTreeError::KeyNotFound)?;

        let mut leaf_ref = leaf.borrow_mut();
        let is_root = self.root.as_ref().map(|r| Rc::ptr_eq(r, &leaf)).unwrap_or(false);
        let mut is_empty_after_delete = false;
        
        if let NodeType::Leaf { ref mut entries, .. } = leaf_ref.node_type {
            let pos = entries
                .binary_search_by(|(k, _)| k.cmp(key))
                .map_err(|_| BPlusTreeError::KeyNotFound)?;
            entries.remove(pos);
            is_empty_after_delete = entries.is_empty();
        }
        drop(leaf_ref);

        // 如果删除后节点为空且是根节点，将树设为空
        if is_empty_after_delete && is_root {
            self.root = None;
            return Ok(());
        }

        // 检查是否需要合并或借用
        let leaf_ref = leaf.borrow();
        let needs_underflow = leaf_ref.is_underflow(self.config.min_order);
        drop(leaf_ref);
        
        if needs_underflow && !is_root {
            self.handle_underflow_leaf(leaf)?;
        }

        Ok(())
    }

    /// 范围查询：返回 [start, end) 范围内的所有键值对
    pub fn range_query(&self, start: &K, end: &K) -> Vec<(K, V)> {
        let mut result = Vec::new();
        let root = match self.root.as_ref() {
            Some(r) => r,
            None => return result,
        };

        // 找到起始键所在的叶子节点
        let mut current_leaf = self.search_leaf(root, start);

        while let Some(leaf) = current_leaf {
            let leaf_ref = leaf.borrow();
            if let NodeType::Leaf { entries, next } = &leaf_ref.node_type {
                for (k, v) in entries {
                    if k >= start && k < end {
                        result.push((k.clone(), v.clone()));
                    } else if k >= end {
                        return result;
                    }
                }
                current_leaf = next.clone();
            } else {
                break;
            }
        }

        result
    }

    /// 获取所有键值对（按顺序）
    pub fn iter(&self) -> BPlusTreeIterator<K, V> {
        let first_leaf = self.root.as_ref().and_then(|root| {
            let mut current = root.clone();
            loop {
                let node_ref = current.borrow();
                if node_ref.is_leaf() {
                    return Some(current.clone());
                }
                if let NodeType::Internal { children, .. } = &node_ref.node_type {
                    if children.is_empty() {
                        return None;
                    }
                    let next = children[0].clone();
                    drop(node_ref);
                    current = next;
                } else {
                    return None;
                }
            }
        });

        BPlusTreeIterator {
            current_leaf: first_leaf,
            current_index: 0,
        }
    }

    /// 搜索包含指定键的叶子节点
    fn search_leaf(&self, root: &NodePtr<K, V>, key: &K) -> Option<NodePtr<K, V>> {
        let mut current = root.clone();

        loop {
            let current_ref = current.borrow();
            if current_ref.is_leaf() {
                return Some(current.clone());
            }

            if let NodeType::Internal { keys, children } = &current_ref.node_type {
                // 在 B+ 树中，内部节点的键表示"大于等于这个键的记录在右子树中"
                // 所以我们需要找到第一个大于 key 的键的位置
                let pos = keys.binary_search(key)
                    .map(|p| p + 1)  // 如果找到，去右子树
                    .unwrap_or_else(|x| x);  // 如果没找到，去应该插入位置的左子树
                // 确保 pos 不超过 children 的长度
                let pos = pos.min(children.len().saturating_sub(1));
                let next = children[pos].clone();
                drop(current_ref);
                current = next;
            } else {
                return None;
            }
        }
    }

    /// 分裂叶子节点
    fn split_leaf(&mut self, leaf: NodePtr<K, V>) -> Result<(), BPlusTreeError> {
        let parent;
        let mid_key;
        let new_leaf;

        {
            // 先获取 parent
            parent = leaf.borrow().parent.clone();
            
            let mut leaf_ref = leaf.borrow_mut();
            if let NodeType::Leaf { entries, next } = &mut leaf_ref.node_type {
                let mid = entries.len() / 2;
                mid_key = entries[mid].0.clone();

                // 创建新叶子节点
                let new_leaf_node = Rc::new(RefCell::new(Node::new_leaf()));
                if let NodeType::Leaf {
                    entries: new_entries,
                    next: new_next,
                } = &mut new_leaf_node.borrow_mut().node_type
                {
                    *new_entries = entries.split_off(mid);
                    *new_next = next.take();
                }
                new_leaf_node.borrow_mut().parent = parent.clone();
                new_leaf = new_leaf_node;

                // 更新原叶子节点的 next 指针
                *next = Some(new_leaf.clone());
            } else {
                return Err(BPlusTreeError::InvalidOperation);
            }
        }

        // 将中间键插入父节点
        if let Some(parent_ptr) = parent {
            self.insert_into_parent(&leaf, mid_key, &new_leaf, &parent_ptr)?;
        } else {
            // 创建新的根节点
            let new_root = Rc::new(RefCell::new(Node::new_internal()));
            if let NodeType::Internal {
                ref mut keys,
                ref mut children,
            } = new_root.borrow_mut().node_type
            {
                keys.push(mid_key);
                children.push(leaf.clone());
                children.push(new_leaf.clone());
            }
            leaf.borrow_mut().parent = Some(new_root.clone());
            new_leaf.borrow_mut().parent = Some(new_root.clone());
            self.root = Some(new_root);
        }

        Ok(())
    }

    /// 将键插入父节点
    fn insert_into_parent(
        &mut self,
        left: &NodePtr<K, V>,
        key: K,
        right: &NodePtr<K, V>,
        parent: &NodePtr<K, V>,
    ) -> Result<(), BPlusTreeError> {
        let mut parent_ref = parent.borrow_mut();
        if let NodeType::Internal {
            ref mut keys,
            ref mut children,
        } = parent_ref.node_type
        {
            // 找到 left 在 children 中的位置
            let pos = children
                .iter()
                .position(|c| Rc::ptr_eq(c, left))
                .ok_or(BPlusTreeError::InvalidOperation)?;
            keys.insert(pos, key);
            children.insert(pos + 1, right.clone());
            right.borrow_mut().parent = Some(parent.clone());
        }

        // 检查父节点是否需要分裂
        if parent_ref.is_full(self.config.min_order) {
            drop(parent_ref);
            self.split_internal(parent.clone())?;
        }

        Ok(())
    }

    /// 分裂内部节点
    fn split_internal(&mut self, node: NodePtr<K, V>) -> Result<(), BPlusTreeError> {
        let parent;
        let mid_key;
        let new_node;

        {
            let mut node_ref = node.borrow_mut();
            if let NodeType::Internal {
                ref mut keys,
                ref mut children,
            } = node_ref.node_type
            {
                let mid = keys.len() / 2;
                mid_key = keys[mid].clone();

                // 创建新内部节点
                let new_node_ptr = Rc::new(RefCell::new(Node::new_internal()));
                if let NodeType::Internal {
                    keys: new_keys,
                    children: new_children,
                } = &mut new_node_ptr.borrow_mut().node_type
                {
                    *new_keys = keys.split_off(mid + 1);
                    *new_children = children.split_off(mid + 1);

                    // 更新新节点子节点的父指针
                    for child in new_children.iter() {
                        child.borrow_mut().parent = Some(new_node_ptr.clone());
                    }
                }
                new_node_ptr.borrow_mut().parent = node_ref.parent.clone();
                new_node = new_node_ptr;
            } else {
                return Err(BPlusTreeError::InvalidOperation);
            }
            parent = node_ref.parent.clone();
        }

        // 将中间键插入父节点
        if let Some(parent_ptr) = parent {
            self.insert_into_parent(&node, mid_key, &new_node, &parent_ptr)?;
        } else {
            // 创建新的根节点
            let new_root = Rc::new(RefCell::new(Node::new_internal()));
            if let NodeType::Internal {
                ref mut keys,
                ref mut children,
            } = new_root.borrow_mut().node_type
            {
                keys.push(mid_key);
                children.push(node.clone());
                children.push(new_node.clone());
            }
            node.borrow_mut().parent = Some(new_root.clone());
            new_node.borrow_mut().parent = Some(new_root.clone());
            self.root = Some(new_root);
        }

        Ok(())
    }

    /// 处理叶子节点下溢
    fn handle_underflow_leaf(&mut self, leaf: NodePtr<K, V>) -> Result<(), BPlusTreeError> {
        let parent = leaf.borrow().parent.clone();
        if parent.is_none() {
            // 根节点，允许少于 min_order 个键
            return Ok(());
        }

        let parent_ptr = parent.unwrap();
        let parent_ref = parent_ptr.borrow();
        let (pos, left_sibling_opt, right_sibling_opt) = if let NodeType::Internal { children, .. } = &parent_ref.node_type {
            let pos = children
                .iter()
                .position(|c| Rc::ptr_eq(c, &leaf))
                .ok_or(BPlusTreeError::InvalidOperation)?;
            
            let left_sibling = if pos > 0 { Some(children[pos - 1].clone()) } else { None };
            let right_sibling = if pos < children.len() - 1 { Some(children[pos + 1].clone()) } else { None };
            (pos, left_sibling, right_sibling)
        } else {
            return Err(BPlusTreeError::InvalidOperation);
        };
        drop(parent_ref);

        // 尝试从左兄弟借用
        if let Some(left_sibling) = &left_sibling_opt {
            let left_ref = left_sibling.borrow();
            if let NodeType::Leaf { entries, .. } = &left_ref.node_type {
                if entries.len() > self.config.min_order {
                    drop(left_ref);
                    self.borrow_from_left_leaf(&leaf, left_sibling, &parent_ptr, pos)?;
                    return Ok(());
                }
            }
        }

        // 尝试从右兄弟借用
        if let Some(right_sibling) = &right_sibling_opt {
            let right_ref = right_sibling.borrow();
            if let NodeType::Leaf { entries, .. } = &right_ref.node_type {
                if entries.len() > self.config.min_order {
                    drop(right_ref);
                    self.borrow_from_right_leaf(&leaf, right_sibling, &parent_ptr, pos)?;
                    return Ok(());
                }
            }
        }

        // 无法借用，需要合并
        if pos > 0 {
            let left = left_sibling_opt.unwrap();
            self.merge_leaves(&left, &leaf, &parent_ptr, pos - 1)?;
        } else if let Some(right) = right_sibling_opt {
            self.merge_leaves(&leaf, &right, &parent_ptr, pos)?;
        }

        Ok(())
    }

    /// 从左兄弟叶子节点借用
    fn borrow_from_left_leaf(
        &self,
        leaf: &NodePtr<K, V>,
        left: &NodePtr<K, V>,
        parent: &NodePtr<K, V>,
        pos: usize,
    ) -> Result<(), BPlusTreeError> {
        let mut left_ref = left.borrow_mut();
        let mut leaf_ref = leaf.borrow_mut();
        let mut parent_ref = parent.borrow_mut();

        if let (
            NodeType::Leaf {
                entries: left_entries,
                ..
            },
            NodeType::Leaf { entries, .. },
            NodeType::Internal { keys, .. },
        ) = (
            &mut left_ref.node_type,
            &mut leaf_ref.node_type,
            &mut parent_ref.node_type,
        ) {
            if let Some((key, value)) = left_entries.pop() {
                entries.insert(0, (key.clone(), value));
                keys[pos - 1] = key;
            }
        }

        Ok(())
    }

    /// 从右兄弟叶子节点借用
    fn borrow_from_right_leaf(
        &self,
        leaf: &NodePtr<K, V>,
        right: &NodePtr<K, V>,
        parent: &NodePtr<K, V>,
        pos: usize,
    ) -> Result<(), BPlusTreeError> {
        let mut leaf_ref = leaf.borrow_mut();
        let mut right_ref = right.borrow_mut();
        let mut parent_ref = parent.borrow_mut();

        if let (
            NodeType::Leaf { entries, .. },
            NodeType::Leaf {
                entries: right_entries,
                ..
            },
            NodeType::Internal { keys, .. },
        ) = (
            &mut leaf_ref.node_type,
            &mut right_ref.node_type,
            &mut parent_ref.node_type,
        ) {
            if !right_entries.is_empty() {
                let (key, value) = right_entries.remove(0);
                entries.push((key.clone(), value));
                if !right_entries.is_empty() {
                    keys[pos] = right_entries[0].0.clone();
                }
            }
        }

        Ok(())
    }

    /// 合并两个叶子节点
    fn merge_leaves(
        &mut self,
        left: &NodePtr<K, V>,
        right: &NodePtr<K, V>,
        parent: &NodePtr<K, V>,
        pos: usize,
    ) -> Result<(), BPlusTreeError> {
        let mut left_ref = left.borrow_mut();
        let mut right_ref = right.borrow_mut();
        let mut parent_ref = parent.borrow_mut();

        if let (
            NodeType::Leaf {
                entries: left_entries,
                next: left_next,
            },
            NodeType::Leaf {
                entries: right_entries,
                next: right_next,
            },
            NodeType::Internal { keys, children },
        ) = (
            &mut left_ref.node_type,
            &mut right_ref.node_type,
            &mut parent_ref.node_type,
        ) {
            left_entries.append(right_entries);
            *left_next = right_next.take();
            keys.remove(pos);
            children.remove(pos + 1);
        }

        drop(left_ref);
        drop(parent_ref);

        // 检查父节点是否需要处理下溢
        let parent_ref = parent.borrow();
        let is_root = self.root.as_ref().map(|r| Rc::ptr_eq(r, parent)).unwrap_or(false);
        let needs_underflow = parent_ref.is_underflow(self.config.min_order);
        drop(parent_ref);
        
        if needs_underflow && !is_root {
            self.handle_underflow_internal(parent.clone())?;
        }

        Ok(())
    }

    /// 处理内部节点下溢
    fn handle_underflow_internal(&mut self, node: NodePtr<K, V>) -> Result<(), BPlusTreeError> {
        let parent = node.borrow().parent.clone();
        if parent.is_none() {
            // 如果根节点只有一个子节点，将其提升为新的根
            let node_ref = node.borrow();
            if let NodeType::Internal { children, .. } = &node_ref.node_type {
                if children.len() == 1 {
                    let new_root = children[0].clone();
                    new_root.borrow_mut().parent = None;
                    self.root = Some(new_root);
                    return Ok(());
                } else if children.is_empty() {
                    // 如果根节点没有子节点，将树设为空
                    self.root = None;
                    return Ok(());
                }
            }
            return Ok(());
        }

        let parent_ptr = parent.unwrap();
        let parent_ref = parent_ptr.borrow();
        let (pos, left_sibling_opt, right_sibling_opt) = if let NodeType::Internal { children, .. } = &parent_ref.node_type {
            let pos = children
                .iter()
                .position(|c| Rc::ptr_eq(c, &node))
                .ok_or(BPlusTreeError::InvalidOperation)?;
            
            let left_sibling = if pos > 0 { Some(children[pos - 1].clone()) } else { None };
            let right_sibling = if pos < children.len() - 1 { Some(children[pos + 1].clone()) } else { None };
            (pos, left_sibling, right_sibling)
        } else {
            return Err(BPlusTreeError::InvalidOperation);
        };
        drop(parent_ref);

        // 尝试从左兄弟借用
        if let Some(left_sibling) = &left_sibling_opt {
            let left_ref = left_sibling.borrow();
            if let NodeType::Internal { keys: left_keys, .. } = &left_ref.node_type {
                if left_keys.len() > self.config.min_order {
                    drop(left_ref);
                    self.borrow_from_left_internal(&node, left_sibling, &parent_ptr, pos)?;
                    return Ok(());
                }
            }
        }

        // 尝试从右兄弟借用
        if let Some(right_sibling) = &right_sibling_opt {
            let right_ref = right_sibling.borrow();
            if let NodeType::Internal { keys: right_keys, .. } = &right_ref.node_type {
                if right_keys.len() > self.config.min_order {
                    drop(right_ref);
                    self.borrow_from_right_internal(&node, right_sibling, &parent_ptr, pos)?;
                    return Ok(());
                }
            }
        }

        // 无法借用，需要合并
        if pos > 0 {
            let left = left_sibling_opt.unwrap();
            self.merge_internal(&left, &node, &parent_ptr, pos - 1)?;
        } else if let Some(right) = right_sibling_opt {
            self.merge_internal(&node, &right, &parent_ptr, pos)?;
        }

        Ok(())
    }

    /// 从左兄弟内部节点借用
    fn borrow_from_left_internal(
        &self,
        node: &NodePtr<K, V>,
        left: &NodePtr<K, V>,
        parent: &NodePtr<K, V>,
        pos: usize,
    ) -> Result<(), BPlusTreeError> {
        let mut left_ref = left.borrow_mut();
        let mut node_ref = node.borrow_mut();
        let mut parent_ref = parent.borrow_mut();

        if let (
            NodeType::Internal {
                keys: left_keys,
                children: left_children,
            },
            NodeType::Internal { keys, children },
            NodeType::Internal { keys: parent_keys, .. },
        ) = (
            &mut left_ref.node_type,
            &mut node_ref.node_type,
            &mut parent_ref.node_type,
        ) {
            if let Some(last_key) = left_keys.pop() {
                let last_child = left_children.pop().unwrap();
                let parent_key = parent_keys[pos - 1].clone();
                keys.insert(0, parent_key);
                children.insert(0, last_child.clone());
                last_child.borrow_mut().parent = Some(node.clone());
                parent_keys[pos - 1] = last_key;
            }
        }

        Ok(())
    }

    /// 从右兄弟内部节点借用
    fn borrow_from_right_internal(
        &self,
        node: &NodePtr<K, V>,
        right: &NodePtr<K, V>,
        parent: &NodePtr<K, V>,
        pos: usize,
    ) -> Result<(), BPlusTreeError> {
        let mut node_ref = node.borrow_mut();
        let mut right_ref = right.borrow_mut();
        let mut parent_ref = parent.borrow_mut();

        if let (
            NodeType::Internal { keys, children },
            NodeType::Internal {
                keys: right_keys,
                children: right_children,
            },
            NodeType::Internal { keys: parent_keys, .. },
        ) = (
            &mut node_ref.node_type,
            &mut right_ref.node_type,
            &mut parent_ref.node_type,
        ) {
            if !right_keys.is_empty() {
                let first_key = right_keys.remove(0);
                let first_child = right_children.remove(0);
                let parent_key = parent_keys[pos].clone();
                keys.push(parent_key);
                children.push(first_child.clone());
                first_child.borrow_mut().parent = Some(node.clone());
                parent_keys[pos] = first_key;
            }
        }

        Ok(())
    }

    /// 合并两个内部节点
    fn merge_internal(
        &mut self,
        left: &NodePtr<K, V>,
        right: &NodePtr<K, V>,
        parent: &NodePtr<K, V>,
        pos: usize,
    ) -> Result<(), BPlusTreeError> {
        let mut left_ref = left.borrow_mut();
        let mut right_ref = right.borrow_mut();
        let mut parent_ref = parent.borrow_mut();

        if let (
            NodeType::Internal {
                keys: left_keys,
                children: left_children,
            },
            NodeType::Internal {
                keys: right_keys,
                children: right_children,
            },
            NodeType::Internal { keys: parent_keys, children },
        ) = (
            &mut left_ref.node_type,
            &mut right_ref.node_type,
            &mut parent_ref.node_type,
        ) {
            let parent_key = parent_keys.remove(pos);
            left_keys.push(parent_key);
            left_keys.append(right_keys);
            for child in right_children.iter() {
                child.borrow_mut().parent = Some(left.clone());
            }
            left_children.append(right_children);
            children.remove(pos + 1);
        }

        drop(left_ref);
        drop(parent_ref);

        // 检查父节点是否需要处理下溢
        let parent_ref = parent.borrow();
        let is_root = self.root.as_ref().map(|r| Rc::ptr_eq(r, parent)).unwrap_or(false);
        let needs_underflow = parent_ref.is_underflow(self.config.min_order);
        drop(parent_ref);
        
        if needs_underflow && !is_root {
            self.handle_underflow_internal(parent.clone())?;
        }

        Ok(())
    }
}

/// B+ 树迭代器
pub struct BPlusTreeIterator<K, V> {
    current_leaf: Option<NodePtr<K, V>>,
    current_index: usize,
}

impl<K, V> Iterator for BPlusTreeIterator<K, V>
where
    K: Ord + Clone + Debug,
    V: Clone + Debug,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let leaf = self.current_leaf.clone()?;
            let leaf_ref = leaf.borrow();
            if let NodeType::Leaf { entries, next } = &leaf_ref.node_type {
                if self.current_index < entries.len() {
                    let result = entries[self.current_index].clone();
                    self.current_index += 1;
                    return Some(result);
                } else {
                    // 移动到下一个叶子节点
                    let next_leaf = next.clone();
                    drop(leaf_ref);
                    self.current_leaf = next_leaf;
                    self.current_index = 0;
                }
            } else {
                return None;
            }
        }
    }
}

use std::fmt::Debug;
