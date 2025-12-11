use std::fmt::Debug;

/// B+ 树节点类型
#[derive(Debug, Clone)]
pub enum NodeType<K, V> {
    /// 内部节点：只存储键和子节点指针
    Internal {
        keys: Vec<K>,
        children: Vec<NodePtr<K, V>>,
    },
    /// 叶子节点：存储键值对，并有指向下一个叶子节点的指针
    Leaf {
        entries: Vec<(K, V)>,
        next: Option<NodePtr<K, V>>,
    },
}

/// 节点指针（使用 Rc 和 RefCell 实现共享所有权和内部可变性）
pub type NodePtr<K, V> = std::rc::Rc<std::cell::RefCell<Node<K, V>>>;

/// B+ 树节点
#[derive(Debug, Clone)]
pub struct Node<K, V> {
    pub node_type: NodeType<K, V>,
    pub parent: Option<NodePtr<K, V>>,
}

impl<K, V> Node<K, V>
where
    K: Ord + Clone + Debug,
    V: Clone + Debug,
{
    /// 创建新的内部节点
    pub fn new_internal() -> Self {
        Self {
            node_type: NodeType::Internal {
                keys: Vec::new(),
                children: Vec::new(),
            },
            parent: None,
        }
    }

    /// 创建新的叶子节点
    pub fn new_leaf() -> Self {
        Self {
            node_type: NodeType::Leaf {
                entries: Vec::new(),
                next: None,
            },
            parent: None,
        }
    }

    /// 检查是否为叶子节点
    pub fn is_leaf(&self) -> bool {
        matches!(self.node_type, NodeType::Leaf { .. })
    }

    /// 检查是否为内部节点
    pub fn is_internal(&self) -> bool {
        matches!(self.node_type, NodeType::Internal { .. })
    }

    /// 获取键的数量
    pub fn key_count(&self) -> usize {
        match &self.node_type {
            NodeType::Internal { keys, .. } => keys.len(),
            NodeType::Leaf { entries, .. } => entries.len(),
        }
    }

    /// 检查节点是否已满（键数 >= 2*min_order）
    pub fn is_full(&self, min_order: usize) -> bool {
        self.key_count() >= 2 * min_order
    }

    /// 检查节点是否过少（键数 < min_order，根节点除外）
    pub fn is_underflow(&self, min_order: usize) -> bool {
        self.key_count() < min_order
    }

    /// 获取内部节点的键
    pub fn get_internal_keys(&self) -> Option<&Vec<K>> {
        match &self.node_type {
            NodeType::Internal { keys, .. } => Some(keys),
            _ => None,
        }
    }

    /// 获取内部节点的子节点
    pub fn get_internal_children(&self) -> Option<&Vec<NodePtr<K, V>>> {
        match &self.node_type {
            NodeType::Internal { children, .. } => Some(children),
            _ => None,
        }
    }

    /// 获取叶子节点的条目
    pub fn get_leaf_entries(&self) -> Option<&Vec<(K, V)>> {
        match &self.node_type {
            NodeType::Leaf { entries, .. } => Some(entries),
            _ => None,
        }
    }

    /// 获取叶子节点的下一个节点指针
    pub fn get_leaf_next(&self) -> Option<NodePtr<K, V>> {
        match &self.node_type {
            NodeType::Leaf { next, .. } => next.clone(),
            _ => None,
        }
    }
}

