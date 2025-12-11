mod node;
mod tree;

#[cfg(test)]
mod tests;

pub use tree::BPlusTree;
pub use tree::BPlusTreeError;

/// B+ 树配置参数
#[derive(Debug, Clone)]
pub struct BPlusTreeConfig {
    /// 节点的最小阶数（每个节点至少包含 min_order 个键，最多包含 2*min_order 个键）
    pub min_order: usize,
}

impl Default for BPlusTreeConfig {
    fn default() -> Self {
        Self { min_order: 3 }
    }
}

