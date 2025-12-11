# B+ 树模块

这是一个使用 Rust 实现的 B+ 树数据结构模块。

## 特性

- ✅ 插入操作（支持节点自动分裂）
- ✅ 查找操作（O(log n) 时间复杂度）
- ✅ 删除操作（支持节点合并和借用）
- ✅ 范围查询（利用叶子节点链表）
- ✅ 迭代器支持（顺序遍历所有键值对）
- ✅ 支持任意可比较的键类型和值类型

## 使用方法

```rust
use b_plus_tree::BPlusTree;

// 创建 B+ 树
let mut tree = BPlusTree::new_default();

// 插入键值对
tree.insert(1, "value1").unwrap();
tree.insert(2, "value2").unwrap();
tree.insert(3, "value3").unwrap();

// 查找
if let Some(value) = tree.get(&2) {
    println!("Found: {}", value);
}

// 范围查询
let results = tree.range_query(&1, &3);
for (key, value) in results {
    println!("{}: {}", key, value);
}

// 迭代器
for (key, value) in tree.iter() {
    println!("{}: {}", key, value);
}

// 删除
tree.delete(&2).unwrap();
```

## 配置

可以通过 `BPlusTreeConfig` 自定义 B+ 树的阶数：

```rust
use b_plus_tree::{BPlusTree, BPlusTreeConfig};

let config = BPlusTreeConfig { min_order: 5 };
let mut tree = BPlusTree::new(config);
```

`min_order` 参数控制每个节点的最小键数。每个节点最多可以包含 `2 * min_order` 个键。

## 运行测试

```bash
cargo test
```

## 数据结构说明

B+ 树是一种自平衡的树数据结构，具有以下特点：

1. **所有数据都在叶子节点**：内部节点只存储键，用于导航
2. **叶子节点形成链表**：便于范围查询和顺序遍历
3. **节点分裂和合并**：保持树的平衡，确保 O(log n) 的查找性能
4. **节点借用**：删除时优先从兄弟节点借用，减少合并操作

## 错误处理

- `BPlusTreeError::KeyNotFound`：尝试删除不存在的键
- `BPlusTreeError::DuplicateKey`：尝试插入已存在的键
- `BPlusTreeError::InvalidOperation`：内部操作错误

