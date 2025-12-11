use b_plus_tree::BPlusTree;

fn main() {
    // 创建 B+ 树
    let mut tree = BPlusTree::new_default();

    // 插入一些数据
    println!("插入数据...");
    for i in 0..20 {
        tree.insert(i, format!("value_{}", i)).unwrap();
    }

    // 查找数据
    println!("\n查找数据:");
    for i in 0..5 {
        if let Some(value) = tree.get(&i) {
            println!("  key: {}, value: {}", i, value);
        }
    }

    // 范围查询
    println!("\n范围查询 [5, 10):");
    let results = tree.range_query(&5, &10);
    for (key, value) in results {
        println!("  key: {}, value: {}", key, value);
    }

    // 迭代器遍历
    println!("\n前 10 个键值对:");
    for (i, (key, value)) in tree.iter().take(10).enumerate() {
        println!("  [{}] key: {}, value: {}", i, key, value);
    }

    // 删除数据
    println!("\n删除键 5...");
    tree.delete(&5).unwrap();
    assert_eq!(tree.get(&5), None);
    println!("  键 5 已删除");

    println!("\nB+ 树操作完成！");
}

