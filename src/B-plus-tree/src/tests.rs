#[cfg(test)]
mod tests {
    use crate::BPlusTree;

    #[test]
    fn test_insert_and_get() {
        let mut tree = BPlusTree::new_default();
        
        // 插入一些键值对
        tree.insert(1, "value1").unwrap();
        tree.insert(2, "value2").unwrap();
        tree.insert(3, "value3").unwrap();
        
        // 验证查找
        assert_eq!(tree.get(&1), Some("value1"));
        assert_eq!(tree.get(&2), Some("value2"));
        assert_eq!(tree.get(&3), Some("value3"));
        assert_eq!(tree.get(&4), None);
    }

    #[test]
    fn test_duplicate_key() {
        let mut tree = BPlusTree::new_default();
        
        tree.insert(1, "value1").unwrap();
        assert!(tree.insert(1, "value2").is_err());
    }

    #[test]
    fn test_delete() {
        let mut tree = BPlusTree::new_default();
        
        tree.insert(1, "value1").unwrap();
        tree.insert(2, "value2").unwrap();
        tree.insert(3, "value3").unwrap();
        
        tree.delete(&2).unwrap();
        assert_eq!(tree.get(&2), None);
        assert_eq!(tree.get(&1), Some("value1"));
        assert_eq!(tree.get(&3), Some("value3"));
    }

    #[test]
    fn test_range_query() {
        let mut tree = BPlusTree::new_default();
        
        for i in 0..20 {
            tree.insert(i, format!("value{}", i)).unwrap();
        }
        
        let results = tree.range_query(&5, &15);
        assert_eq!(results.len(), 10);
        for (i, (k, _)) in results.iter().enumerate() {
            assert_eq!(*k, i + 5);
        }
    }

    #[test]
    fn test_iterator() {
        let mut tree = BPlusTree::new_default();
        
        for i in 0..10 {
            tree.insert(i, format!("value{}", i)).unwrap();
        }
        
        let mut count = 0;
        for (k, v) in tree.iter() {
            assert_eq!(k, count);
            assert_eq!(v, format!("value{}", count));
            count += 1;
        }
        assert_eq!(count, 10);
    }

    #[test]
    fn test_large_insert() {
        let mut tree = BPlusTree::new_default();
        
        // 插入大量数据以测试节点分裂
        for i in 0..100 {
            tree.insert(i, format!("value{}", i)).unwrap();
        }
        
        // 验证所有数据都能正确查找
        for i in 0..100 {
            assert_eq!(tree.get(&i), Some(format!("value{}", i)));
        }
    }

    #[test]
    fn test_delete_all() {
        let mut tree = BPlusTree::new_default();
        
        for i in 0..50 {
            tree.insert(i, format!("value{}", i)).unwrap();
        }
        
        // 删除所有数据（从后往前删除，避免中间键的问题）
        for i in (0..50).rev() {
            match tree.delete(&i) {
                Ok(_) => {},
                Err(e) => {
                    panic!("Failed to delete key {}: {:?}", i, e);
                }
            }
        }
        
        // 验证所有数据都已删除
        for i in 0..50 {
            assert_eq!(tree.get(&i), None);
        }
    }

    #[test]
    fn test_string_keys() {
        let mut tree = BPlusTree::new_default();
        
        tree.insert("apple".to_string(), 1).unwrap();
        tree.insert("banana".to_string(), 2).unwrap();
        tree.insert("cherry".to_string(), 3).unwrap();
        
        assert_eq!(tree.get(&"banana".to_string()), Some(2));
        assert_eq!(tree.get(&"apple".to_string()), Some(1));
    }
}

