use BananaDB::FileSystem;

#[test]
fn test_create_table() {
    let mut fm = FileSystem::FileManager::new();
    let result1 = fm.open_table("abc");
    assert!(result1.is_err());

    let result2 = fm.new_table("abc");
    assert!(result2.is_ok());

    let result3 = fm.open_table("abc");
    assert!(result3.is_ok());

    let result4 = fm.new_table("abc");
    assert!(result4.is_err());
}
