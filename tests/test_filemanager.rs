use BananaDB::io_manager::cache_system;
use BananaDB::io_manager::file_system;

#[test]
fn test_create_table() {
    let mut fm = file_system::FileManager::new();
    let result1 = fm.open_table("abc");
    assert!(result1.is_err());

    let result2 = fm.new_table("abc");
    assert!(result2.is_ok());

    let result3 = fm.open_table("abc");
    assert!(result3.is_ok());

    let result4 = fm.new_table("abc");
    assert!(result4.is_err());

    assert!(fm.new_table("def").is_ok());

    let table_id = match fm.open_table("def") {
        Ok(uuid) => uuid,
        Err(_) => {
            assert!(false);
            "".to_string()
        }
    };

    let mut buf: [u8; 4096] = [8; 4096];

    assert!(fm.read_page(&table_id, 8, &mut buf).is_err());
    assert!(fm.write_page(&table_id, 8, &buf).is_err());

    assert!(fm.new_page(&table_id, 4).is_ok());

    assert!(fm.read_page(&table_id, 8, &mut buf).is_err());
    assert!(fm.write_page(&table_id, 8, &buf).is_err());

    assert!(fm.write_page(&table_id, 4, &buf).is_ok());
}
