use BananaDB::io_manager::cache_system;
use BananaDB::io_manager::file_system;

#[test]
fn test_file_manager() {
    let mut f1 = file_system::FileManager::new();
    assert!(f1.new_table("abc").is_ok());
    let mut t1 = f1.open_file("./base/abc");
    assert!(t1.is_ok());
    let mut t2 = f1.open_file("./base/def");
    assert!(t2.is_err());
    assert!(f1.new_table("def").is_ok());
    assert!(f1.new_table("def").is_err());
    let mut t3 = f1.open_file("./base/def");
    assert!(t3.is_ok());

    let mut buffer = [0u8; 4096];

    for i in 0..buffer.len() {
        buffer[i] = (i % 256) as u8;
    }
    let mut t = t1.unwrap();

    let mut p1 = f1.write_page(&mut t, 0, &mut buffer);
    let mut p2 = f1.write_page(&mut t, 2, &mut buffer);
}
