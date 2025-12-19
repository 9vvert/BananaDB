use BananaDB::dbms::{DBMS, resource::PageType};

#[test]
fn test_dbms() {
    let mut manager = DBMS::<4, 4096>::new();

    manager.db_io.create_file("abc", &PageType::TABLE);

    let page0 = manager.db_io.get_page("abc", 0, &PageType::TABLE);
    for i in 0..page0.data.len() {
        page0.data[i] = (i % 256) as u8;
    }
    page0.set_dirty();

    let page1 = manager.db_io.get_page("abc", 1, &PageType::TABLE);
    for i in 0..page1.data.len() {
        page1.data[1] = (i % 256) as u8;
    }
    page1.set_dirty();

    let page2 = manager.db_io.get_page("abc", 2, &PageType::TABLE);
    let page3 = manager.db_io.get_page("abc", 3, &PageType::TABLE);
    let page4 = manager.db_io.get_page("abc", 4, &PageType::TABLE);
}
