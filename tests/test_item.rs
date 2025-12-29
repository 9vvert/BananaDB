use BananaDB::{
    dbms::DBMS,
    table::page::record::{ColumnType, RecordId},
};

#[test]
fn test_item() {
    let mut manager = DBMS::<4, 4096>::new();
    assert!(
        manager
            .create_table(
                "abc",
                vec![&ColumnType::INT, &ColumnType::INT, &ColumnType::CHAR(3)],
                vec!["age", "money", "name"],
            )
            .is_ok()
    );

    manager.show_table_metadata("abc");
    // insert the first item
    let data: Vec<u8> = vec![1, 3, 4, 5, 0, 0, 2, 11, 0x64, 0x65, 0x66];
    manager.insert_item("abc", data).unwrap();

    manager.show_table_metadata("abc");
    manager.show_table_page("abc", 0);
    // insert the first item
    let data: Vec<u8> = vec![1, 3, 4, 5, 0, 0, 2, 11, 0x64, 0x65, 0x66];
    manager.insert_item("abc", data).unwrap();

    let data2: Vec<u8> = vec![4, 3, 4, 5, 0, 0, 2, 11, 0x0, 0x0, 0x0];
    manager.insert_item("abc", data2).unwrap();

    let data3: Vec<u8> = vec![7, 3, 4, 5, 0, 0, 2, 11, 0x0, 0x0, 0x0];
    manager.insert_item("abc", data3).unwrap();

    manager.show_next_free_slot("abc");
    manager.show_table_page("abc", 0);

    // delte an item
    manager.delete_item("abc", RecordId(1)).unwrap();
    manager.show_next_free_slot("abc");
    manager.show_table_page("abc", 0);

    // insert
    let data4: Vec<u8> = vec![5, 3, 4, 5, 0, 0, 2, 11, 0x0, 0x0, 0x0];
    manager.insert_item("abc", data4).unwrap();
    manager.show_next_free_slot("abc");
    manager.show_table_page("abc", 0);
}
