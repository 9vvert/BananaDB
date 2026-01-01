use BananaDB::{
    dbms::DBMS,
    table::page::record::{ColumnType, ColumnValue, RecordId},
};

#[test]
fn test_item_val() {
    let mut manager = DBMS::<4>::new();
    assert!(
        manager
            .create_table(
                "abc",
                vec![&ColumnType::INT, &ColumnType::INT, &ColumnType::CHAR(10)],
                vec!["age", "money", "name"],
            )
            .is_ok()
    );

    manager.show_table_metadata("abc");
    // insert the first item
    let data: Vec<u8> = vec![
        1, 3, 4, 5, 0, 0, 2, 11, 0x64, 0x65, 0x66, 0x65, 0x66, 0x65, 0x66, 0x65, 0x66, 0x65,
    ];
    manager.insert_item("abc", data).unwrap();

    let result0 = manager.read_item_all("abc", RecordId(0)).unwrap();

    assert_eq!(result0.len(), 3);

    println!("{}, {}, {}", result0[0], result0[1], result0[2]);

    manager
        .write_item_col("abc", RecordId(0), "money", ColumnValue::INT(60))
        .unwrap();
    manager
        .write_item_col(
            "abc",
            RecordId(0),
            "name",
            ColumnValue::CAHR("deadbeef".to_string()),
        )
        .unwrap();
    manager
        .write_item_col("abc", RecordId(0), "money", ColumnValue::INT(30))
        .unwrap();
    manager
        .write_item_col("abc", RecordId(0), "age", ColumnValue::INT(18))
        .unwrap();

    let age = manager.read_item_col("abc", RecordId(0), "age").unwrap();
    let money = manager.read_item_col("abc", RecordId(0), "money").unwrap();
    let name = manager.read_item_col("abc", RecordId(0), "name").unwrap();

    assert!(manager.read_item_col("abc", RecordId(0), "no_col").is_err());

    assert_eq!(age, ColumnValue::INT(18));
    assert_eq!(money, ColumnValue::INT(30));
    assert_eq!(name, ColumnValue::CAHR("deadbeef".to_string()));
}
