use BananaDB::{dbms::DBMS, item::record::ColumnType};

#[test]
fn test_newfs() {
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

    // same name
    assert!(
        manager
            .create_table(
                "abc",
                vec![&ColumnType::INT, &ColumnType::INT, &ColumnType::CHAR(3)],
                vec!["age", "money", "name"],
            )
            .is_err()
    );

    // table doesn't exist
    assert!(manager.create_index("def", 2).is_err());
    // exceed column bound
    assert!(manager.create_index("abc", 3).is_err());

    // a second table
    assert!(
        manager
            .create_table(
                "def",
                vec![&ColumnType::CHAR(8), &ColumnType::INT, &ColumnType::CHAR(3)],
                vec!["id", "apple", "banana"],
            )
            .is_ok()
    );
    // create valid index
    // match manager.create_index("def", 2) {
    //     Ok(_) => {}
    //     Err(e) => println!("{}", e),
    // }
    // manager.create_index("def", 2).unwrap();

    assert!(manager.create_index("def", 2).is_ok());
    assert!(manager.create_index("def", 0).is_ok());
}
