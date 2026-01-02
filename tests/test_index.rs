use BananaDB::{
    dbms::{FilterOp, DBMS},
    table::page::record::{ColumnType, ColumnValue, RecordId},
};

fn pack_row(a: i32, b: i32) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&a.to_le_bytes());
    v.extend_from_slice(&b.to_le_bytes());
    v
}

#[test]
fn test_index_build_and_query() {
    let mut manager = DBMS::<4>::new();
    // clean if exists
    let _ = manager.delete_table("idx_people");

    manager
        .create_table(
            "idx_people",
            vec![&ColumnType::INT, &ColumnType::INT],
            vec!["age", "score"],
        )
        .unwrap();

    let rids = vec![
        manager.insert_item("idx_people", pack_row(18, 90)).unwrap(),
        manager.insert_item("idx_people", pack_row(25, 80)).unwrap(),
        manager.insert_item("idx_people", pack_row(32, 88)).unwrap(),
        manager.insert_item("idx_people", pack_row(40, 75)).unwrap(),
    ];

    // build index after data inserted
    manager.create_index("idx_people", 0).unwrap();

    let greater_30 = manager
        .filter_rids("idx_people", "age", FilterOp::Gt, ColumnValue::INT(30))
        .unwrap();
    assert_eq!(greater_30.len(), 2);
    assert!(greater_30.contains(&rids[2]) && greater_30.contains(&rids[3]));

    let eq_25 = manager
        .filter_rids("idx_people", "age", FilterOp::Eq, ColumnValue::INT(25))
        .unwrap();
    assert_eq!(eq_25, vec![rids[1]]);

    // delete one row and ensure index updated
    manager.delete_item("idx_people", rids[2]).unwrap();
    let greater_30_after = manager
        .filter_rids("idx_people", "age", FilterOp::Gt, ColumnValue::INT(30))
        .unwrap();
    assert_eq!(greater_30_after, vec![rids[3]]);

    // update value through indexed column
    manager
        .write_item_col(
            "idx_people",
            rids[0],
            "age",
            ColumnValue::INT(50),
        )
        .unwrap();
    let eq_50 = manager
        .filter_rids("idx_people", "age", FilterOp::Eq, ColumnValue::INT(50))
        .unwrap();
    assert_eq!(eq_50, vec![rids[0]]);

    // select rows using index filter
    let rows = manager
        .select_all_where("idx_people", "age", FilterOp::Ge, ColumnValue::INT(25))
        .unwrap();
    assert_eq!(rows.len(), 3);
    let mut ages: Vec<i32> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            ColumnValue::INT(x) => Some(*x),
            _ => None,
        })
        .collect();
    ages.sort();
    assert_eq!(ages, vec![25, 40, 50]);
}
