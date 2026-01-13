use BananaDB::parser::{ast::Statement, parse_sql};

#[test]
fn parse_create_table() {
    let sql = "CREATE TABLE users (id INT, name VARCHAR(20), PRIMARY KEY(id));";
    let stmts = parse_sql(sql).expect("should parse create table");
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateTable {
            name,
            columns,
            constraints,
        } => {
            assert_eq!(name, "users");
            assert_eq!(columns.len(), 2);
            assert_eq!(constraints.len(), 1);
        }
        other => panic!("unexpected statement: {:?}", other),
    }
}

#[test]
fn parse_select_with_where() {
    let sql = "SELECT * FROM users WHERE age >= 18 AND name LIKE 'A%';";
    let stmts = parse_sql(sql).expect("should parse select");
    assert_eq!(stmts.len(), 1);
    assert!(matches!(&stmts[0], Statement::Select(_)));
    println!("{}", &stmts[0])
}

#[test]
fn parse_insert_values() {
    let sql = "INSERT INTO users VALUES (1, 'alice'), (2, 'bob');";
    let stmts = parse_sql(sql).expect("should parse insert");
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::Insert { table, rows } => {
            assert_eq!(table, "users");
            assert_eq!(rows.len(), 2);
        }
        other => panic!("unexpected statement: {:?}", other),
    }
}
