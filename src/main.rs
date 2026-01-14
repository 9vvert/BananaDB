pub mod config;
pub mod dbms;
pub mod error_type;
pub mod index;
pub mod parser;
pub mod table;

use std::{
    fs,
    io::{self, BufRead, Write},
    path::PathBuf,
};

use clap::Parser;
use dbms::DBMS;
use parser::{
    ast::{Assignment, ColumnDef, ColumnTypeDef, Select, Statement, TableConstraint, Value},
    parse_sql,
};
use table::page::record::ColumnType;

const PAGE_NUM: usize = 1000; // TODO: enlarge the cache buffer size

#[derive(Parser, Debug)]
#[command(name = "BananaDB", about = "Execute SQL scripts for BananaDB.")]
struct Args {
    /// Run initialization before executing any statements
    #[arg(long)]
    init: bool,

    #[arg(short = 'b')]
    b: bool,

    /// Path to the SQL script to execute (e.g. dbs-testcase/in/xxx.sql)
    #[arg(short, long, value_name = "SQL_FILE")]
    script: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut manager = DBMS::<PAGE_NUM>::new();

    let args = Args::parse();

    if args.init {
        // init and simply return
        run_init(&manager);
        return Ok(());
    }

    let mut ctx = ExecutionContext::new(&mut manager);

    if let Some(path) = args.script {
        run_script(&mut ctx, &path)?;
        return Ok(());
    }

    run_repl(&mut ctx)
}

fn run_init(dbms: &DBMS<PAGE_NUM>) {
    let base_path = dbms.base_path.clone();
    let global_path = dbms.global_path.clone();
    fs::remove_dir_all(base_path).unwrap();
    fs::remove_dir_all(global_path).unwrap();
}

fn run_script(
    ctx: &mut ExecutionContext,
    path: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let sql = load_script(path)?;
    let statements = parse_sql(&sql).map_err(|err| {
        eprintln!("Failed to parse SQL from {path:?}: {err}");
        err
    })?;

    for statement in statements {
        if let Err(err) = dispatch_statement(ctx, statement) {
            eprintln!("Error executing statement: {err}");
        }
    }
    Ok(())
}

fn run_repl(ctx: &mut ExecutionContext) -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    for line in stdin.lock().lines() {
        let raw = match line {
            Ok(content) => content,
            Err(err) => {
                eprintln!("Failed to read line: {err}");
                break;
            }
        };

        let trimmed = raw.trim();

        if trimmed.eq_ignore_ascii_case("exit") {
            break;
        }
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.eq_ignore_ascii_case("exit") {
            break;
        }

        // pasre the sql command
        let statements = match parse_sql(trimmed) {
            Ok(stmts) => stmts,
            Err(err) => {
                eprintln!("Failed to parse SQL \"{trimmed}\": {err}");
                // Still print terminator so the checker can proceed.
                writeln!(stdout, "@{raw}")?;
                stdout.flush()?;
                continue;
            }
        };

        for stmt in statements {
            if let Err(err) = dispatch_statement(ctx, stmt) {
                eprintln!("Error executing statement \"{trimmed}\": {err}");
            }
        }

        writeln!(stdout, "@{raw}")?;
        stdout.flush()?;
    }
    Ok(())
}

fn load_script(path: &PathBuf) -> Result<String, std::io::Error> {
    let content = fs::read_to_string(path)?;
    let filtered = content
        .lines()
        .filter(|line| !line.trim_start().starts_with('@'))
        .collect::<Vec<_>>()
        .join("\n");
    Ok(filtered)
}

struct ExecutionContext<'a> {
    dbms: &'a mut DBMS<PAGE_NUM>,
}

impl<'a> ExecutionContext<'a> {
    fn new(dbms: &'a mut DBMS<PAGE_NUM>) -> Self {
        ExecutionContext { dbms }
    }

    fn create_database(&mut self, name: String) -> Result<(), String> {
        // println!("");
        self.dbms.add_database(&name);
        Ok(())
    }

    fn drop_database(&mut self, name: String) -> Result<(), String> {
        // println!("");
        self.dbms.del_database(&name);
        Ok(())
    }

    fn show_databases(&mut self) -> Result<(), String> {
        self.dbms.show_database();
        Ok(())
    }

    fn use_database(&mut self, name: String) -> Result<(), String> {
        self.dbms.load_database(&name);
        Ok(())
    }

    fn show_tables(&mut self) -> Result<(), String> {
        self.dbms.show_table();
        Ok(())
    }

    fn show_indexes(&mut self) -> Result<(), String> {
        // TODO:
        println!("TODO: show indexes");
        Ok(())
    }

    fn create_table(
        &mut self,
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
    ) -> Result<(), String> {
        let (col_name, col_type, col_not_null, col_default): (
            Vec<String>,
            Vec<ColumnType>,
            Vec<bool>,
            Vec<Option<Value>>,
        ) = columns.into_iter().fold(
            (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
            |(mut col_name, mut col_type, mut col_not_null, mut col_default),
             ColumnDef {
                 name: x_name,
                 data_type: x_data_type,
                 not_null: x_not_null,
                 default: x_default,
             }| {
                match x_data_type {
                    ColumnTypeDef::Int => col_type.push(ColumnType::INT),
                    ColumnTypeDef::Float => col_type.push(ColumnType::FLOAT),
                    ColumnTypeDef::Varchar(x) => col_type.push(ColumnType::CHAR(x)),
                }
                col_name.push(x_name);
                col_not_null.push(x_not_null);
                col_default.push(x_default);
                (col_name, col_type, col_not_null, col_default)
            },
        );
        // TODO: Add constraint

        self.dbms
            .create_table_with_constraint(
                &name,
                col_type.iter().collect(),
                col_name.iter().map(|x| x.as_str()).collect(),
                col_not_null,
                col_default,
                constraints,
            )
            .unwrap();
        Ok(())
    }

    fn drop_table(&mut self, name: String) -> Result<(), String> {
        self.dbms.delete_table(&name).unwrap();
        Ok(())
    }

    fn describe_table(&mut self, name: String) -> Result<(), String> {
        self.dbms.desc_table(&name);
        Ok(())
    }

    fn load_data(&mut self, path: String, table: String, delimiter: String) -> Result<(), String> {
        // TODO:
        println!("TODO: load data from {path} into {table} using delimiter '{delimiter}'");
        Ok(())
    }

    fn insert(&mut self, table: String, rows: Vec<Vec<Value>>) -> Result<(), String> {
        let rows_len = rows.len();

        let col_types = match self.dbms.metadata_map.get(&table) {
            // NOTE: 这里需要clone，如果使用引用/移动，会导致后面将无法对dbms进行mut ref
            Some(t) => t.const_info.column_type.clone(),
            None => {
                println!("table {} doesn't exist!", table);
                return Err("Table doesn't exist".to_string());
            }
        };

        for r in rows {
            // insert row by row
            if r.len() != col_types.len() {
                println!("Column len mismatch!");
            }
            let mut insert_data: Vec<u8> = Vec::new();

            for i in 0..col_types.len() {
                let (curr_val, curr_type): (&Value, ColumnType) = (&r[i], col_types[i]);
                insert_data.append(&mut curr_val.to_bytes(curr_type));
            }
            self.dbms.insert_item(&table, insert_data).unwrap();
        }
        println!("rows");
        println!("{}", rows_len);
        Ok(())
    }

    fn delete(
        // TODO:
        &mut self,
        table: String,
        selection: Option<parser::ast::Expr>,
    ) -> Result<(), String> {
        println!("TODO: delete from {table} where {selection:?}");
        Ok(())
    }

    fn update(
        &mut self,
        table: String,
        assignments: Vec<Assignment>,
        selection: Option<parser::ast::Expr>,
    ) -> Result<(), String> {
        // TODO:
        println!("TODO: update {table} set {assignments:?} where {selection:?}");
        Ok(())
    }

    fn select(&mut self, query: Select) -> Result<(), String> {
        // TODO:
        println!("{}", query.projections);
        println!("TODO: select with query {query:?}");
        Ok(())
    }

    fn alter_add_index(
        &mut self,
        table: String,
        name: Option<String>,
        columns: Vec<String>,
    ) -> Result<(), String> {
        // TODO:
        println!("TODO: alter table {table} add index {name:?} on {columns:?}");
        Ok(())
    }

    fn alter_drop_index(&mut self, table: String, name: String) -> Result<(), String> {
        // TODO:
        println!("TODO: alter table {table} drop index {name}");
        Ok(())
    }

    fn alter_drop_primary_key(
        &mut self,
        table: String,
        name: Option<String>,
    ) -> Result<(), String> {
        // TODO:
        println!("TODO: alter table {table} drop primary key {name:?}");
        Ok(())
    }

    fn alter_drop_foreign_key(&mut self, table: String, name: String) -> Result<(), String> {
        // TODO:
        println!("TODO: alter table {table} drop foreign key {name}");
        Ok(())
    }

    fn alter_add_primary_key(
        &mut self,
        table: String,
        constraint: TableConstraint,
    ) -> Result<(), String> {
        // TODO:
        println!("TODO: alter table {table} add primary key {constraint:?}");
        Ok(())
    }

    fn alter_add_foreign_key(
        &mut self,
        table: String,
        constraint: TableConstraint,
    ) -> Result<(), String> {
        // TODO:
        println!("TODO: alter table {table} add foreign key {constraint:?}");
        Ok(())
    }

    fn alter_add_unique(
        &mut self,
        table: String,
        constraint: TableConstraint,
    ) -> Result<(), String> {
        // TODO:
        println!("TODO: alter table {table} add unique constraint {constraint:?}");
        Ok(())
    }
}

// NOTE: dispatch according to the parse result
fn dispatch_statement(ctx: &mut ExecutionContext, statement: Statement) -> Result<(), String> {
    match statement {
        Statement::CreateDatabase { name } => ctx.create_database(name),
        Statement::DropDatabase { name } => ctx.drop_database(name),
        Statement::ShowDatabases => ctx.show_databases(),
        Statement::UseDatabase { name } => ctx.use_database(name),
        Statement::ShowTables => ctx.show_tables(),
        Statement::ShowIndexes => ctx.show_indexes(),
        Statement::CreateTable {
            name,
            columns,
            constraints,
        } => ctx.create_table(name, columns, constraints),
        Statement::DropTable { name } => ctx.drop_table(name),
        Statement::DescribeTable { name } => ctx.describe_table(name),
        Statement::LoadData {
            path,
            table,
            delimiter,
        } => ctx.load_data(path, table, delimiter),
        Statement::Insert { table, rows } => ctx.insert(table, rows),
        Statement::Delete { table, selection } => ctx.delete(table, selection),
        Statement::Update {
            table,
            assignments,
            selection,
        } => ctx.update(table, assignments, selection),
        Statement::Select(select) => ctx.select(select),
        Statement::AlterAddIndex {
            table,
            name,
            columns,
        } => ctx.alter_add_index(table, name, columns),
        Statement::AlterDropIndex { table, name } => ctx.alter_drop_index(table, name),
        Statement::AlterDropPrimaryKey { table, name } => ctx.alter_drop_primary_key(table, name),
        Statement::AlterDropForeignKey { table, name } => ctx.alter_drop_foreign_key(table, name),
        Statement::AlterAddPrimaryKey { table, constraint } => {
            ctx.alter_add_primary_key(table, constraint)
        }
        Statement::AlterAddForeignKey { table, constraint } => {
            ctx.alter_add_foreign_key(table, constraint)
        }
        Statement::AlterAddUnique { table, constraint } => ctx.alter_add_unique(table, constraint),
        Statement::Null => Ok(()),
    }
}
