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
    ast::{
        Assignment, BinaryOp, ColumnDef, ColumnRef, ColumnTypeDef, Expr, Select, SelectItem,
        Statement, TableConstraint, Value,
    },
    parse_sql,
};
use table::{
    TableMetaData,
    page::record::{ColumnType, ColumnValue, RecordId},
};

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

        let metadata = match self.dbms.metadata_map.get(&table) {
            // NOTE: 这里需要clone，如果使用引用/移动，会导致后面将无法对dbms进行mut ref
            Some(t) => t.clone(),
            None => {
                println!("table {} doesn't exist!", table);
                return Err("Table doesn't exist".to_string());
            }
        };

        let col_types = metadata.const_info.column_type.clone();
        let pk_indices = self.primary_key_indices(&metadata)?;

        // convert all rows and validate length
        let mut converted_rows: Vec<Vec<ColumnValue>> = Vec::new();
        for r in &rows {
            if r.len() != col_types.len() {
                println!("Column len mismatch!");
                return Err("Column len mismatch".to_string());
            }
            let mut converted = Vec::with_capacity(col_types.len());
            for (i, val) in r.iter().enumerate() {
                converted.push(self.convert_value_for_column(val, col_types[i])?);
            }
            converted_rows.push(converted);
        }

        if !pk_indices.is_empty() {
            let existing_rows = self.dbms.scan_table_all(&table)?;
            let mut existing_keys: Vec<Vec<ColumnValue>> = existing_rows
                .iter()
                .map(|row| pk_indices.iter().map(|&idx| row[idx].clone()).collect())
                .collect();
            let mut new_keys: Vec<Vec<ColumnValue>> = Vec::new();

            for r in &converted_rows {
                let key: Vec<ColumnValue> =
                    pk_indices.iter().map(|&idx| r[idx].clone()).collect();
                if existing_keys.iter().any(|k| *k == key) || new_keys.iter().any(|k| *k == key) {
                    println!("!ERROR");
                    println!("duplicate");
                    return Ok(());
                }
                new_keys.push(key);
            }
            existing_keys.append(&mut new_keys);
        }

        // foreign key check
        for converted in &converted_rows {
            if !self.check_foreign_constraints(&table, &metadata, converted)? {
                println!("!ERROR");
                println!("foreign");
                return Ok(());
            }
        }

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
        &mut self,
        table: String,
        selection: Option<parser::ast::Expr>,
    ) -> Result<(), String> {
        let metadata = self.get_metadata(&table)?;
        let rows = self.dbms.scan_table_rows(&table)?;
        let mut delete_rids = Vec::new();

        for (rid, row) in rows {
            if selection.as_ref().map_or(
                Ok(true),
                |expr| {
                    self.eval_predicate_multi(
                        expr,
                        &vec![table.clone()],
                        &vec![metadata.clone()],
                        &vec![&row],
                    )
                },
            )? {
                // prevent deleting referenced parent rows
                if self.has_referencing_children(&table, &metadata, &row)? {
                    println!("!ERROR");
                    println!("foreign");
                    return Ok(());
                }
                delete_rids.push(rid);
            }
        }

        for rid in delete_rids.iter() {
            self.dbms.delete_item(&table, *rid)?;
        }

        println!("rows");
        println!("{}", delete_rids.len());
        Ok(())
    }

    fn update(
        &mut self,
        table: String,
        assignments: Vec<Assignment>,
        selection: Option<parser::ast::Expr>,
    ) -> Result<(), String> {
        let metadata = self.get_metadata(&table)?;
        let rows = self.dbms.scan_table_rows(&table)?;
        let mut affected = 0;
        let pk_indices = self.primary_key_indices(&metadata)?;
        let pk_names: Vec<String> = pk_indices
            .iter()
            .map(|&idx| metadata.const_info.column_name[idx].clone())
            .collect();

        // Two-phase: determine affected rows and validate constraints before mutating.
        let mut targets: Vec<(RecordId, Vec<ColumnValue>, Vec<ColumnValue>)> = Vec::new();
        for (rid, row) in rows {
            if !selection.as_ref().map_or(
                Ok(true),
                |expr| {
                    self.eval_predicate_multi(
                        expr,
                        &vec![table.clone()],
                        &vec![metadata.clone()],
                        &vec![&row],
                    )
                },
            )? {
                continue;
            }

            let mut new_row = row.clone();
            for Assignment { column, value } in &assignments {
                let idx = metadata
                    .const_info
                    .column_name
                    .iter()
                    .position(|c| c == column)
                    .ok_or_else(|| "Invalid Colume name".to_string())?;
                let col_type = metadata.const_info.column_type[idx];
                let col_val = self.convert_value_for_column(value, col_type)?;
                new_row[idx] = col_val;
            }
            targets.push((rid, row, new_row));
        }

        // If updating PK columns, ensure no referencing children.
        if !pk_names.is_empty()
            && assignments
                .iter()
                .any(|a| pk_names.iter().any(|n| n == &a.column))
        {
            for (_rid, old_row, _new_row) in &targets {
                if self.has_referencing_children(&table, &metadata, old_row)? {
                    println!("!ERROR");
                    println!("foreign");
                    return Ok(());
                }
            }
        }

        // Check foreign keys for updated rows.
        for (_rid, _old_row, new_row) in &targets {
            if !self.check_foreign_constraints(&table, &metadata, new_row)? {
                println!("!ERROR");
                println!("foreign");
                return Ok(());
            }
        }

        // Apply updates
        for (rid, _old_row, new_row) in targets {
            for (idx, val) in new_row.iter().enumerate() {
                let col_name: &String = &metadata.const_info.column_name[idx];
                self.dbms
                    .write_item_col(&table, rid, col_name.as_str(), val.clone())?;
            }
            affected += 1;
        }

        println!("rows");
        println!("{affected}");
        Ok(())
    }

    fn select(&mut self, query: Select) -> Result<(), String> {
        if query.from.is_empty() {
            return Err("No table specified".to_string());
        }

        let table_names = query.from.clone();
        let mut metas = Vec::new();
        for t in &table_names {
            metas.push(self.get_metadata(t)?);
        }

        // gather rows for each table
        let mut all_rows: Vec<Vec<Vec<ColumnValue>>> = Vec::new();
        for t in &table_names {
            all_rows.push(self.dbms.scan_table_all(t)?);
        }

        // only support up to two tables for now
        if all_rows.len() > 2 {
            return Err("Only up to two tables select is supported".to_string());
        }

        let projections = self.resolve_projection_multi(&query.projections, &table_names, &metas)?;

        let header = projections
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>()
            .join(",");
        println!("{header}");

        // produce row combinations
        let emit_row = |row_refs: Vec<&Vec<ColumnValue>>| -> Result<(), String> {
            if let Some(ref expr) = query.r#where {
                if !self.eval_predicate_multi(expr, &table_names, &metas, &row_refs)? {
                    return Ok(());
                }
            }

            let out = projections
                .iter()
                .map(|(_, (t_idx, c_idx))| Self::format_value(&row_refs[*t_idx][*c_idx]))
                .collect::<Vec<_>>()
                .join(",");
            println!("{out}");
            Ok(())
        };

        if all_rows.len() == 1 {
            for row in &all_rows[0] {
                emit_row(vec![row])?;
            }
        } else {
            for row_a in &all_rows[0] {
                for row_b in &all_rows[1] {
                    emit_row(vec![row_a, row_b])?;
                }
            }
        }

        Ok(())
    }

    fn get_metadata(&self, table: &str) -> Result<TableMetaData, String> {
        self.dbms
            .metadata_map
            .get(table)
            .cloned()
            .ok_or_else(|| "Table doesn't exist".to_string())
    }

    fn column_index_multi(
        &self,
        col_ref: &ColumnRef,
        table_names: &[String],
        metas: &[TableMetaData],
    ) -> Result<(usize, usize), String> {
        let target_table = match &col_ref.table {
            Some(t) => t.clone(),
            None => {
                if table_names.len() == 1 {
                    table_names[0].clone()
                } else {
                    return Err("Ambiguous column reference".to_string());
                }
            }
        };
        let t_idx = table_names
            .iter()
            .position(|t| t == &target_table)
            .ok_or_else(|| "Invalid table reference".to_string())?;
        let c_idx = metas[t_idx]
            .const_info
            .column_name
            .iter()
            .position(|name| name == &col_ref.column)
            .ok_or_else(|| "Invalid Colume name".to_string())?;
        Ok((t_idx, c_idx))
    }

    fn value_from_expr_multi(
        &self,
        expr: &Expr,
        table_names: &[String],
        metas: &[TableMetaData],
        rows: &[&Vec<ColumnValue>],
    ) -> Result<ColumnValue, String> {
        match expr {
            Expr::Value(Value::Int(i)) => Ok(ColumnValue::INT(*i as i32)),
            Expr::Value(Value::Float(f)) => Ok(ColumnValue::FLOAT(*f)),
            Expr::Value(Value::String(s)) => Ok(ColumnValue::CAHR(s.clone())),
            Expr::Column(col_ref) => {
                let (t_idx, c_idx) = self.column_index_multi(col_ref, table_names, metas)?;
                Ok(rows[t_idx][c_idx].clone())
            }
            _ => Err("Unsupported expression in where clause".to_string()),
        }
    }

    fn compare_values(
        &self,
        op: BinaryOp,
        lhs: &ColumnValue,
        rhs: &ColumnValue,
    ) -> Result<bool, String> {
        let ord = match (lhs, rhs) {
            (ColumnValue::INT(a), ColumnValue::INT(b)) => a.cmp(b),
            (ColumnValue::CAHR(a), ColumnValue::CAHR(b)) => a.cmp(b),
            (ColumnValue::FLOAT(a), ColumnValue::FLOAT(b)) => a
                .partial_cmp(b)
                .ok_or_else(|| "Invalid float compare".to_string())?,
            _ => return Err("Column type mismatch".to_string()),
        };
        let res = match op {
            BinaryOp::Eq => ord == std::cmp::Ordering::Equal,
            BinaryOp::Ne => ord != std::cmp::Ordering::Equal,
            BinaryOp::Lt => ord == std::cmp::Ordering::Less,
            BinaryOp::Le => ord != std::cmp::Ordering::Greater,
            BinaryOp::Gt => ord == std::cmp::Ordering::Greater,
            BinaryOp::Ge => ord != std::cmp::Ordering::Less,
            BinaryOp::And => return Err("Invalid logical op for compare".to_string()),
        };
        Ok(res)
    }

    fn eval_predicate_multi(
        &self,
        expr: &Expr,
        table_names: &[String],
        metas: &[TableMetaData],
        rows: &[&Vec<ColumnValue>],
    ) -> Result<bool, String> {
        match expr {
            Expr::Binary { left, op, right } => {
                if *op == BinaryOp::And {
                    Ok(self.eval_predicate_multi(left, table_names, metas, rows)?
                        && self.eval_predicate_multi(right, table_names, metas, rows)?)
                } else {
                    let lhs =
                        self.value_from_expr_multi(left, table_names, metas, rows)?;
                    let rhs =
                        self.value_from_expr_multi(right, table_names, metas, rows)?;
                    self.compare_values(*op, &lhs, &rhs)
                }
            }
            _ => Err("Unsupported where clause".to_string()),
        }
    }

    fn resolve_projection_multi(
        &self,
        items: &[SelectItem],
        table_names: &[String],
        metas: &[TableMetaData],
    ) -> Result<Vec<(String, (usize, usize))>, String> {
        if items
            .iter()
            .any(|item| matches!(item, SelectItem::Wildcard))
        {
            let mut cols = Vec::new();
            for (t_idx, meta) in metas.iter().enumerate() {
                for (c_idx, name) in meta.const_info.column_name.iter().enumerate() {
                    cols.push((name.clone(), (t_idx, c_idx)));
                }
            }
            return Ok(cols);
        }

        let mut cols = Vec::new();
        for item in items {
            match item {
                SelectItem::Column(col_ref) => {
                    let (t_idx, c_idx) =
                        self.column_index_multi(col_ref, table_names, metas)?;
                    cols.push((col_ref.column.clone(), (t_idx, c_idx)));
                }
                _ => return Err("Unsupported select item".to_string()),
            }
        }
        Ok(cols)
    }

    fn primary_key_indices(&self, metadata: &TableMetaData) -> Result<Vec<usize>, String> {
        for constraint in &metadata.const_info.column_constraint {
            if let TableConstraint::PrimaryKey { columns, .. } = constraint {
                let mut idxs = Vec::new();
                for col in columns {
                    let idx = self.find_col_index(metadata, col)?;
                    idxs.push(idx);
                }
                return Ok(idxs);
            }
        }
        Ok(Vec::new())
    }

    fn find_col_index(&self, metadata: &TableMetaData, name: &str) -> Result<usize, String> {
        metadata
            .const_info
            .column_name
            .iter()
            .position(|c| c == name)
            .ok_or_else(|| "Invalid Colume name".to_string())
    }

    fn convert_value_for_column(
        &self,
        value: &Value,
        col_type: ColumnType,
    ) -> Result<ColumnValue, String> {
        match (value, col_type) {
            (Value::Int(i), ColumnType::INT) => Ok(ColumnValue::INT(*i as i32)),
            (Value::Float(f), ColumnType::FLOAT) => Ok(ColumnValue::FLOAT(*f)),
            (Value::String(s), ColumnType::CHAR(_)) => Ok(ColumnValue::CAHR(s.clone())),
            (Value::Null, ColumnType::INT) => Ok(ColumnValue::INT(0)),
            (Value::Null, ColumnType::FLOAT) => Ok(ColumnValue::FLOAT(0.0)),
            (Value::Null, ColumnType::CHAR(_)) => Ok(ColumnValue::CAHR(String::new())),
            _ => Err("Column type mismatch".to_string()),
        }
    }

    fn format_value(val: &ColumnValue) -> String {
        match val {
            ColumnValue::INT(x) => x.to_string(),
            ColumnValue::CAHR(s) => s.to_string(),
            ColumnValue::FLOAT(f) => format!("{:.2}", f),
        }
    }

    fn check_foreign_constraints(
        &mut self,
        _table: &str,
        metadata: &TableMetaData,
        row: &[ColumnValue],
    ) -> Result<bool, String> {
        for constraint in &metadata.const_info.column_constraint {
            if let TableConstraint::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                ..
            } = constraint
            {
                let ref_meta = self.get_metadata(ref_table)?;
                let mut child_indices = Vec::new();
                let mut parent_indices = Vec::new();
                for (c, rc) in columns.iter().zip(ref_columns.iter()) {
                    child_indices.push(self.find_col_index(metadata, c)?);
                    parent_indices.push(self.find_col_index(&ref_meta, rc)?);
                }

                let ref_rows = self.dbms.scan_table_all(ref_table)?;
                let mut found = false;
                for ref_row in &ref_rows {
                    let mut match_all = true;
                    for (ci, pi) in child_indices.iter().zip(parent_indices.iter()) {
                        if row[*ci] != ref_row[*pi] {
                            match_all = false;
                            break;
                        }
                    }
                    if match_all {
                        found = true;
                        break;
                    }
                }
                if !found {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn has_referencing_children(
        &mut self,
        table: &str,
        metadata: &TableMetaData,
        row: &[ColumnValue],
    ) -> Result<bool, String> {
        let child_entries: Vec<(String, TableMetaData)> = self
            .dbms
            .metadata_map
            .iter()
            .map(|(n, m)| (n.clone(), m.clone()))
            .collect();

        // gather referencing foreign keys
        for (child_name, child_meta) in child_entries {
            for constraint in &child_meta.const_info.column_constraint {
                if let TableConstraint::ForeignKey {
                    columns,
                    ref_table,
                    ref_columns,
                    ..
                } = constraint
                {
                    if ref_table != table {
                        continue;
                    }
                    let mut child_indices = Vec::new();
                    let mut parent_indices = Vec::new();
                    for (c, rc) in columns.iter().zip(ref_columns.iter()) {
                        child_indices.push(self.find_col_index(&child_meta, c)?);
                        parent_indices.push(self.find_col_index(metadata, rc)?);
                    }

                    let child_rows = self.dbms.scan_table_all(&child_name)?;
                    for child_row in &child_rows {
                        let mut match_all = true;
                        for (ci, pi) in child_indices.iter().zip(parent_indices.iter()) {
                            if child_row[*ci] != row[*pi] {
                                match_all = false;
                                break;
                            }
                        }
                        if match_all {
                            return Ok(true);
                        }
                    }
                }
            }
        }
        Ok(false)
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
