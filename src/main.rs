pub mod config;
pub mod dbms;
pub mod error_type;
pub mod index;
pub mod parser;
pub mod table;

use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{self, BufRead, BufReader, ErrorKind, Write},
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
use table::page::TablePage;
use table::{
    page::record::{ColumnType, ColumnValue, RecordId},
    TableMetaData,
};

const PAGE_NUM: usize = 10000; // TODO: enlarge the cache buffer size

#[derive(Parser, Debug)]
#[command(name = "BananaDB", about = "Execute SQL scripts for BananaDB.")]
struct Args {
    /// Run initialization before executing any statements
    #[arg(long)]
    init: bool,

    #[arg(short = 'b', long = "batch")]
    batch: bool,

    /// Import data from a file into a target table
    #[arg(short = 'f', long = "file", value_name = "PATH", requires = "table")]
    file: Option<PathBuf>,

    /// Target table for data import
    #[arg(short = 't', long = "table", value_name = "TABLE", requires = "file")]
    table: Option<String>,

    /// Database to select on startup
    #[arg(short = 'd', long = "database", value_name = "DB")]
    database: Option<String>,

    /// Path to the SQL script to execute (e.g. dbs-testcase/in/xxx.sql)
    #[arg(short, long, value_name = "SQL_FILE", conflicts_with_all = ["file", "table"])]
    script: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut manager = DBMS::<PAGE_NUM>::new();

    if args.init {
        // init and simply return
        run_init(&manager)?;
        return Ok(());
    }

    let mut ctx = ExecutionContext::new(&mut manager);

    if let Some(db) = args.database.as_deref() {
        ctx.use_database(db.to_string())
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
    }

    if let (Some(path), Some(table)) = (&args.file, &args.table) {
        load_file_into_table(&mut ctx, path, table)?;
        return Ok(());
    }

    if let Some(path) = args.script {
        run_script(&mut ctx, &path)?;
        return Ok(());
    }

    run_repl(&mut ctx)
}

fn run_init(dbms: &DBMS<PAGE_NUM>) -> io::Result<()> {
    for path in [&dbms.base_path, &dbms.global_path] {
        match fs::remove_dir_all(path) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        fs::create_dir_all(path)?;
    }
    let map_file = dbms.global_path.clone() + "@database_map.json";
    fs::File::create(map_file)?.write_all(b"{}")?;
    Ok(())
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

fn load_file_into_table(
    ctx: &mut ExecutionContext,
    path: &PathBuf,
    table: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let metadata = ctx
        .get_metadata(table)
        .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut rows = Vec::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() != metadata.const_info.column_type.len() {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Line {} has {} fields but table {} expects {}",
                    idx + 1,
                    parts.len(),
                    table,
                    metadata.const_info.column_type.len()
                ),
            )
            .into());
        }
        let mut parsed_row = Vec::new();
        for (raw, col_type) in parts.iter().zip(metadata.const_info.column_type.iter()) {
            parsed_row.push(parse_value_for_type(raw, *col_type)?);
        }
        rows.push(parsed_row);
    }
    ctx.insert(table.to_string(), rows)
        .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
    Ok(())
}

fn encode_value_bytes(
    raw: &str,
    col_type: ColumnType,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let raw_trimmed = raw.trim();
    if raw_trimmed.eq_ignore_ascii_case("NULL") {
        return Ok(match col_type {
            ColumnType::INT => 0i32.to_le_bytes().to_vec(),
            ColumnType::FLOAT => 0f64.to_le_bytes().to_vec(),
            ColumnType::CHAR(len) => vec![0u8; len],
        });
    }

    let literal = raw.trim_matches(|c| c == '"' || c == '\'');
    match col_type {
        ColumnType::INT => Ok(literal.trim().parse::<i32>()?.to_le_bytes().to_vec()),
        ColumnType::FLOAT => Ok(literal.trim().parse::<f64>()?.to_le_bytes().to_vec()),
        ColumnType::CHAR(len) => {
            let mut buf = vec![0u8; len];
            let bytes = literal.as_bytes();
            let n = buf.len().min(bytes.len());
            buf[..n].copy_from_slice(&bytes[..n]);
            Ok(buf)
        }
    }
}

fn parse_value_for_type(
    raw: &str,
    col_type: ColumnType,
) -> Result<Value, Box<dyn std::error::Error>> {
    let raw_trimmed = raw.trim();
    if raw_trimmed.eq_ignore_ascii_case("NULL") {
        return Ok(Value::Null);
    }
    let literal = raw.trim_matches(|c| c == '"' || c == '\'');
    let v = match col_type {
        ColumnType::INT => Value::Int(literal.trim().parse()?),
        ColumnType::FLOAT => Value::Float(literal.trim().parse()?),
        ColumnType::CHAR(_) => Value::String(literal.to_string()),
    };
    Ok(v)
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
        if delimiter.is_empty() {
            return Err("Delimiter cannot be empty".to_string());
        }
        let delim_bytes = delimiter.as_bytes();
        if delim_bytes.len() != 1 {
            return Err("Delimiter must be a single byte".to_string());
        }
        let metadata = self.get_metadata(&table)?;
        // Stream the CSV file with a bounded buffer so we never hold the whole file in memory.
        const CSV_BUF_CAP: usize = 2 * 1024 * 1024;
        let file = fs::File::open(&path).map_err(|e| format!("Failed to open file {path}: {e}"))?;
        let reader = BufReader::with_capacity(CSV_BUF_CAP, file);
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(delim_bytes[0])
            .flexible(true)
            .buffer_capacity(CSV_BUF_CAP)
            .from_reader(reader);

        let mut inserted = 0usize;
        let mut buf_record = csv::ByteRecord::new();
        const LOAD_BATCH: usize = 50_000;
        for idx in 0.. {
            if !reader
                .read_byte_record(&mut buf_record)
                .map_err(|e| format!("Failed to read record {}: {e}", idx + 1))?
            {
                break;
            }
            if buf_record.is_empty() {
                continue;
            }
            if buf_record.len() != metadata.const_info.column_type.len() {
                return Err(format!(
                    "Line {} has {} fields but table {table} expects {}",
                    idx + 1,
                    buf_record.len(),
                    metadata.const_info.column_type.len()
                ));
            }

            let mut insert_data: Vec<u8> = Vec::with_capacity(metadata.const_info.item_size);
            for (raw, col_type) in buf_record
                .iter()
                .zip(metadata.const_info.column_type.iter())
            {
                let s = std::str::from_utf8(raw)
                    .map_err(|e| format!("Line {} value error: {e}", idx + 1))?;
                let bytes = encode_value_bytes(s, *col_type)
                    .map_err(|e| format!("Line {} value error: {e}", idx + 1))?;
                insert_data.extend_from_slice(&bytes);
            }
            self.dbms
                .insert_item(&table, insert_data)
                .map_err(|e| format!("{e}"))?;
            inserted += 1;

            if inserted % LOAD_BATCH == 0 {
                self.dbms.flush_all();
                self.dbms.update_meta_json();
            }
        }
        // Persist once after bulk load to avoid per-row JSON churn
        self.dbms.flush_all();
        self.dbms.update_meta_json();

        println!("rows");
        println!("{inserted}");
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
            // Check duplicates within incoming batch first.
            let mut new_keys: Vec<Vec<ColumnValue>> = Vec::new();
            for r in &converted_rows {
                let key: Vec<ColumnValue> = pk_indices.iter().map(|&idx| r[idx].clone()).collect();
                if new_keys.iter().any(|k| *k == key) {
                    println!("!ERROR");
                    println!("duplicate");
                    return Ok(());
                }
                new_keys.push(key);
            }

            // Stream existing PKs to avoid loading entire large tables into memory.
            const DUP_SENTINEL: &str = "__PK_DUP__";
            let res = self
                .dbms
                .for_each_row(&table, Some(&pk_indices), |row_keys| {
                    if new_keys.iter().any(|k| k == row_keys) {
                        return Err(DUP_SENTINEL.to_string());
                    }
                    Ok(())
                });
            if let Err(e) = res {
                if e == DUP_SENTINEL {
                    println!("!ERROR");
                    println!("duplicate");
                    return Ok(());
                }
                return Err(e);
            }
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

        // Persist once after batch insert
        self.dbms.update_meta_json();
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
        let mut delete_rids = Vec::new();
        let table_names = vec![table.clone()];
        let metas = vec![metadata.clone()];
        let where_expr = selection.clone();

        let table_path = self.dbms.table_path(&table);
        for page_id in 0..metadata.mut_info.data_page_count {
            let mut rows_in_page: Vec<(RecordId, Vec<ColumnValue>)> = Vec::new();
            {
                let page = self.dbms.db_io.get_page(
                    &table_path,
                    page_id,
                    &dbms::resource::PageType::TABLE,
                    "",
                );
                let mut table_page = TablePage::new(&metadata.const_info, &mut page.data);
                for slot in 0..metadata.const_info.page_item_capacity {
                    if !table_page.check_slot_stat(slot) {
                        continue;
                    }
                    let item = table_page.get_item(slot);
                    let mut row = Vec::with_capacity(metadata.const_info.column_count);
                    for col_idx in 0..metadata.const_info.column_count {
                        row.push(item.get_column_val(col_idx)?);
                    }
                    let rid_val =
                        (page_id * metadata.const_info.page_item_capacity + slot) as u32;
                    rows_in_page.push((RecordId(rid_val), row));
                }
            } // drop page borrow

            for (rid, row) in rows_in_page {
                let predicate_ok = where_expr.as_ref().map_or(Ok(true), |expr| {
                    self.eval_predicate_multi(expr, &table_names, &metas, &vec![&row])
                })?;
                if !predicate_ok {
                    continue;
                }

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

        self.dbms.update_meta_json();

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
            if !selection.as_ref().map_or(Ok(true), |expr| {
                self.eval_predicate_multi(
                    expr,
                    &vec![table.clone()],
                    &vec![metadata.clone()],
                    &vec![&row],
                )
            })? {
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

        // only support up to two tables for now
        if table_names.len() > 2 {
            return Err("Only up to two tables select is supported".to_string());
        }

        let projections =
            self.resolve_projection_multi(&query.projections, &table_names, &metas)?;

        let header = projections
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>()
            .join(",");
        println!("{header}");

        // Fast path: single-table select, stream rows to avoid holding the whole table in memory.
        if table_names.len() == 1 {
            let table_name = table_names[0].clone();
            let meta = metas[0].clone();
            let (needed_cols, proj_idxs, idx_to_pos) =
                self.collect_needed_cols_single(&query, &meta, &table_name)?;
            let projections_for_cb = proj_idxs;
            let where_expr = query.r#where.clone();
            self.dbms
                .for_each_row(&table_name, Some(&needed_cols), |row| {
                    // Build only needed columns to reduce per-row allocations.
                    let vals: Vec<ColumnValue> = row.clone();

                    if let Some(ref expr) = where_expr {
                        // Evaluate using the compact value vector.
                        let getter = |col_ref: &ColumnRef| -> Result<ColumnValue, String> {
                            if col_ref.table.as_ref().map_or(true, |t| t == &table_name) {
                                let col_idx = idx_to_pos
                                    .get(
                                        &meta
                                            .const_info
                                            .column_name
                                            .iter()
                                            .position(|c| c == &col_ref.column)
                                            .ok_or_else(|| "Invalid column".to_string())?,
                                    )
                                    .ok_or_else(|| "Column not projected".to_string())?;
                                Ok(vals[*col_idx].clone())
                            } else {
                                Err("Invalid table reference".to_string())
                            }
                        };

                        let eval = |e: &Expr| -> Result<ColumnValue, String> {
                            match e {
                                Expr::Value(Value::Int(i)) => Ok(ColumnValue::INT(*i as i32)),
                                Expr::Value(Value::Float(f)) => Ok(ColumnValue::FLOAT(*f)),
                                Expr::Value(Value::String(s)) => Ok(ColumnValue::CAHR(s.clone())),
                                Expr::Column(col_ref) => getter(col_ref),
                                _ => Err("Unsupported where clause".to_string()),
                            }
                        };

                        fn eval_predicate<F>(expr: &Expr, eval: &F) -> Result<bool, String>
                        where
                            F: Fn(&Expr) -> Result<ColumnValue, String>,
                        {
                            match expr {
                                Expr::Binary { left, op, right } => {
                                    if *op == BinaryOp::And {
                                        Ok(eval_predicate(left, eval)?
                                            && eval_predicate(right, eval)?)
                                    } else {
                                        let lhs = eval(left)?;
                                        let rhs = eval(right)?;
                                        ExecutionContext::compare_values_static(*op, &lhs, &rhs)
                                    }
                                }
                                _ => Err("Unsupported where clause".to_string()),
                            }
                        }

                        if !eval_predicate(expr, &eval)? {
                            return Ok(());
                        }
                    }

                    let out = projections_for_cb
                        .iter()
                        .map(|c_idx| {
                            let pos = idx_to_pos.get(c_idx).unwrap();
                            Self::format_value(&vals[*pos])
                        })
                        .collect::<Vec<_>>()
                        .join(",");
                    println!("{out}");
                    Ok(())
                })?;
            return Ok(());
        }

        // gather rows for join (still simple nested loop)
        let mut all_rows: Vec<Vec<Vec<ColumnValue>>> = Vec::new();
        for t in &table_names {
            all_rows.push(self.dbms.scan_table_all(t)?);
        }

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

    fn collect_needed_cols_single(
        &self,
        query: &Select,
        meta: &TableMetaData,
        table_name: &str,
    ) -> Result<(Vec<usize>, Vec<usize>, HashMap<usize, usize>), String> {
        let mut needed: HashSet<usize> = HashSet::new();
        let mut proj_idxs: Vec<usize> = Vec::new();

        for item in &query.projections {
            match item {
                SelectItem::Wildcard => {
                    for i in 0..meta.const_info.column_count {
                        needed.insert(i);
                        proj_idxs.push(i);
                    }
                }
                SelectItem::Column(col_ref) => {
                    if let Some(t) = &col_ref.table {
                        if t != table_name {
                            return Err("Invalid table reference".to_string());
                        }
                    }
                    let idx = self.find_col_index(meta, &col_ref.column)?;
                    needed.insert(idx);
                    proj_idxs.push(idx);
                }
                _ => return Err("Unsupported select item".to_string()),
            }
        }

        fn collect_expr_cols(
            expr: &Expr,
            meta: &TableMetaData,
            table_name: &str,
            out: &mut HashSet<usize>,
        ) -> Result<(), String> {
            match expr {
                Expr::Binary { left, right, .. } => {
                    collect_expr_cols(left, meta, table_name, out)?;
                    collect_expr_cols(right, meta, table_name, out)?;
                }
                Expr::Column(col_ref) => {
                    if col_ref.table.as_ref().map_or(true, |t| t == table_name) {
                        let idx = meta
                            .const_info
                            .column_name
                            .iter()
                            .position(|c| c == &col_ref.column)
                            .ok_or_else(|| "Invalid column".to_string())?;
                        out.insert(idx);
                    }
                }
                _ => {}
            }
            Ok(())
        }

        if let Some(ref expr) = query.r#where {
            collect_expr_cols(expr, meta, table_name, &mut needed)?;
        }

        let mut needed_vec: Vec<usize> = needed.into_iter().collect();
        needed_vec.sort_unstable();
        let mut idx_to_pos = HashMap::new();
        for (pos, idx) in needed_vec.iter().enumerate() {
            idx_to_pos.insert(*idx, pos);
        }
        Ok((needed_vec, proj_idxs, idx_to_pos))
    }

    fn column_index_multi_static(
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

    fn value_from_expr_multi_static(
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
                let (t_idx, c_idx) = Self::column_index_multi_static(col_ref, table_names, metas)?;
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

    fn compare_values_static(
        op: BinaryOp,
        lhs: &ColumnValue,
        rhs: &ColumnValue,
    ) -> Result<bool, String> {
        let ord = match (lhs, rhs) {
            (ColumnValue::INT(a), ColumnValue::INT(b)) => a.cmp(b),
            (ColumnValue::CAHR(a), ColumnValue::CAHR(b)) => a.cmp(b),
            (ColumnValue::FLOAT(a), ColumnValue::FLOAT(b)) => {
                // floats can be NaN; treat them as incomparable
                a.partial_cmp(b)
                    .ok_or_else(|| "Invalid float comparison".to_string())?
            }
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
                    let lhs = self.value_from_expr_multi(left, table_names, metas, rows)?;
                    let rhs = self.value_from_expr_multi(right, table_names, metas, rows)?;
                    self.compare_values(*op, &lhs, &rhs)
                }
            }
            _ => Err("Unsupported where clause".to_string()),
        }
    }

    fn eval_predicate_multi_static(
        expr: &Expr,
        table_names: &[String],
        metas: &[TableMetaData],
        rows: &[&Vec<ColumnValue>],
    ) -> Result<bool, String> {
        match expr {
            Expr::Binary { left, op, right } => {
                if *op == BinaryOp::And {
                    Ok(
                        Self::eval_predicate_multi_static(left, table_names, metas, rows)?
                            && Self::eval_predicate_multi_static(right, table_names, metas, rows)?,
                    )
                } else {
                    let lhs = Self::value_from_expr_multi_static(left, table_names, metas, rows)?;
                    let rhs = Self::value_from_expr_multi_static(right, table_names, metas, rows)?;
                    Self::compare_values_static(*op, &lhs, &rhs)
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
                    let (t_idx, c_idx) = self.column_index_multi(col_ref, table_names, metas)?;
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

    fn columns_to_indices(
        &self,
        metadata: &TableMetaData,
        cols: &[String],
    ) -> Result<Vec<usize>, String> {
        cols.iter()
            .map(|c| self.find_col_index(metadata, c))
            .collect()
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
        const FOUND_BREAK: &str = "__FOUND_FK_MATCH__";
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

                let mut found = false;
                let res = self
                    .dbms
                    .for_each_row(ref_table, Some(&parent_indices), |parent_vals| {
                        let mut match_all = true;
                        for (ci, pv) in child_indices.iter().zip(parent_vals.iter()) {
                            if row[*ci] != *pv {
                                match_all = false;
                                break;
                            }
                        }
                        if match_all {
                            found = true;
                            // short-circuit by returning a sentinel error
                            return Err(FOUND_BREAK.to_string());
                        }
                        Ok(())
                    });
                if let Err(e) = res {
                    if e != FOUND_BREAK {
                        return Err(e);
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
        const FOUND_CHILD: &str = "__HAS_CHILD__";
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

                    let res =
                        self.dbms
                            .for_each_row(&child_name, Some(&child_indices), |child_vals| {
                                let mut match_all = true;
                                for (pos, pi) in parent_indices.iter().enumerate() {
                                    if child_vals[pos] != row[*pi] {
                                        match_all = false;
                                        break;
                                    }
                                }
                                if match_all {
                                    return Err(FOUND_CHILD.to_string());
                                }
                                Ok(())
                            });
                    if let Err(e) = res {
                        if e == FOUND_CHILD {
                            return Ok(true);
                        }
                        return Err(e);
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
        if columns.is_empty() {
            return Err("No column specified for index".to_string());
        }
        let meta = match self.dbms.metadata_map.get(&table) {
            Some(m) => m.clone(),
            None => return Err("Table doesn't exist".to_string()),
        };
        let col_idx = self.find_col_index(&meta, &columns[0])?;

        if meta.mut_info.column_index.contains(&col_idx) {
            return Ok(());
        }

        if let Err(_e) = self.dbms.create_index(&table, col_idx) {
            println!("!ERROR");
            println!("index");
            return Ok(());
        }
        Ok(())
    }

    fn alter_drop_index(&mut self, table: String, name: String) -> Result<(), String> {
        let meta = match self.dbms.metadata_map.get(&table) {
            Some(m) => m.clone(),
            None => return Err("Table doesn't exist".to_string()),
        };
        let col_idx = match meta.mut_info.column_index.first() {
            Some(idx) => *idx,
            None => {
                println!("!ERROR");
                println!("index");
                return Ok(());
            }
        };

        if let Err(_e) = self.dbms.delete_index(&table, col_idx) {
            println!("!ERROR");
            println!("index");
            return Ok(());
        }
        Ok(())
    }

    fn alter_drop_primary_key(
        &mut self,
        table: String,
        name: Option<String>,
    ) -> Result<(), String> {
        let meta = match self.dbms.metadata_map.get_mut(&table) {
            Some(m) => m,
            None => return Err("Table doesn't exist".to_string()),
        };

        let before = meta.const_info.column_constraint.len();
        meta.const_info
            .column_constraint
            .retain(|c| !matches!(c, TableConstraint::PrimaryKey { .. }));

        if meta.const_info.column_constraint.len() == before {
            println!("!ERROR");
            println!("primary");
            return Ok(());
        }

        self.dbms.persist_metadata();
        Ok(())
    }

    fn alter_drop_foreign_key(&mut self, table: String, name: String) -> Result<(), String> {
        let meta = match self.dbms.metadata_map.get_mut(&table) {
            Some(m) => m,
            None => return Err("Table doesn't exist".to_string()),
        };

        let before = meta.const_info.column_constraint.len();
        meta.const_info.column_constraint.retain(|c| match c {
            TableConstraint::ForeignKey {
                name: n,
                columns: _,
                ref_table: _,
                ref_columns: _,
            } => match n {
                Some(nn) => nn != &name,
                None => true,
            },
            _ => true,
        });

        if meta.const_info.column_constraint.len() == before {
            println!("!ERROR");
            println!("foreign");
            return Ok(());
        }

        self.dbms.persist_metadata();
        Ok(())
    }

    fn alter_add_primary_key(
        &mut self,
        table: String,
        constraint: TableConstraint,
    ) -> Result<(), String> {
        let meta_view = match self.dbms.metadata_map.get(&table) {
            Some(m) => m.clone(),
            None => return Err("Table doesn't exist".to_string()),
        };

        // only one primary key allowed
        if meta_view
            .const_info
            .column_constraint
            .iter()
            .any(|c| matches!(c, TableConstraint::PrimaryKey { .. }))
        {
            println!("!ERROR");
            println!("primary");
            return Ok(());
        }

        // extract columns
        let cols = match constraint {
            TableConstraint::PrimaryKey { columns, .. } => columns,
            _ => return Err("Invalid constraint".to_string()),
        };

        if cols.is_empty() {
            return Err("Primary key needs columns".to_string());
        }

        let pk_indices = self.columns_to_indices(&meta_view, &cols)?;

        // duplication check
        let rows = self.dbms.scan_table_all(&table)?;
        let mut seen: Vec<Vec<ColumnValue>> = Vec::new();
        for r in &rows {
            let key: Vec<ColumnValue> = pk_indices.iter().map(|&i| r[i].clone()).collect();
            if seen.iter().any(|k| *k == key) {
                println!("!ERROR");
                println!("duplicate");
                return Ok(());
            }
            seen.push(key);
        }

        let meta = self.dbms.metadata_map.get_mut(&table).unwrap();
        for &idx in &pk_indices {
            if idx < meta.const_info.column_not_null.len() {
                meta.const_info.column_not_null[idx] = true;
            }
        }
        meta.const_info
            .column_constraint
            .push(TableConstraint::PrimaryKey {
                name: Some("PK".to_string()),
                columns: cols,
            });
        self.dbms.persist_metadata();
        Ok(())
    }

    fn alter_add_foreign_key(
        &mut self,
        table: String,
        constraint: TableConstraint,
    ) -> Result<(), String> {
        let child_meta_view = match self.dbms.metadata_map.get(&table) {
            Some(m) => m.clone(),
            None => return Err("Table doesn't exist".to_string()),
        };

        let (columns, ref_table, ref_columns, name) = match constraint {
            TableConstraint::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                name,
            } => (columns, ref_table, ref_columns, name),
            _ => return Err("Invalid constraint".to_string()),
        };

        // ensure referenced table exists and has pk covering ref_columns
        let parent_meta = match self.dbms.metadata_map.get(&ref_table) {
            Some(m) => m.clone(),
            None => {
                println!("!ERROR");
                println!("foreign");
                return Ok(());
            }
        };

        if columns.len() != ref_columns.len() {
            println!("!ERROR");
            println!("foreign");
            return Ok(());
        }

        // check referenced columns exist
        let child_idx = self.columns_to_indices(&child_meta_view, &columns)?;
        let parent_idx = self.columns_to_indices(&parent_meta, &ref_columns)?;

        // ensure parent pk exists and matches ref columns
        let parent_pk = self.primary_key_indices(&parent_meta)?;
        if parent_pk.len() != parent_idx.len()
            || parent_idx.iter().zip(parent_pk.iter()).any(|(a, b)| a != b)
        {
            println!("!ERROR");
            println!("foreign");
            return Ok(());
        }

        // validate existing child rows
        let ref_rows = self.dbms.scan_table_all(&ref_table)?;
        let child_rows = self.dbms.scan_table_all(&table)?;
        for crow in &child_rows {
            let mut found = false;
            for prow in &ref_rows {
                let mut match_all = true;
                for (ci, pi) in child_idx.iter().zip(parent_idx.iter()) {
                    if crow[*ci] != prow[*pi] {
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
                println!("!ERROR");
                println!("foreign");
                return Ok(());
            }
        }

        let child_meta = self.dbms.metadata_map.get_mut(&table).unwrap();
        child_meta
            .const_info
            .column_constraint
            .push(TableConstraint::ForeignKey {
                name,
                columns,
                ref_table,
                ref_columns,
            });
        self.dbms.persist_metadata();
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
