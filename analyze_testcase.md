# Testcase Requirements Overview

This summarizes what each `dbs-testcase/in/*.sql` file expects and the interfaces/features needed to match the corresponding `ans/*.ans`.

## Conventions Visible in Answers
- Statements are echoed with a leading `@` line (e.g., `@USE DB;`, `@INSERT ...;`).
- Query outputs print a header line of column names (comma-separated) followed by data rows, each comma-separated on its own line.
- For commands like `SHOW DATABASES` / `SHOW TABLES` / `DESC`, a keyword line (`DATABASES`, `TABLES`, `Field,Type,Null,Default`) precedes the rows.
- Empty result sets still print the header and then no data rows.

## Required Features by Test

- **0-system**: DB-level DDL. Needs `SHOW DATABASES`, `CREATE DATABASE`, `USE`, `DROP DATABASE`. Switching between DBs must update default catalog.

- **1-table**: Table DDL per DB. Supports `CREATE TABLE` with `INT`, `VARCHAR(n)`, `FLOAT`, `NOT NULL`. `SHOW TABLES`, `DESC <table>` must list columns (name, type, nullability, default) and table-level constraints. `DROP TABLE` removes from listing.

- **2-table-data**: Full schema DDL (TPC-H–like). Must parse/execute `PRIMARY KEY` (single & composite), `FOREIGN KEY` (single & composite), nullable columns by default, larger varchar lengths. `DESC` must include PK/FK lines. Requires multiple tables in one DB.

- **2-data** (flag `data`): Bulk load: `LOAD DATA INFILE '<path>' INTO TABLE <tbl> FIELDS TERMINATED BY ',';` loading CSVs from given absolute paths. Must respect delimiters and populate tables.

- **3-query-a/b/c/d**: Basic DML and predicates on small tables.
  - `INSERT INTO <tbl> VALUES (...)` with multiple tuples.
  - `SELECT` with `*` or column list; predicates using `= <> < > <= >=` and `AND`; column-qualified names; order of rows as inserted unless filtered.
  - Character comparisons are lexicographic; allow duplicates.
  - Simple `DELETE`/`UPDATE` used in later `query-b/c/d` files; predicates same as above.

- **3-query-data-a/b** (flags `query`, depend on loaded DATASET): Queries on large tables with filters only (no joins). Must scan/filter on different columns and print selected columns.

- **4-join** / **4-join-data** (flag `join`): Cartesian join with `FROM T1, T2` and predicates in `WHERE` (equi-join). Must handle large intermediate sizes and select mixed columns from both tables.

- **6-pk** / **6-comb-pk**: Primary-key enforcement (single & composite). Reject inserts that violate PK uniqueness. `DESC` must show `PRIMARY KEY (...)`.

- **7-fk** / **7-comb-fk**: Foreign-key enforcement (single & composite). Inserting child rows requires referenced parent rows; deleting/updates that break FK should fail (no cascading defined). Error outputs in `.ans` reflect failures for bad rows.

- **8-pk/8-fk/8-comb-pk-schema/8-comb-fk-schema**: Schema validation of PK/FK declarations and `DESC` formatting for them (no data). Need parser support for these ALTER/CREATE clauses.

- **9-index-schema** (flag `index`): `ALTER TABLE ... ADD/DROP INDEX <name>(col)` plus queries using that index. Must still return correct rows with and without index; `DESC` should list `INDEX <name> (col);`. Duplicate keys permitted for secondary index. Dropping removes index metadata.

- **10-index-data**: Running queries with indexes on loaded DATASET; expect same results as table scan but efficiently. Requires index-aware filtering on range/equality.

- **10-optional**: Checkpoint marker only; implies all prior (non-optional) features must work before optional suite.

- **11-multi-join / 11-multi-join-opt**: Multi-table joins (3+ tables) using comma list and equality predicates. Optional variant may add more rows/joins but semantics same.

- **12-query-aggregate** (flag `aggregate`): Aggregations `MIN/MAX/SUM/AVG/COUNT(*)` with `WHERE` filters; grouped aggregates with `GROUP BY <col>`. Output columns are aggregates and group key.

- **12-query-group**: Group-by without aggregates in select list other than group key? (Check file: group queries on DATASET). Requires grouping + COUNT/SUM/AVG, possibly HAVING-like predicates embedded via filters.

- **12-query-order** (flag `order`): `ORDER BY` support (ascending by default) on selected columns; result order must match `.ans`.

- **12-query-nest**: Nested queries/subqueries in `IN`, comparisons of column against subquery results. Requires subquery execution returning scalar or list.

- **12-query-fuzzy** (flag `fuzzy`): `LIKE` with `%` and `_` wildcards on strings; matching must respect pattern lengths.

- **13-date** (flag `date`): `DATE` type support with validation (leap years rules, invalid dates rejected). Comparisons (=, <, >, ranges) and `DESC` reporting `DATE`. Inserts of invalid dates should fail.

- **14-unique** (flag `unique`): `UNIQUE` constraints (single and composite). Reject duplicate inserts; show constraint in `DESC`.

- **15-null** (flag `null`): Full NULL semantics.
  - Allow NULL inserts/updates for nullable columns.
  - Enforce `NOT NULL` and PK columns cannot be NULL; FK columns can be NULL if defined nullable (constraints waived when NULL).
  - Predicates `IS NULL` / `IS NOT NULL`; comparison with NULL yields empty unless explicitly checked.
  - Updating referenced FK column to NULL allowed only if column nullable; FK check skipped on NULL.

- **10-index-data/12-query-...** (performance expectation): Indexes should accelerate range/equality filters on indexed columns but correctness is primary.

## Interfaces/Operations Needed
- SQL parser/executor covering: `CREATE/DROP DATABASE`, `USE`; `CREATE/DROP TABLE`; `SHOW DATABASES|TABLES`; `DESC <table>`.
- Column types: `INT`, `FLOAT`, `VARCHAR(n)`, `DATE`, NULL/NOT NULL defaults.
- Constraints: `PRIMARY KEY`, `FOREIGN KEY` (single/composite), `UNIQUE`, `INDEX` via `ALTER TABLE ADD/DROP INDEX`, implicit indexes for PK recommended; constraint enforcement on DML.
- DML: `INSERT` (multi-row), `DELETE` with `WHERE`, `UPDATE ... SET ... WHERE ...`.
- Bulk load: `LOAD DATA INFILE ... FIELDS TERMINATED BY ','`.
- Queries: `SELECT <cols|*> FROM <tables[, ...]> [WHERE ...] [GROUP BY ...] [ORDER BY ...] [LIMIT/OFFSET]` with aggregates (`COUNT, SUM, AVG, MIN, MAX`), `LIKE`, `IN (list|subquery)`, subquery comparisons, AND chaining, simple range predicates, joins via comma + WHERE equality, nested subqueries.
- Null handling: literal `NULL`, `IS NULL/IS NOT NULL`, nullable FKs, constraint checks on NOT NULL/PK/UNIQUE.
- Date handling: parsing/validation/comparison of `YYYY-MM-DD`.
- Output formatting as observed in `ans/*.ans` (echo lines with `@`, headers before rows, numeric formatting: INT plain, FLOAT with two decimals in provided answers).
