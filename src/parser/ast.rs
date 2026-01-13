#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateDatabase { name: String },
    DropDatabase { name: String },
    ShowDatabases,
    UseDatabase { name: String },
    ShowTables,
    ShowIndexes,
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
    },
    DropTable { name: String },
    DescribeTable { name: String },
    LoadData {
        path: String,
        table: String,
        delimiter: String,
    },
    Insert {
        table: String,
        rows: Vec<Vec<Value>>,
    },
    Delete {
        table: String,
        selection: Option<Expr>,
    },
    Update {
        table: String,
        assignments: Vec<Assignment>,
        selection: Option<Expr>,
    },
    Select(Select),
    AlterAddIndex {
        table: String,
        name: Option<String>,
        columns: Vec<String>,
    },
    AlterDropIndex {
        table: String,
        name: String,
    },
    AlterDropPrimaryKey {
        table: String,
        name: Option<String>,
    },
    AlterDropForeignKey {
        table: String,
        name: String,
    },
    AlterAddPrimaryKey {
        table: String,
        constraint: TableConstraint,
    },
    AlterAddForeignKey {
        table: String,
        constraint: TableConstraint,
    },
    AlterAddUnique {
        table: String,
        constraint: TableConstraint,
    },
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: ColumnTypeDef,
    pub not_null: bool,
    pub default: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnTypeDef {
    Int,
    Varchar(usize),
    Float,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
    },
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    pub projections: Vec<SelectItem>,
    pub from: Vec<String>,
    pub r#where: Option<Expr>,
    pub group_by: Option<ColumnRef>,
    pub order_by: Option<OrderBy>,
    pub limit: Option<LimitClause>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Wildcard,
    Column(ColumnRef),
    Aggregate {
        func: Aggregator,
        column: ColumnRef,
    },
    CountAll,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Aggregator {
    Count,
    Avg,
    Max,
    Min,
    Sum,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub column: ColumnRef,
    pub asc: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LimitClause {
    pub count: i64,
    pub offset: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Value(Value),
    Column(ColumnRef),
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    IsNull {
        expr: Box<Expr>,
        not: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Value>,
    },
    InSelect {
        expr: Box<Expr>,
        select: Box<Select>,
    },
    Like {
        expr: Box<Expr>,
        pattern: String,
    },
    SubqueryCompare {
        left: ColumnRef,
        op: BinaryOp,
        select: Box<Select>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    String(String),
    Null,
}

impl Expr {
    pub fn and_chain(exprs: Vec<Expr>) -> Option<Expr> {
        let mut iter = exprs.into_iter();
        let first = iter.next()?;
        Some(iter.fold(first, |acc, rhs| Expr::Binary {
            left: Box::new(acc),
            op: BinaryOp::And,
            right: Box::new(rhs),
        }))
    }
}
