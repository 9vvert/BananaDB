use crate::parser::error::ParserError;
use crate::parser::error::ParserErrorKind;

#[derive(Debug, Clone, PartialEq)]
pub enum Tok {
    // symbols
    Comma,
    Semi,
    LParen,
    RParen,
    Dot,
    Star,
    Eq,
    Less,
    Greater,
    LessEqual,
    GreaterEqual,
    NotEqual,
    // literals
    Identifier(String),
    Integer(i64),
    Float(f64),
    String(String),
    Null,
    // keywords
    Create,
    Database,
    Databases,
    Drop,
    Show,
    Use,
    Tables,
    Indexes,
    Table,
    Desc,
    Load,
    Data,
    Infile,
    Into,
    Fields,
    Terminated,
    By,
    Insert,
    Values,
    Delete,
    From,
    Where,
    Update,
    Set,
    Select,
    Group,
    Order,
    Limit,
    Offset,
    Alter,
    Add,
    Index,
    Primary,
    Key,
    Foreign,
    References,
    Constraint,
    Unique,
    Default,
    Count,
    Avg,
    Max,
    Min,
    Sum,
    Int,
    Varchar,
    FloatTy,
    Like,
    In,
    Is,
    Not,
    And,
    Asc,
}

pub fn tokenize(input: &str) -> Result<Vec<(usize, Tok, usize)>, ParserError> {
    let bytes = input.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        let c = bytes[i];
        // whitespace
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }

        // comment: starts with --
        if c == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' && bytes[i] != b';' {
                i += 1;
            }
            continue;
        }

        // punctuation and operators (multi-char first)
        if c == b'<' || c == b'>' || c == b'=' {
            let start = i;
            if c == b'<' && i + 1 < bytes.len() && bytes[i + 1] == b'>' {
                i += 2;
                tokens.push((start, Tok::NotEqual, i));
                continue;
            }
            if c == b'<' && i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                i += 2;
                tokens.push((start, Tok::LessEqual, i));
                continue;
            }
            if c == b'>' && i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                i += 2;
                tokens.push((start, Tok::GreaterEqual, i));
                continue;
            }
            i += 1;
            let tok = match c {
                b'<' => Tok::Less,
                b'>' => Tok::Greater,
                _ => Tok::Eq,
            };
            tokens.push((start, tok, i));
            continue;
        }

        match c {
            b',' => {
                tokens.push((i, Tok::Comma, i + 1));
                i += 1;
                continue;
            }
            b';' => {
                tokens.push((i, Tok::Semi, i + 1));
                i += 1;
                continue;
            }
            b'(' => {
                tokens.push((i, Tok::LParen, i + 1));
                i += 1;
                continue;
            }
            b')' => {
                tokens.push((i, Tok::RParen, i + 1));
                i += 1;
                continue;
            }
            b'.' => {
                tokens.push((i, Tok::Dot, i + 1));
                i += 1;
                continue;
            }
            b'*' => {
                tokens.push((i, Tok::Star, i + 1));
                i += 1;
                continue;
            }
            _ => {}
        }

        // number (allow leading minus)
        if c.is_ascii_digit() || (c == b'-' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit())
        {
            let start = i;
            i += if c == b'-' { 1 } else { 0 };
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            let mut is_float = false;
            if i < bytes.len() && bytes[i] == b'.' {
                is_float = true;
                i += 1;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
            }
            let slice = &input[start..i];
            if is_float {
                let value: f64 = slice.parse().map_err(|e| ParserError {
                    kind: ParserErrorKind::Lexer(format!("invalid float literal: {e}")),
                    start,
                    end: i,
                })?;
                tokens.push((start, Tok::Float(value), i));
            } else {
                let value: i64 = slice.parse().map_err(|e| ParserError {
                    kind: ParserErrorKind::Lexer(format!("invalid integer literal: {e}")),
                    start,
                    end: i,
                })?;
                tokens.push((start, Tok::Integer(value), i));
            }
            continue;
        }

        // string
        if c == b'\'' {
            let start = i;
            i += 1;
            let mut value = String::new();
            let mut closed = false;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    // escaped quote represented by two single quotes
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        value.push('\'');
                        i += 2;
                        continue;
                    } else {
                        closed = true;
                        i += 1;
                        break;
                    }
                } else {
                    value.push(bytes[i] as char);
                    i += 1;
                }
            }
            if !closed {
                return Err(ParserError {
                    kind: ParserErrorKind::Lexer("unterminated string literal".to_string()),
                    start,
                    end: i,
                });
            }
            tokens.push((start, Tok::String(value), i));
            continue;
        }

        // identifier / keyword
        if is_ident_start(c) {
            let start = i;
            i += 1;
            while i < bytes.len() && is_ident_continue(bytes[i]) {
                i += 1;
            }
            let slice = &input[start..i];
            let upper = slice.to_ascii_uppercase();
            if let Some(keyword) = keyword_token(&upper, slice) {
                tokens.push((start, keyword, i));
            } else {
                tokens.push((start, Tok::Identifier(slice.to_string()), i));
            }
            continue;
        }

        return Err(ParserError {
            kind: ParserErrorKind::Lexer(format!("unexpected character: {}", c as char)),
            start: i,
            end: i + 1,
        });
    }

    Ok(tokens)
}

fn is_ident_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c == b'_'
}

fn is_ident_continue(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

fn keyword_token(upper: &str, _raw: &str) -> Option<Tok> {
    match upper {
        "CREATE" => Some(Tok::Create),
        "DATABASE" => Some(Tok::Database),
        "DATABASES" => Some(Tok::Databases),
        "DROP" => Some(Tok::Drop),
        "SHOW" => Some(Tok::Show),
        "USE" => Some(Tok::Use),
        "TABLES" => Some(Tok::Tables),
        "INDEXES" => Some(Tok::Indexes),
        "TABLE" => Some(Tok::Table),
        "DESC" => Some(Tok::Desc),
        "LOAD" => Some(Tok::Load),
        "DATA" => Some(Tok::Data),
        "INFILE" => Some(Tok::Infile),
        "INTO" => Some(Tok::Into),
        "FIELDS" => Some(Tok::Fields),
        "TERMINATED" => Some(Tok::Terminated),
        "BY" => Some(Tok::By),
        "INSERT" => Some(Tok::Insert),
        "VALUES" => Some(Tok::Values),
        "DELETE" => Some(Tok::Delete),
        "FROM" => Some(Tok::From),
        "WHERE" => Some(Tok::Where),
        "UPDATE" => Some(Tok::Update),
        "SET" => Some(Tok::Set),
        "SELECT" => Some(Tok::Select),
        "GROUP" => Some(Tok::Group),
        "ORDER" => Some(Tok::Order),
        "LIMIT" => Some(Tok::Limit),
        "OFFSET" => Some(Tok::Offset),
        "ALTER" => Some(Tok::Alter),
        "ADD" => Some(Tok::Add),
        "INDEX" => Some(Tok::Index),
        "PRIMARY" => Some(Tok::Primary),
        "KEY" => Some(Tok::Key),
        "FOREIGN" => Some(Tok::Foreign),
        "REFERENCES" => Some(Tok::References),
        "CONSTRAINT" => Some(Tok::Constraint),
        "UNIQUE" => Some(Tok::Unique),
        "DEFAULT" => Some(Tok::Default),
        "COUNT" => Some(Tok::Count),
        "AVG" => Some(Tok::Avg),
        "MAX" => Some(Tok::Max),
        "MIN" => Some(Tok::Min),
        "SUM" => Some(Tok::Sum),
        "NULL" => Some(Tok::Null),
        "INT" => Some(Tok::Int),
        "VARCHAR" => Some(Tok::Varchar),
        "FLOAT" => Some(Tok::FloatTy),
        "LIKE" => Some(Tok::Like),
        "IN" => Some(Tok::In),
        "IS" => Some(Tok::Is),
        "NOT" => Some(Tok::Not),
        "AND" => Some(Tok::And),
        "ASC" => Some(Tok::Asc),
        _ => None,
    }
}
