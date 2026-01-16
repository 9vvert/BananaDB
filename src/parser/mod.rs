#![allow(clippy::result_large_err)]

pub mod ast;
pub mod error;
pub mod lexer;

pub mod sql {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/parser/sql.rs"));
}

use ast::Statement;
use error::ParserError;

/// Parse a SQL script into a list of statements.
/// The function tokenizes the input and feeds tokens into the lalrpop-generated parser.
pub fn parse_sql(input: &str) -> Result<Vec<Statement>, ParserError> {
    let tokens = lexer::tokenize(input)?;
    sql::ProgramParser::new()
        .parse(tokens.into_iter())
        .map_err(ParserError::from)
}
