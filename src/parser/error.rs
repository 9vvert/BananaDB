use lalrpop_util::ParseError;
use thiserror::Error;

use super::lexer::Tok;

#[derive(Debug, Error)]
pub enum ParserErrorKind {
    #[error("lexer error: {0}")]
    Lexer(String),
    #[error("unrecognized token")]
    UnrecognizedToken,
    #[error("unexpected end of input")]
    UnrecognizedEof,
    #[error("invalid token")]
    InvalidToken,
}

#[derive(Debug, Error)]
#[error("{kind} at {start}..{end}")]
pub struct ParserError {
    pub kind: ParserErrorKind,
    pub start: usize,
    pub end: usize,
}

impl ParserError {
    pub fn lexer(msg: impl Into<String>, start: usize, end: usize) -> Self {
        Self {
            kind: ParserErrorKind::Lexer(msg.into()),
            start,
            end,
        }
    }
}

impl From<ParseError<usize, Tok, ParserError>> for ParserError {
    fn from(err: ParseError<usize, Tok, ParserError>) -> Self {
        match err {
            ParseError::InvalidToken { location } => ParserError {
                kind: ParserErrorKind::InvalidToken,
                start: location,
                end: location,
            },
            ParseError::UnrecognizedEof { location, .. } => ParserError {
                kind: ParserErrorKind::UnrecognizedEof,
                start: location,
                end: location,
            },
            ParseError::UnrecognizedToken {
                token: (start, _, end),
                ..
            } => ParserError {
                kind: ParserErrorKind::UnrecognizedToken,
                start,
                end,
            },
            ParseError::ExtraToken {
                token: (start, _, end),
                ..
            } => ParserError {
                kind: ParserErrorKind::UnrecognizedToken,
                start,
                end,
            },
            ParseError::User { error } => error,
        }
    }
}
