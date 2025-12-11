use std::io;

//
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IOManagerError {
    #[error("IO Error: {0}")]
    IOError(#[from] io::Error), // 严重的IO类型错误
    #[error("{0}")]
    NotFoundError(String),
    #[error("{0}")]
    AlreadyExistError(String),
    #[error("unknown error")]
    Unknown,
}
