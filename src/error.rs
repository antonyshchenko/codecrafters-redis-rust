use tokio::io;

#[derive(Debug)]
pub enum Error {
    IoError(io::Error),
    RespError(RespError),
}

impl Error {
    pub fn resp_generic(msg: &str) -> Error {
        Error::RespError(RespError::Generic(String::from(msg)))
    }
}

#[derive(Debug, PartialEq)]
pub enum RespError {
    WrongType(&'static str),
    Generic(String),
}

pub const OPERATION_ON_WRONG_TYPE: RespError =
    RespError::WrongType("Operation against a key holding the wrong kind of value");
