#[derive(Debug, PartialEq)]
pub enum Error {
    WrongType(&'static str),
    Generic(&'static str),
}

pub const OPERATION_ON_WRONG_TYPE: Error =
    Error::WrongType("Operation against a key holding the wrong kind of value");
