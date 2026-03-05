use crate::error::Error;

#[derive(Debug)]
pub enum RespVal {
    Null,
    #[allow(dead_code)]
    Integer(i64),
    BulkString(BulkString),
    SimpleString(String),
    Array(Vec<RespVal>),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct BulkString(pub Vec<u8>);

impl BulkString {
    pub fn from_str(s: &str) -> Self {
        BulkString(s.as_bytes().to_vec())
    }

    pub fn as_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.0).ok()
    }
}

impl TryFrom<RespVal> for BulkString {
    type Error = Error;

    fn try_from(value: RespVal) -> Result<Self, Self::Error> {
        match value {
            RespVal::BulkString(bs) => Ok(bs),
            _ => Err(Error::resp_generic("BulkString expected")),
        }
    }
}

impl<'a> TryFrom<&'a BulkString> for &'a str {
    type Error = Error;

    fn try_from(value: &'a BulkString) -> Result<Self, Self::Error> {
        std::str::from_utf8(&value.0)
            .map_err(|_| Error::resp_generic("Failed to parse utf8 string"))
    }
}

impl<'a> TryFrom<&'a RespVal> for &'a str {
    type Error = Error;

    fn try_from(value: &'a RespVal) -> Result<Self, Self::Error> {
        match value {
            RespVal::BulkString(bs) => bs.try_into(),
            _ => Err(Error::resp_generic("BulkString expected")),
        }
    }
}
