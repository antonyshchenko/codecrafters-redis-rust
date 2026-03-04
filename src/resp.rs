use crate::error::Error;
use std::io::{self};
use std::pin::Pin;
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter,
};

pub struct RespCodec<R, W> {
    reader: BufReader<R>,
    writer: BufWriter<W>,
}

impl<R: AsyncRead + Unpin + Send, W: AsyncWrite + Unpin + Send> RespCodec<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        RespCodec {
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
        }
    }

    pub async fn read_command(&mut self) -> io::Result<Option<RespCommand>> {
        match self.read_resp_type().await? {
            Some(resp_type) => Ok(Some(RespCommand::parse(resp_type)?)),
            None => Ok(None),
        }
    }

    // boxed has future has to be returned here due to recursion (ready_array calls read_resp_type)
    fn read_resp_type(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<RespType>>> + Send + '_>> {
        Box::pin(async move {
            let byte = match self.reader.read_u8().await {
                Ok(byte) => byte,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // unexpected EOF means that client has disconnected
                    return Ok(None);
                }
                Err(e) => return Err(e),
            };

            match byte {
                b'_' => Ok(Some(RespType::Null)),
                b':' => Ok(Some(RespType::Integer(self.read_integer().await?))),
                b'$' => Ok(Some(match self.read_bulk_string().await? {
                    Some(bs) => RespType::BulkString(bs),
                    None => RespType::Null,
                })),
                b'*' => Ok(Some(RespType::Array(self.read_array().await?))),
                _ => Err(invalid_data_err("Invalid data type")),
            }
        })
    }

    async fn read_integer(&mut self) -> io::Result<i64> {
        let mut bytes = Vec::new();
        self.reader.read_until(b'\n', &mut bytes).await?;
        if !bytes.ends_with(b"\r\n") {
            return Err(invalid_data_err("CRLF expected"));
        }
        bytes.truncate(bytes.len() - 2);

        std::str::from_utf8(&bytes)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
            .parse::<i64>()
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
    }

    async fn read_array(&mut self) -> io::Result<Vec<RespType>> {
        let array_len = self.read_len().await?;
        let mut array = Vec::with_capacity(array_len);
        for _ in 0..array_len {
            if let Some(resp_type) = self.read_resp_type().await? {
                array.push(resp_type)
            } else {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
        }
        Ok(array)
    }

    async fn read_len(&mut self) -> io::Result<usize> {
        let len = self.read_integer().await?;
        let len: usize = len
            .try_into()
            .map_err(|_| invalid_data_err("Invalid length"))?;
        Ok(len)
    }

    async fn read_bulk_string(&mut self) -> io::Result<Option<BulkString>> {
        let payload_len = self.read_integer().await?;
        if payload_len == -1 {
            return Ok(None);
        }

        let payload_len: usize = payload_len
            .try_into()
            .map_err(|_| invalid_data_err("Invalid length"))?;

        let mut payload = vec![0u8; payload_len];
        self.reader.read_exact(&mut payload).await?;

        let mut crlf = [0u8; 2];
        self.reader.read_exact(&mut crlf).await?;
        if &crlf != b"\r\n" {
            return Err(invalid_data_err("CRLF expected"));
        }

        Ok(Some(BulkString(payload)))
    }

    pub async fn write_bulk_str(&mut self, bs: BulkString) -> io::Result<()> {
        self.writer.write_u8(b'$').await?;
        self.writer
            .write_all(bs.0.len().to_string().as_bytes())
            .await?;
        self.writer.write_all(b"\r\n").await?;
        self.writer.write_all(&bs.0).await?;
        self.writer.write_all(b"\r\n").await
    }

    pub async fn write_bulk_str_opt(&mut self, bs: Option<BulkString>) -> io::Result<()> {
        match bs {
            Some(bs) => self.write_bulk_str(bs).await,
            None => self.writer.write_all(b"$-1\r\n").await,
        }
    }

    pub async fn write_simple_str(&mut self, s: &str) -> io::Result<()> {
        self.writer.write_u8(b'+').await?;
        self.writer.write_all(s.as_bytes()).await?;
        self.writer.write_all(b"\r\n").await
    }

    pub async fn write_err(&mut self, err: Error) -> io::Result<()> {
        self.writer.write_u8(b'-').await?;
        self.writer
            .write_all(
                match err {
                    Error::WrongType(msg) => format!("WRONGTYPE {msg}"),
                    Error::Generic(msg) => format!("ERR {msg}"),
                }
                .as_bytes(),
            )
            .await?;
        self.writer.write_all(b"\r\n").await
    }

    pub async fn write_usize(&mut self, i: usize) -> io::Result<()> {
        self.writer.write_u8(b':').await?;
        self.writer.write_all(i.to_string().as_bytes()).await?;
        self.writer.write_all(b"\r\n").await
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        self.writer.flush().await
    }
}

#[derive(Debug)]
enum RespType {
    Null,
    #[allow(dead_code)]
    Integer(i64),
    BulkString(BulkString),
    Array(Vec<RespType>),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct BulkString(Vec<u8>);

impl BulkString {
    pub fn from_str(s: &str) -> Self {
        BulkString(s.as_bytes().to_vec())
    }

    pub fn as_str(&self) -> io::Result<&str> {
        std::str::from_utf8(&self.0).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

struct CommandArgParser<I>(I);

impl CommandArgParser<std::vec::IntoIter<RespType>> {
    fn new(args_vec: Vec<RespType>) -> Self {
        CommandArgParser(args_vec.into_iter())
    }
}

impl<I: Iterator<Item = RespType>> CommandArgParser<I> {
    fn next(&mut self) -> io::Result<BulkString> {
        self.0.next().ok_or(invalid_command_args_err())?.try_into()
    }

    fn next_optional(&mut self) -> io::Result<Option<BulkString>> {
        self.0.next().map(|rt| rt.try_into()).transpose()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum RespCommand {
    Echo {
        message: BulkString,
    },
    Ping {
        message: Option<BulkString>,
    },
    Set {
        key: BulkString,
        value: BulkString,
        condition: Option<SetCmdCondition>,
        expiry: Option<SetCmdExpiry>,
    },
    Get {
        key: BulkString,
    },
    RPush {
        key: BulkString,
        elements: Vec<BulkString>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum SetCmdCondition {
    UnlessKeyExists,
    IfKeyExists,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SetCmdExpiry {
    TimeToLive { millis: u64 },
}

impl RespCommand {
    fn parse(resp_type: RespType) -> io::Result<Self> {
        match resp_type {
            RespType::Array(array) => Self::parse_from_array(array),
            _ => Err(invalid_data_err(&format!(
                "Unexpected resp type. Expeced Array, got {:?}",
                resp_type,
            ))),
        }
    }

    fn parse_from_array(value: Vec<RespType>) -> io::Result<Self> {
        let mut args = CommandArgParser::new(value);

        let command_name = args.next()?;
        let command_name = command_name.as_str()?;

        if command_name.eq_ignore_ascii_case("ECHO") {
            Ok(RespCommand::Echo {
                message: args.next()?,
            })
        } else if command_name.eq_ignore_ascii_case("PING") {
            Ok(RespCommand::Ping {
                message: args.next_optional()?,
            })
        } else if command_name.eq_ignore_ascii_case("SET") {
            let key = args.next()?;
            let value = args.next()?;
            let mut condition: Option<SetCmdCondition> = None;
            let mut expiry: Option<SetCmdExpiry> = None;

            while let Some(opt_name) = args.next_optional()? {
                let opt_name = opt_name.as_str()?;
                if opt_name.eq_ignore_ascii_case("XX") {
                    condition = Some(SetCmdCondition::IfKeyExists);
                } else if opt_name.eq_ignore_ascii_case("NX") {
                    condition = Some(SetCmdCondition::UnlessKeyExists);
                } else if opt_name.eq_ignore_ascii_case("PX") {
                    let millis = args.next()?.as_str()?.parse::<u64>().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "PX option arg must be unsigned integer",
                        )
                    })?;
                    expiry = Some(SetCmdExpiry::TimeToLive { millis });
                } else {
                    return Err(invalid_data_err(&format!("Unexpected option {}", opt_name)));
                }
            }

            Ok(RespCommand::Set {
                key,
                value,
                condition,
                expiry,
            })
        } else if command_name.eq_ignore_ascii_case("GET") {
            Ok(RespCommand::Get { key: args.next()? })
        } else if command_name.eq_ignore_ascii_case("RPUSH") {
            let key = args.next()?;
            let mut elements = Vec::new();

            while let Some(element) = args.next_optional()? {
                elements.push(element);
            }

            if elements.is_empty() {
                return Err(invalid_data_err("At least one element must be provided"));
            }

            Ok(RespCommand::RPush { key, elements })
        } else {
            Err(invalid_data_err("Command parsing failed"))
        }
    }
}

impl TryFrom<RespType> for BulkString {
    type Error = io::Error;

    fn try_from(value: RespType) -> Result<Self, Self::Error> {
        match value {
            RespType::BulkString(bs) => Ok(bs),
            _ => Err(invalid_data_err("BulkString expected")),
        }
    }
}

impl<'a> TryFrom<&'a BulkString> for &'a str {
    type Error = io::Error;

    fn try_from(value: &'a BulkString) -> Result<Self, Self::Error> {
        std::str::from_utf8(&value.0).map_err(|_| invalid_data_err("Failed to parse utf8 string"))
    }
}

impl<'a> TryFrom<&'a RespType> for &'a str {
    type Error = io::Error;

    fn try_from(value: &'a RespType) -> Result<Self, Self::Error> {
        match value {
            RespType::BulkString(bs) => bs.try_into(),
            _ => Err(invalid_data_err("BulkString expected")),
        }
    }
}

fn invalid_data_err(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

fn invalid_command_args_err() -> io::Error {
    invalid_data_err("Invalid command arguments")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_ping_command_with_no_message() -> io::Result<()> {
        let input = b"*1\r\n$4\r\nPING\r\n";
        let output = Vec::new();
        let mut codec = RespCodec::new(&input[..], output);
        let cmd = codec.read_command().await?;

        assert_eq!(Some(RespCommand::Ping { message: None }), cmd);
        Ok(())
    }

    #[tokio::test]
    async fn read_ping_command_with_message() -> io::Result<()> {
        let input = b"*2\r\n$4\r\nPING\r\n$3\r\nhey\r\n";
        let output = Vec::new();
        let mut codec = RespCodec::new(&input[..], output);
        let cmd = codec.read_command().await?;

        assert_eq!(
            Some(RespCommand::Ping {
                message: Some(BulkString("hey".as_bytes().to_vec()))
            }),
            cmd
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_echo_command() -> io::Result<()> {
        let input = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
        let output = Vec::new();
        let mut codec = RespCodec::new(&input[..], output);
        let cmd = codec.read_command().await?;

        assert_eq!(
            Some(RespCommand::Echo {
                message: BulkString::from_str("hey")
            }),
            cmd
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_set_command() -> io::Result<()> {
        let input = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let output = Vec::new();
        let mut codec = RespCodec::new(&input[..], output);
        let cmd = codec.read_command().await?;

        assert_eq!(
            Some(RespCommand::Set {
                key: BulkString::from_str("key"),
                value: BulkString::from_str("value"),
                condition: None,
                expiry: None,
            }),
            cmd
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_get_command() -> io::Result<()> {
        let input = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let output = Vec::new();
        let mut codec = RespCodec::new(&input[..], output);
        let cmd = codec.read_command().await?;

        assert_eq!(
            Some(RespCommand::Get {
                key: BulkString::from_str("key"),
            }),
            cmd
        );
        Ok(())
    }

    #[tokio::test]
    async fn command_name_is_case_insensitive() -> io::Result<()> {
        let input = b"*2\r\n$4\r\neChO\r\n$3\r\nhey\r\n";
        let output = Vec::new();
        let mut codec = RespCodec::new(&input[..], output);
        let cmd = codec.read_command().await?;

        assert_eq!(
            Some(RespCommand::Echo {
                message: BulkString::from_str("hey")
            }),
            cmd
        );
        Ok(())
    }

    #[tokio::test]
    async fn write_bulk_str() {
        let mut output = Vec::new();
        let mut codec = RespCodec::new(&b""[..], &mut output);

        codec
            .write_bulk_str(BulkString::from_str("hello"))
            .await
            .unwrap();

        codec.flush().await.unwrap();

        assert_eq!(output, b"$5\r\nhello\r\n");
    }
}
