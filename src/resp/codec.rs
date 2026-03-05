use crate::error::{Error, RespError};
use crate::resp::cmd::RespCommand;
use crate::resp::types::{BulkString, RespVal};
use std::io;
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

    pub async fn read_command(&mut self) -> Result<Option<RespCommand>, Error> {
        match self.read_resp_val().await? {
            Some(resp_val) => Ok(Some(RespCommand::parse(resp_val)?)),
            None => Ok(None),
        }
    }

    // boxed has future has to be returned here due to recursion (ready_array calls read_resp_val)
    fn read_resp_val(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<RespVal>, Error>> + Send + '_>> {
        Box::pin(async move {
            let byte = match self.reader.read_u8().await {
                Ok(byte) => byte,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // unexpected EOF means that client has disconnected
                    return Ok(None);
                }
                Err(e) => return Err(Error::IoError(e)),
            };

            match byte {
                b'_' => Ok(Some(RespVal::Null)),
                b':' => Ok(Some(RespVal::Integer(self.read_integer().await?))),
                b'$' => Ok(Some(match self.read_bulk_string().await? {
                    Some(bs) => RespVal::BulkString(bs),
                    None => RespVal::Null,
                })),
                b'*' => Ok(Some(RespVal::Array(self.read_array().await?))),
                _ => Err(Error::resp_generic("Invalid data type")),
            }
        })
    }

    async fn read_integer(&mut self) -> Result<i64, Error> {
        let mut bytes = Vec::new();
        self.reader
            .read_until(b'\n', &mut bytes)
            .await
            .map_err(|err| Error::IoError(err))?;
        if !bytes.ends_with(b"\r\n") {
            return Err(Error::resp_generic("CRLF expected"));
        }
        bytes.truncate(bytes.len() - 2);

        std::str::from_utf8(&bytes)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or(Error::resp_generic("Invalid integer"))
    }

    async fn read_array(&mut self) -> Result<Vec<RespVal>, Error> {
        let array_len = self.read_len().await?;
        let mut array = Vec::with_capacity(array_len);
        for _ in 0..array_len {
            if let Some(resp_val) = self.read_resp_val().await? {
                array.push(resp_val)
            } else {
                return Err(Error::resp_generic("Unexpected end of array"));
            }
        }
        Ok(array)
    }

    async fn read_len(&mut self) -> Result<usize, Error> {
        let len = self.read_integer().await?;
        let len: usize = len
            .try_into()
            .map_err(|_| Error::resp_generic("Invalid length"))?;
        Ok(len)
    }

    async fn read_bulk_string(&mut self) -> Result<Option<BulkString>, Error> {
        let payload_len = self.read_integer().await?;
        if payload_len == -1 {
            return Ok(None);
        }

        let payload_len: usize = payload_len
            .try_into()
            .map_err(|_| Error::resp_generic("Invalid length"))?;

        let mut payload = vec![0u8; payload_len];
        self.reader
            .read_exact(&mut payload)
            .await
            .map_err(|err| Error::IoError(err))?;

        let mut crlf = [0u8; 2];
        self.reader
            .read_exact(&mut crlf)
            .await
            .map_err(|err| Error::IoError(err))?;

        if &crlf != b"\r\n" {
            return Err(Error::resp_generic("CRLF expected"));
        }

        Ok(Some(BulkString(payload)))
    }

    pub fn write_resp_val(
        &mut self,
        rt: RespVal,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            match rt {
                RespVal::Null => self.write_null().await,
                RespVal::Integer(i) => self.write_i64(i).await,
                RespVal::BulkString(bs) => self.write_bulk_str(bs).await,
                RespVal::SimpleString(ss) => self.write_simple_str(ss.as_str()).await,
                RespVal::Array(array) => self.write_array(array).await,
            }
        })
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

    pub async fn write_null(&mut self) -> io::Result<()> {
        self.writer.write_all(b"$-1\r\n").await
    }

    pub async fn write_simple_str(&mut self, s: &str) -> io::Result<()> {
        self.writer.write_u8(b'+').await?;
        self.writer.write_all(s.as_bytes()).await?;
        self.writer.write_all(b"\r\n").await
    }

    pub async fn write_array(&mut self, array: Vec<RespVal>) -> io::Result<()> {
        self.writer.write_u8(b'*').await?;
        self.writer
            .write_all(array.len().to_string().as_bytes())
            .await?;
        self.writer.write_all(b"\r\n").await?;
        for el in array {
            self.write_resp_val(el).await?;
        }
        Ok(())
    }

    pub async fn write_err(&mut self, err: RespError) -> io::Result<()> {
        self.writer.write_u8(b'-').await?;
        self.writer
            .write_all(
                match err {
                    RespError::WrongType(msg) => format!("WRONGTYPE {msg}"),
                    RespError::Generic(msg) => format!("ERR {msg}"),
                }
                .as_bytes(),
            )
            .await?;
        self.writer.write_all(b"\r\n").await
    }

    pub async fn write_i64(&mut self, i: i64) -> io::Result<()> {
        self.writer.write_u8(b':').await?;
        self.writer.write_all(i.to_string().as_bytes()).await?;
        self.writer.write_all(b"\r\n").await
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        self.writer.flush().await
    }
}
