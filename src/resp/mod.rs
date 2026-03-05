pub mod cmd;
pub mod codec;
pub mod types;

#[cfg(test)]
mod tests {
    use crate::error::Error;
    use crate::resp::cmd::RespCommand;
    use crate::resp::codec::RespCodec;
    use crate::resp::types::BulkString;

    #[tokio::test]
    async fn read_ping_command_with_no_message() -> Result<(), Error> {
        let input = b"*1\r\n$4\r\nPING\r\n";
        let output = Vec::new();
        let mut codec = RespCodec::new(&input[..], output);
        let cmd = codec.read_command().await?;

        assert_eq!(Some(RespCommand::Ping { message: None }), cmd);
        Ok(())
    }

    #[tokio::test]
    async fn read_ping_command_with_message() -> Result<(), Error> {
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
    async fn read_echo_command() -> Result<(), Error> {
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
    async fn read_set_command() -> Result<(), Error> {
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
    async fn read_get_command() -> Result<(), Error> {
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
    async fn command_name_is_case_insensitive() -> Result<(), Error> {
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
