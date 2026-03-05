use crate::error::Error;
use crate::resp::types::{BulkString, RespVal};

struct CommandArgParser<I>(I);

impl CommandArgParser<std::vec::IntoIter<RespVal>> {
    fn new(args_vec: Vec<RespVal>) -> Self {
        CommandArgParser(args_vec.into_iter())
    }
}

impl<I: Iterator<Item = RespVal>> CommandArgParser<I> {
    fn next(&mut self) -> Result<BulkString, Error> {
        self.0
            .next()
            .ok_or(Error::resp_generic("Invalid command"))?
            .try_into()
    }

    fn next_optional(&mut self) -> Result<Option<BulkString>, Error> {
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
    pub fn parse(resp_type: RespVal) -> Result<Self, Error> {
        match resp_type {
            RespVal::Array(array) => Self::parse_from_array(array),
            _ => Err(Error::resp_generic("Invalid command")),
        }
    }

    fn parse_from_array(value: Vec<RespVal>) -> Result<Self, Error> {
        let mut args = CommandArgParser::new(value);

        let command_name = args.next()?;
        let command_name = command_name
            .as_str()
            .ok_or(Error::resp_generic("Invalid command name"))?;

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
                let opt_name = opt_name
                    .as_str()
                    .ok_or(Error::resp_generic("Invalid command option"))?;
                if opt_name.eq_ignore_ascii_case("XX") {
                    condition = Some(SetCmdCondition::IfKeyExists);
                } else if opt_name.eq_ignore_ascii_case("NX") {
                    condition = Some(SetCmdCondition::UnlessKeyExists);
                } else if opt_name.eq_ignore_ascii_case("PX") {
                    let millis = args
                        .next()?
                        .as_str()
                        .ok_or(Error::resp_generic("Invalid PX value"))?
                        .parse::<u64>()
                        .map_err(|_| Error::resp_generic("PX value must be unsigned integer"))?;
                    expiry = Some(SetCmdExpiry::TimeToLive { millis });
                } else {
                    return Err(Error::resp_generic(&format!(
                        "Unexpected option {}",
                        opt_name
                    )));
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
                return Err(Error::resp_generic("At least one element must be provided"));
            }

            Ok(RespCommand::RPush { key, elements })
        } else {
            Err(Error::resp_generic("Invalid command"))
        }
    }
}
