use anyhow;

use crate::client::Client;
use crate::commands::{self, Options};

const HELP: &str = r###"
Introspection
  \l, \list-databases      list databases
  \lT, \list-scalar-types  list scalar types

Help
  \?                       Show help on backslash commands
"###;

pub const HINTS: &'static [&'static str] = &[
    r"\?",
    r"\l",
    r"\list-databases",
    r"\lT [PATTERN]",
    r"\list-scalar-types [PATTERN]",
];

pub const COMMAND_NAMES: &'static [&'static str] = &[
    r"\?",
    r"\l",
    r"\list-databases",
    r"\lT",
    r"\list-scalar-types",
];

pub enum Command {
    Help,
    ListDatabases,
    ListScalarTypes {
        pattern: Option<String>,
        system: bool,
        insensitive: bool,
    },
}

pub struct ParseError {
    pub message: String,
    pub hint: String,
}

pub fn error<T, S: ToString>(message: S, hint: &str) -> Result<T, ParseError> {
    Err(ParseError {
        message: message.to_string(),
        hint: hint.into(),
    })
}

pub fn parse(s: &str) -> Result<Command, ParseError> {
    let s = s.trim_start();
    if !s.starts_with("\\") {
        return error("Backslash command must start with a backslash", "");
    }
    let cmd = s[1..].split_whitespace().next().unwrap();
    let arg = s[1+cmd.len()..].trim_start();
    let arg = if arg.len() > 0 { Some(arg) } else { None };
    match (cmd, arg) {
        ("?", None) => Ok(Command::Help),
        ("?", Some(_)) => error("Help command `\\?` doesn't support arguments",
                                "no argument expected"),
        | ("list-databases", None)
        | ("l", None)
        => Ok(Command::ListDatabases),
        | ("list-databases", Some(_))
        | ("l", Some(_)) => {
            error("Help command `\\list-databses` doesn't support arguments",
                  "no argument expected")
        }
        | ("list-scalar-types", pattern)
        | ("lT", pattern)
        => Ok(Command::ListScalarTypes {
            pattern: pattern.map(|x| x.to_owned()),
            system: false, // TODO(tailhook)
            insensitive: false, // TODO(tailhook)
        }),
        (_, _) => {
            error(format_args!("Unkown command `\\{}'", cmd.escape_default()),
                  "unknown command")
        }
    }
}

pub async fn execute<'x>(cli: &mut Client<'x>, cmd: Command)
    -> Result<(), anyhow::Error>
{
    use Command::*;
    let options = Options {
        command_line: false,
    };
    match cmd {
        Help => {
            print!("{}", HELP);
            Ok(())
        }
        ListDatabases => commands::list_databases(cli, &options).await,
        ListScalarTypes { pattern, insensitive, system } => {
            commands::list_scalar_types(cli, &options,
                &pattern, insensitive, system).await
        }
    }
}
