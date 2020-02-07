use anyhow;

use crate::client::Client;
use crate::commands::{self, Options};
use crate::prompt;
use crate::repl;
use crate::server_params::PostgresAddress;


const HELP: &str = r###"
Introspection
  \l, \list-databases      list databases
  \lT, \list-scalar-types  list scalar types

Settings
  \vi                      switch to vi-mode editing
  \emacs                   switch to emacs (normal) mode editing, disables vi-mode
  \implicit-properties     print implicit properties of objects (id, type id)
  \no-implicit-properties  disable printing implicit properties

Development
  \pgaddr                  show the network addr of the postgres server
  \psql                    open psql to the current postgres process

Help
  \?                       Show help on backslash commands
"###;

pub const HINTS: &'static [&'static str] = &[
    r"\?",
    r"\emacs",
    r"\implicit-properties",
    r"\l",
    r"\lT [PATTERN]",
    r"\list-databases",
    r"\list-scalar-types [PATTERN]",
    r"\no-implicit-properties",
    r"\pgaddr",
    r"\psql",
    r"\vi",
];

pub const COMMAND_NAMES: &'static [&'static str] = &[
    r"\?",
    r"\emacs",
    r"\implicit-properties",
    r"\l",
    r"\lT",
    r"\list-databases",
    r"\list-scalar-types",
    r"\no-implicit-properties",
    r"\pgaddr",
    r"\psql",
    r"\vi",
];

pub enum Command {
    Help,
    ListDatabases,
    ListScalarTypes {
        pattern: Option<String>,
        system: bool,
        insensitive: bool,
    },
    PostgresAddr,
    Psql,
    ViMode,
    EmacsMode,
    ImplicitProperties,
    NoImplicitProperties,
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
        | ("list-databases", None)
        | ("l", None)
        => Ok(Command::ListDatabases),
        | ("list-scalar-types", pattern)
        | ("lT", pattern)
        => Ok(Command::ListScalarTypes {
            pattern: pattern.map(|x| x.to_owned()),
            system: false, // TODO(tailhook)
            insensitive: false, // TODO(tailhook)
        }),
        ("pgaddr", None) => Ok(Command::PostgresAddr),
        ("psql", None) => Ok(Command::Psql),
        ("vi", None) => Ok(Command::ViMode),
        ("emacs", None) => Ok(Command::EmacsMode),
        ("implicit-properties", None) => Ok(Command::ImplicitProperties),
        ("no-implicit-properties", None) => Ok(Command::NoImplicitProperties),
        (_, Some(_)) if COMMAND_NAMES.contains(&&s[..cmd.len()+1]) => {
            error(format_args!("Command `\\{}` doesn't support arguments",
                               cmd.escape_default()),
                  "no argument expected")
        }
        (_, _) => {
            error(format_args!("Unknown command `\\{}'", cmd.escape_default()),
                  "unknown command")
        }
    }
}

pub async fn execute<'x>(cli: &mut Client<'x>, cmd: Command,
    prompt: &mut repl::State)
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
        PostgresAddr => {
            match cli.params.get::<PostgresAddress>() {
                Some(addr) => {
                    println!("{}", serde_json::to_string_pretty(addr)?);
                }
                None => {
                    eprintln!("\\pgaddr requires EdgeDB to run in DEV mode");
                }
            }
            Ok(())
        }
        Psql => {
            commands::psql(cli, &options).await?;
            Ok(())
        }
        ViMode => {
            prompt.control.send(prompt::Control::ViMode).await;
            Ok(())
        }
        EmacsMode => {
            prompt.control.send(prompt::Control::EmacsMode).await;
            Ok(())
        }
        ImplicitProperties => {
            prompt.print.implicit_properties = true;
            Ok(())
        }
        NoImplicitProperties => {
            prompt.print.implicit_properties = true;
            Ok(())
        }
    }
}
