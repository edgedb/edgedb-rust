pub enum Command {
    Help,
}

pub struct Error {
    pub message: String,
    pub hint: String,
}

pub fn error<T, S: ToString>(message: S, hint: &str) -> Result<T, Error> {
    Err(Error {
        message: message.to_string(),
        hint: hint.into(),
    })
}

pub fn parse(s: &str) -> Result<Command, Error> {
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
        (_, _) => {
            error(format_args!("Unkown command `\\{}'", cmd.escape_default()),
                  "unknown command")
        }
    }
}
