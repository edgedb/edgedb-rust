use std::fmt;
use std::str;
use std::collections::BTreeMap;

use bytes::Bytes;

use edgedb_errors::{Error, InternalServerError};

pub use crate::server_message::{ErrorSeverity, ErrorResponse};

pub const FIELD_HINT: u16 = 0x_00_01;
pub const FIELD_DETAILS: u16 = 0x_00_02;
pub const FIELD_SERVER_TRACEBACK: u16 = 0x_01_01;

// TODO(tailhook) these might be deprecated?
pub const FIELD_POSITION_START: u16 = 0x_FF_F1;
pub const FIELD_POSITION_END: u16 = 0x_FF_F2;
pub const FIELD_LINE: u16 = 0x_FF_F3;
pub const FIELD_COLUMN: u16 = 0x_FF_F4;

pub struct DisplayError<'a>(&'a Error, bool);
pub struct VerboseError<'a>(&'a Error);

struct DisplayNum<'a>(Option<&'a Bytes>);

pub fn display_error(e: &Error, verbose: bool) -> DisplayError {
    DisplayError(e, verbose)
}
pub fn display_error_verbose(e: &Error) -> VerboseError {
    VerboseError(e)
}

impl Into<Error> for ErrorResponse {
    fn into(self) -> Error {
        Error::from_code(self.code)
        .context(self.message)
        .with_headers(self.attributes)
    }
}

impl fmt::Display for DisplayError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let DisplayError(ref e, verbose) = self;
        write!(f, "ERROR: {:#}", e)?;
        if let Some(hint) = e.headers().get(&FIELD_HINT) {
            if let Ok(hint) = str::from_utf8(hint) {
                write!(f, "\n  Hint: {}", hint)?;
            }
        }
        if let Some(detail) = e.headers().get(&FIELD_DETAILS) {
            if let Ok(detail) = str::from_utf8(detail) {
                write!(f, "\n  Detail: {}", detail)?;
            }
        }
        if e.is::<InternalServerError>() || *verbose {
            let tb = e.headers().get(&FIELD_SERVER_TRACEBACK);
            if let Some(traceback) = tb {
                if let Ok(traceback) = str::from_utf8(traceback) {
                    write!(f, "\n  Server traceback:")?;
                    for line in traceback.lines() {
                        write!(f, "\n      {}", line)?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl fmt::Display for DisplayNum<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let num = self.0.as_ref()
            .and_then(|x| str::from_utf8(x).ok())
            .and_then(|x| x.parse::<usize>().ok());
        match num {
            Some(x) => x.fmt(f),
            None => "?".fmt(f),
        }
    }
}

impl fmt::Display for VerboseError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let e = self.0;
        writeln!(f, "Error type: {}", e.kind_debug())?;
        writeln!(f, "Message: {:#}", e)?;
        let mut attr = e.headers().iter().collect::<BTreeMap<_, _>>();
        if let Some(hint) = attr.remove(&FIELD_HINT) {
            if let Ok(hint) = str::from_utf8(hint) {
                writeln!(f, "Hint: {}", hint)?;
            }
        }
        if let Some(detail) = attr.remove(&FIELD_DETAILS) {
            if let Ok(detail) = str::from_utf8(detail) {
                writeln!(f, "Detail: {}", detail)?;
            }
        }
        if let Some(hint) = attr.remove(&FIELD_HINT) {
            if let Ok(hint) = str::from_utf8(hint) {
                writeln!(f, "Hint: {}", hint)?;
            }
        }
        let pstart = attr.remove(&FIELD_POSITION_START);
        let pend = attr.remove(&FIELD_POSITION_END);
        let line = attr.remove(&FIELD_LINE);
        let column = attr.remove(&FIELD_COLUMN);
        if [pstart, pend, line, column].iter().any(|x| x.is_some()) {
            writeln!(f, "Span: {}-{}, line {}, column {}",
                DisplayNum(pstart), DisplayNum(pend),
                DisplayNum(line), DisplayNum(column))?;
        }
        if let Some(traceback) = attr.remove(&FIELD_SERVER_TRACEBACK) {
            if let Ok(traceback) = str::from_utf8(traceback) {
                writeln!(f, "Server traceback:")?;
                for line in traceback.lines() {
                    writeln!(f, "    {}", line)?;
                }
            }
        }

        if !attr.is_empty() {
            writeln!(f, "Other attributes:")?;
            for (k, v) in attr {
                writeln!(f, "  0x{:04x}: {:?}", k, v)?;
            }
        }
        Ok(())
    }
}
