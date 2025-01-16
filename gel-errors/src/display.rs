use std::fmt;

use crate::{Error, InternalServerError};

pub struct DisplayError<'a>(&'a Error, bool);
pub struct VerboseError<'a>(&'a Error);

struct DisplayNum(Option<usize>);

pub fn display_error(e: &Error, verbose: bool) -> DisplayError {
    DisplayError(e, verbose)
}
pub fn display_error_verbose(e: &Error) -> VerboseError {
    VerboseError(e)
}

impl fmt::Display for DisplayError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let DisplayError(ref e, verbose) = self;
        write!(f, "{:#}", e)?;
        if e.is::<InternalServerError>() || *verbose {
            if let Some(traceback) = e.server_traceback() {
                write!(f, "\n  Server traceback:")?;
                for line in traceback.lines() {
                    write!(f, "\n      {}", line)?;
                }
            }
        }
        Ok(())
    }
}

impl fmt::Display for DisplayNum {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
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
        let pstart = e.position_start();
        let pend = e.position_end();
        let line = e.line();
        let column = e.column();
        if [pstart, pend, line, column].iter().any(|x| x.is_some()) {
            writeln!(
                f,
                "Span: {}-{}, line {}, column {}",
                DisplayNum(pstart),
                DisplayNum(pend),
                DisplayNum(line),
                DisplayNum(column)
            )?;
        }
        if let Some(traceback) = e.server_traceback() {
            writeln!(f, "Server traceback:")?;
            for line in traceback.lines() {
                writeln!(f, "    {}", line)?;
            }
        }

        let attr = e.unknown_headers().collect::<Vec<_>>();
        if !attr.is_empty() {
            writeln!(f, "Other attributes:")?;
            for (k, v) in attr {
                writeln!(f, "  0x{:04x}: {:?}", k, v)?;
            }
        }
        Ok(())
    }
}
