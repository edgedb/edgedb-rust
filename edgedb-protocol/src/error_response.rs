use std::fmt;
use std::str;
use std::error::Error;
use std::collections::BTreeMap;

use bytes::Bytes;

pub use crate::server_message::{ErrorSeverity, ErrorResponse};

pub const FIELD_HINT: u16 = 0x_00_01;
pub const FIELD_DETAILS: u16 = 0x_00_02;
pub const FIELD_SERVER_TRACEBACK: u16 = 0x_01_01;

// TODO(tailhook) these might be deprecated?
pub const FIELD_POSITION_START: u16 = 0x_FF_F1;
pub const FIELD_POSITION_END: u16 = 0x_FF_F2;
pub const FIELD_LINE: u16 = 0x_FF_F3;
pub const FIELD_COLUMN: u16 = 0x_FF_F4;

pub struct DisplayError<'a>(&'a ErrorResponse, bool);
pub struct VerboseError<'a>(&'a ErrorResponse);

struct DisplayNum<'a>(Option<&'a Bytes>);

impl ErrorResponse {
    pub fn display(&self, verbose: bool) -> DisplayError {
        DisplayError(self, verbose)
    }
    pub fn display_verbose(&self) -> VerboseError {
        VerboseError(self)
    }
}

pub fn severity_marker(code: ErrorSeverity) -> &'static str {
    match code {
        ErrorSeverity::Error => "ERROR",
        ErrorSeverity::Fatal => "FATAL",
        ErrorSeverity::Panic => "PANIC",
        ErrorSeverity::Unknown(_) => "UNEXPECTED",
    }
}

pub fn error_name(code: u32) -> &'static str {
    match code {
        0x_01_00_00_00 => "InternalServerError",
        0x_02_00_00_00 => "UnsupportedFeatureError",
        0x_03_00_00_00 => "ProtocolError",
        0x_03_01_00_00 => "BinaryProtocolError",
        0x_03_01_00_01 => "UnsupportedProtocolVersionError",
        0x_03_01_00_02 => "TypeSpecNotFoundError",
        0x_03_01_00_03 => "UnexpectedMessageError",
        0x_03_02_00_00 => "InputDataError",
        0x_03_03_00_00 => "ResultCardinalityMismatchError",
        0x_04_00_00_00 => "QueryError",
        0x_04_01_00_00 => "InvalidSyntaxError",
        0x_04_01_01_00 => "EdgeQLSyntaxError",
        0x_04_01_02_00 => "SchemaSyntaxError",
        0x_04_01_03_00 => "GraphQLSyntaxError",
        0x_04_02_00_00 => "InvalidTypeError",
        0x_04_02_01_00 => "InvalidTargetError",
        0x_04_02_01_01 => "InvalidLinkTargetError",
        0x_04_02_01_02 => "InvalidPropertyTargetError",
        0x_04_03_00_00 => "InvalidReferenceError",
        0x_04_03_00_01 => "UnknownModuleError",
        0x_04_03_00_02 => "UnknownLinkError",
        0x_04_03_00_03 => "UnknownPropertyError",
        0x_04_03_00_04 => "UnknownUserError",
        0x_04_03_00_05 => "UnknownDatabaseError",
        0x_04_03_00_06 => "UnknownParameterError",
        0x_04_04_00_00 => "SchemaError",
        0x_04_05_00_00 => "SchemaDefinitionError",
        0x_04_05_01_00 => "InvalidDefinitionError",
        0x_04_05_01_01 => "InvalidModuleDefinitionError",
        0x_04_05_01_02 => "InvalidLinkDefinitionError",
        0x_04_05_01_03 => "InvalidPropertyDefinitionError",
        0x_04_05_01_04 => "InvalidUserDefinitionError",
        0x_04_05_01_05 => "InvalidDatabaseDefinitionError",
        0x_04_05_01_06 => "InvalidOperatorDefinitionError",
        0x_04_05_01_07 => "InvalidViewDefinitionError",
        0x_04_05_01_08 => "InvalidFunctionDefinitionError",
        0x_04_05_01_09 => "InvalidConstraintDefinitionError",
        0x_04_05_01_0A => "InvalidCastDefinitionError",
        0x_04_05_02_00 => "DuplicateDefinitionError",
        0x_04_05_02_01 => "DuplicateModuleDefinitionError",
        0x_04_05_02_02 => "DuplicateLinkDefinitionError",
        0x_04_05_02_03 => "DuplicatePropertyDefinitionError",
        0x_04_05_02_04 => "DuplicateUserDefinitionError",
        0x_04_05_02_05 => "DuplicateDatabaseDefinitionError",
        0x_04_05_02_06 => "DuplicateOperatorDefinitionError",
        0x_04_05_02_07 => "DuplicateViewDefinitionError",
        0x_04_05_02_08 => "DuplicateFunctionDefinitionError",
        0x_04_05_02_09 => "DuplicateConstraintDefinitionError",
        0x_04_05_02_0A => "DuplicateCastDefinitionError",
        0x_04_06_00_00 => "QueryTimeoutError",
        0x_05_00_00_00 => "ExecutionError",
        0x_05_01_00_00 => "InvalidValueError",
        0x_05_01_00_01 => "DivisionByZeroError",
        0x_05_01_00_02 => "NumericOutOfRangeError",
        0x_05_02_00_00 => "IntegrityError",
        0x_05_02_00_01 => "ConstraintViolationError",
        0x_05_02_00_02 => "CardinalityViolationError",
        0x_05_02_00_03 => "MissingRequiredError",
        0x_05_03_00_00 => "TransactionError",
        0x_05_03_01_01 => "TransactionSerializationError",
        0x_05_03_01_02 => "TransactionDeadlockError",
        0x_06_00_00_00 => "ConfigurationError",
        0x_07_00_00_00 => "AccessError",
        0x_07_01_00_00 => "AuthenticationError",
        0x_F0_00_00_00 => "LogMessage",
        0x_F0_01_00_00 => "WarningMessage",
        0x_FF_00_00_00 => "ClientError",
        0x_FF_01_00_00 => "ClientConnectionError",
        0x_FF_02_00_00 => "InterfaceError",
        0x_FF_02_01_00 => "QueryArgumentError",
        0x_FF_02_01_01 => "MissingArgumentError",
        0x_FF_02_01_02 => "UnknownArgumentError",
        0x_FF_03_00_00 => "NoDataError",

        // Backwards-compatible names
        0x_05_03_00_01 => "TransactionSerializationError",
        0x_05_03_00_02 => "TransactionDeadlockError",
        _ => "UnknownError",
    }
}

impl Error for ErrorResponse {}

impl fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.display(false).fmt(f)
    }
}

impl fmt::Display for DisplayError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let DisplayError(ref e, verbose) = self;
        write!(f, "{}: {}: {}",
            severity_marker(e.severity),
            error_name(e.code),
            e.message)?;
        if let Some(hint) = e.attributes.get(&FIELD_HINT) {
            if let Ok(hint) = str::from_utf8(hint) {
                write!(f, "\n  Hint: {}", hint)?;
            }
        }
        if let Some(detail) = e.attributes.get(&FIELD_DETAILS) {
            if let Ok(detail) = str::from_utf8(detail) {
                write!(f, "\n  Detail: {}", detail)?;
            }
        }
        if e.code == 0x_01_00_00_00 || *verbose {
            let tb = e.attributes.get(&FIELD_SERVER_TRACEBACK);
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
        writeln!(f, "Severity: {} [{}]",
            severity_marker(e.severity), e.severity.to_u8())?;
        writeln!(f, "Error type: {} [0x{:x}]", error_name(e.code), e.code)?;
        writeln!(f, "Message: {}", e.message)?;
        let mut attr = e.attributes.iter().collect::<BTreeMap<_, _>>();
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
