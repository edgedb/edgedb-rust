use std::fmt;
use std::str;

use crate::server_message::{ErrorSeverity, ErrorResponse};

const FIELD_HINT: u16 = 0x_00_01;
const FIELD_DETAILS: u16 = 0x_00_02;
const FIELD_SERVER_TRACEBACK: u16 = 0x_01_01;

// TODO(tailhook) these might be deprecated?
const FIELD_POSITION_START: u16 = 0x_FF_F1;
const FIELD_POSITION_END: u16 = 0x_FF_F2;
const FIELD_LINE: u16 = 0x_FF_F3;
const FIELD_COLUMN: u16 = 0x_FF_F4;

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
        0x_05_03_00_01 => "TransactionSerializationError",
        0x_05_03_00_02 => "TransactionDeadlockError",
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
        _ => "UnknownError",
    }
}

impl fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {}: {}",
            severity_marker(self.severity),
            error_name(self.code),
            self.message)?;
        if let Some(hint) = self.headers.get(&FIELD_HINT) {
            if let Ok(hint) = str::from_utf8(hint) {
                write!(f, "\n  Hint: {}", hint)?;
            }
        }
        if let Some(detail) = self.headers.get(&FIELD_DETAILS) {
            if let Ok(detail) = str::from_utf8(detail) {
                write!(f, "\n  Detail: {}", detail)?;
            }
        }
        if let Some(traceback) = self.headers.get(&FIELD_SERVER_TRACEBACK) {
            if let Ok(traceback) = str::from_utf8(traceback) {
                write!(f, "\n  Server traceback:")?;
                for line in traceback.lines() {
                    write!(f, "\n      {}", line)?;
                }
            }
        }
        Ok(())
    }
}
