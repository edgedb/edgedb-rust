//! # EdgeDB Types Used for Data Modelling

mod bignum;
mod json;
mod memory;
mod time;
mod vector;

pub(crate) mod range;

pub use memory::ConfigMemory;
pub use range::Range;
pub use vector::Vector;
pub use self::bignum:: {BigInt, Decimal};
pub use self::json::Json;
pub use self::time::{LocalDatetime, LocalDate, LocalTime, Duration, Datetime};
pub use self::time::{RelativeDuration,DateDuration};
pub use uuid::Uuid;

use std::fmt;
use std::num::ParseIntError;

/// Error converting an out of range value to/from EdgeDB type.
#[derive(Debug, PartialEq)]
pub struct OutOfRangeError;

impl std::error::Error for OutOfRangeError {}
impl fmt::Display for OutOfRangeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        "value is out of range".fmt(f)
    }
}

impl From<std::num::TryFromIntError> for OutOfRangeError {
    fn from(_: std::num::TryFromIntError) -> OutOfRangeError {
        OutOfRangeError
    }
}


/// Error parsing string into EdgeDB Duration type.
#[derive(Debug, PartialEq)]
pub struct ParseDurationError {
    pub(crate) message: String,
    pub(crate) pos: usize,
    pub(crate) is_final: bool,
}

impl std::error::Error for ParseDurationError {}
impl fmt::Display for ParseDurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format!(
            "Error parsing input at position {}: {}",
            self.pos,
            self.message,
        ).fmt(f)
    }
}

impl From<std::num::ParseIntError> for ParseDurationError {
    fn from(e: ParseIntError) -> Self {
        Self::new(format!("{}", e))
    }
}

impl ParseDurationError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            pos: 0,
            message: message.into(),
            is_final: true,
        }
    }
    pub(crate) fn not_final(mut self) -> Self {
        self.is_final = false;
        self
    }
    pub(crate) fn pos(mut self, value: usize) -> Self {
        self.pos = value;
        self
    }
}
