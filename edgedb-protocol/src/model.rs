mod bignum;
mod time;
mod json;

pub use self::time::{ LocalDatetime, LocalDate, LocalTime, Duration, Datetime };
pub use self::time::{RelativeDuration};
pub use self::bignum:: {BigInt, Decimal};
pub use self::json::Json;
pub use uuid::Uuid;

use std::fmt;

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
