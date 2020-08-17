// turn these into sub-modules later
use crate::time;
use crate::bignum;
use crate::json;

pub use self::time::{ LocalDatetime, LocalDate, LocalTime, Duration };
pub use self::bignum:: {BigInt, Decimal};
pub use self::json::Json;
pub use uuid::Uuid;

use std::fmt;

#[derive(Debug)]
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