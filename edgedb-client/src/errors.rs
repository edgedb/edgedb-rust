//! Errors that can be returned by a client
use std::fmt;
use bytes::Bytes;

pub use edgedb_errors::{Error, Tag, ErrorKind, ResultExt, kinds::*};

/// Request has timed out or interrupted in the middle, should reconnect
#[derive(Debug, thiserror::Error)]
#[error("Connection is inconsistent state. Please reconnect.")]
pub struct ConnectionDirty;

/// This error returned when trying to query a DDL statement
#[derive(Debug)]
pub struct NoResultExpected {
    pub completion_message: Bytes,
}

impl std::error::Error for NoResultExpected {}

impl fmt::Display for NoResultExpected {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "no result expected: {}",
            String::from_utf8_lossy(&self.completion_message[..]))
    }
}

/// Temporary to convert from anyhow::Error
pub trait Anyhow<T, E> {
    fn err_kind<K: ErrorKind>(self) -> Result<T, Error>;
}

impl<T> Anyhow<T, anyhow::Error> for Result<T, anyhow::Error> {
    fn err_kind<K: ErrorKind>(self) -> Result<T, Error> {
        self.map_err(|e| K::with_message(e.to_string()))
    }
}
