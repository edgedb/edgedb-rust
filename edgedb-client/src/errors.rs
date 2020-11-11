//! Errors that can be returned by a client
use std::fmt;
use bytes::Bytes;

/// Request has timed out or interrupted in the middle, should reconnect
#[derive(Debug, thiserror::Error)]
#[error("Connection is inconsistent state. Please reconnect.")]
pub struct ConnectionDirty;

/// Authentication error: password required
#[derive(Debug, thiserror::Error)]
#[error("Password required for the specified user/host")]
pub struct PasswordRequired;

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
