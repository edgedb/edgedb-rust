use std::str;

use snafu::{Snafu, Backtrace};
use uuid;


#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum DecodeError {
    #[snafu(display("unexpected end of frame"))]
    Underflow { backtrace: Backtrace },
    #[snafu(display("invalid utf8 when decoding string: {}", source))]
    InvalidUtf8 { backtrace: Backtrace, source: str::Utf8Error },
    #[snafu(display("invalid auth status: {:x}", auth_status))]
    AuthStatusInvalid { backtrace: Backtrace, auth_status: u8 },
    #[snafu(display("unsupported transaction state: {:x}", transaction_state))]
    InvalidTransactionState { backtrace: Backtrace, transaction_state: u8 },
    #[snafu(display("unsupported io format: {:x}", io_format))]
    InvalidIoFormat { backtrace: Backtrace, io_format: u8 },
    #[snafu(display("unsupported cardinality: {:x}", cardinality))]
    InvalidCardinality { backtrace: Backtrace, cardinality: u8 },
    #[snafu(display("unsupported describe aspect: {:x}", aspect))]
    InvalidAspect { backtrace: Backtrace, aspect: u8 },
    #[snafu(display("unsupported type descriptor: {:x}", descriptor))]
    InvalidTypeDescriptor { backtrace: Backtrace, descriptor: u8 },
    #[snafu(display("invalid uuid: {}", source))]
    InvalidUuid { backtrace: Backtrace, source: uuid::Error },
    #[doc(hidden)]
    __NonExhaustive1,
}

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum EncodeError {
    #[snafu(display("message doesn't fit 4GiB"))]
    MessageTooLong { backtrace: Backtrace },
    #[snafu(display("string is larger than 64KiB"))]
    StringTooLong { backtrace: Backtrace },
    #[snafu(display("more than 64Ki extensions"))]
    TooManyExtensions { backtrace: Backtrace },
    #[snafu(display("more than 64Ki headers"))]
    TooManyHeaders { backtrace: Backtrace },
    #[snafu(display("more than 64Ki params"))]
    TooManyParams { backtrace: Backtrace },
    #[snafu(display("more than 64Ki attributes"))]
    TooManyAttributes { backtrace: Backtrace },
    #[snafu(display("unknown message types can't be encoded"))]
    UnknownMessageCantBeEncoded { backtrace: Backtrace },
    #[doc(hidden)]
    __NonExhaustive2,
}
