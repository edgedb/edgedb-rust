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

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum CodecError {
    #[snafu(display("type position {} is absent", position))]
    UnexpectedTypePos { backtrace: Backtrace, position: u16 },
    #[snafu(display("uuid {} not found", uuid))]
    UuidNotFound { backtrace: Backtrace, uuid: uuid::Uuid },
    #[snafu(display("base scalar with uuid {} not found", uuid))]
    UndefinedBaseScalar { backtrace: Backtrace, uuid: uuid::Uuid },
    #[snafu(display("too may descriptors ({})", index))]
    TooManyDescriptors { backtrace: Backtrace, index: usize },
    #[doc(hidden)]
    __NonExhaustive3,
}
