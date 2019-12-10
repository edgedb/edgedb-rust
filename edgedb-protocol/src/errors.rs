use std::str;

use snafu::{Snafu, Backtrace};
use uuid;

use crate::value::Value;


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
    #[snafu(display("invalid duration"))]
    InvalidDuration { backtrace: Backtrace },
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
    #[snafu(display("more than 64Ki authentication methods"))]
    TooManyMethods { backtrace: Backtrace },
    #[snafu(display("unknown message types can't be encoded"))]
    UnknownMessageCantBeEncoded { backtrace: Backtrace },
    #[snafu(display("trying to encode invalid value type {} with codec {}",
                    value_type, codec))]
    InvalidValue { backtrace: Backtrace,
                   value_type: &'static str, codec: &'static str },
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

pub fn invalid_value(codec: &'static str, value: &Value) -> EncodeError
{
    InvalidValue { codec, value_type: value.kind() }.fail::<()>().unwrap_err()
}
