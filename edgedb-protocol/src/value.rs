use std::time::{SystemTime, Duration};

use uuid::Uuid;
use bigdecimal::BigDecimal;

use crate::codec::{NamedTupleShape, ObjectShape, EnumValue};


#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Set(Vec<Value>),
    Object { shape: ObjectShape, fields: Vec<Value> },
    Scalar(Scalar),
    Tuple(Vec<Value>),
    NamedTuple { shape: NamedTupleShape, fields: Vec<Value> },
    Array(Vec<Value>),
    Enum(EnumValue),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Scalar {
    Uuid(Uuid),
    Str(String),
    Bytes(Vec<u8>),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Decimal(BigDecimal),
    Bool(bool),
    Datetime(SystemTime),
    LocalDatetime(LocalDatetime),
    LocalDate(LocalDate),
    LocalTime(LocalTime),
    Duration(Duration),
    Json(String),  // or should we use serde::Json?
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalDatetime {
    // TODO(tailhook)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalDate {
    // TODO(tailhook)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalTime {
    // TODO(tailhook)
}
