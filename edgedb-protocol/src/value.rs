use std::time::{SystemTime, Duration as StdDuration};

use uuid::Uuid;
use bigdecimal::BigDecimal;

use crate::codec::{NamedTupleShape, ObjectShape, EnumValue};

#[derive(Clone, Debug, PartialEq)]
pub struct Duration {
    pub positive: bool,
    pub amount: StdDuration,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Set(Vec<Value>),
    Object { shape: ObjectShape, fields: Vec<Value> },
    Scalar(Scalar),
    Tuple(Vec<Value>),
    NamedTuple { shape: NamedTupleShape, fields: Vec<Value> },
    Array(Vec<Value>),
    Enum(EnumValue),
    Nothing,
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

impl Value {
    pub fn kind(&self) -> &'static str {
        use Value::*;
        match self {
            Set(..) => "set",
            Object { .. } => "object",
            Scalar(s) => s.kind(),
            Tuple(..) => "tuple",
            NamedTuple { .. } => "named_tuple",
            Array(..) => "array",
            Enum(..) => "enum",
            Nothing => "nothing",
        }
    }
}
impl Scalar {
    pub fn kind(&self) -> &'static str {
        use Scalar::*;
        match self {
            Uuid(..) => "uuid",
            Str(..) => "string",
            Bytes(..) => "bytes",
            Int16(..) => "int16",
            Int32(..) => "int32",
            Int64(..) => "int64",
            Float32(..) => "float32",
            Float64(..) => "float64",
            Decimal(..) => "decimal",
            Bool(..) => "bool",
            Datetime(..) => "datetime",
            LocalDatetime(..) => "cal::local_datetime",
            LocalDate(..) => "cal::local_date",
            LocalTime(..) => "cal::local_time",
            Duration(..) => "duration",
            Json(..) => "json",
        }
    }
}

impl Duration {
    pub fn from_secs(val: i64) -> Duration {
        Duration {
            positive: val >= 0,
            amount: StdDuration::from_secs(val.abs() as u64),
        }
    }
    pub fn from_micros(val: i64) -> Duration {
        Duration {
            positive: val >= 0,
            amount: StdDuration::from_micros(val.abs() as u64),
        }
    }
}
