use crate::codec::{NamedTupleShape, ObjectShape, EnumValue};
use crate::model::{ LocalDatetime, LocalDate, LocalTime, Duration, Datetime};
use crate::model::{ BigInt, Decimal, Uuid };

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Nothing,
    Uuid(Uuid),
    Str(String),
    Bytes(Vec<u8>),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    BigInt(BigInt),
    Decimal(Decimal),
    Bool(bool),
    Datetime(Datetime),
    LocalDatetime(LocalDatetime),
    LocalDate(LocalDate),
    LocalTime(LocalTime),
    Duration(Duration),
    Json(String),  // or should we use serde::Json?
    Set(Vec<Value>),
    Object { shape: ObjectShape, fields: Vec<Option<Value>> },
    Tuple(Vec<Value>),
    NamedTuple { shape: NamedTupleShape, fields: Vec<Value> },
    Array(Vec<Value>),
    Enum(EnumValue),
}

impl Value {
    pub fn kind(&self) -> &'static str {
        use Value::*;
        match self {
            Nothing => "nothing",
            Uuid(..) => "uuid",
            Str(..) => "string",
            Bytes(..) => "bytes",
            Int16(..) => "int16",
            Int32(..) => "int32",
            Int64(..) => "int64",
            Float32(..) => "float32",
            Float64(..) => "float64",
            BigInt(..) => "bigint",
            Decimal(..) => "decimal",
            Bool(..) => "bool",
            Datetime(..) => "datetime",
            LocalDatetime(..) => "cal::local_datetime",
            LocalDate(..) => "cal::local_date",
            LocalTime(..) => "cal::local_time",
            Duration(..) => "duration",
            Json(..) => "json",
            Set(..) => "set",
            Object { .. } => "object",
            Tuple(..) => "tuple",
            NamedTuple { .. } => "named_tuple",
            Array(..) => "array",
            Enum(..) => "enum",
        }
    }
    pub fn empty_tuple() -> Value {
        Value::Tuple(Vec::new())
    }
}
