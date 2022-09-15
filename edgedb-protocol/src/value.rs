use std::iter::IntoIterator;

use bytes::Bytes;

use crate::codec::{NamedTupleShape, ObjectShape, ShapeElement};
use crate::common::{Cardinality};
use crate::model::{BigInt, Decimal, Uuid, ConfigMemory, Range};
use crate::model::{LocalDatetime, LocalDate, LocalTime, Duration, Datetime};
use crate::model::{RelativeDuration, DateDuration, Json};
pub use crate::codec::EnumValue;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Nothing,
    Uuid(Uuid),
    Str(String),
    Bytes(Bytes),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    BigInt(BigInt),
    ConfigMemory(ConfigMemory),
    Decimal(Decimal),
    Bool(bool),
    Datetime(Datetime),
    LocalDatetime(LocalDatetime),
    LocalDate(LocalDate),
    LocalTime(LocalTime),
    Duration(Duration),
    RelativeDuration(RelativeDuration),
    DateDuration(DateDuration),
    Json(Json),
    Set(Vec<Value>),
    Object { shape: ObjectShape, fields: Vec<Option<Value>> },
    SparseObject(SparseObject),
    Tuple(Vec<Value>),
    NamedTuple { shape: NamedTupleShape, fields: Vec<Value> },
    Array(Vec<Value>),
    Enum(EnumValue),
    Range(Range<Box<Value>>),
}

#[derive(Clone, Debug)]
pub struct SparseObject {
    pub(crate) shape: ObjectShape,
    pub(crate) fields: Vec<Option<Option<Value>>>,
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
            ConfigMemory(..) => "cfg::memory",
            Decimal(..) => "decimal",
            Bool(..) => "bool",
            Datetime(..) => "datetime",
            LocalDatetime(..) => "cal::local_datetime",
            LocalDate(..) => "cal::local_date",
            LocalTime(..) => "cal::local_time",
            Duration(..) => "duration",
            RelativeDuration(..) => "cal::relative_duration",
            DateDuration(..) => "cal::date_duration",
            Json(..) => "json",
            Set(..) => "set",
            Object { .. } => "object",
            SparseObject { .. } => "sparse_object",
            Tuple(..) => "tuple",
            NamedTuple { .. } => "named_tuple",
            Array(..) => "array",
            Enum(..) => "enum",
            Range{..} => "range",
        }
    }
    pub fn empty_tuple() -> Value {
        Value::Tuple(Vec::new())
    }
}

impl SparseObject {
    /// Create a new sparse object from key-value pairs
    ///
    /// Note: this method has two limitations:
    /// 1. Shape created uses `AtMostOne` cardinality for all the elements.
    /// 2. There are no extra shape elements
    /// Both of these are irrelevant when serializing the object.
    pub fn from_pairs<N: ToString, V: Into<Option<Value>>>(
        iter: impl IntoIterator<Item=(N, V)>)
        -> SparseObject
    {
        let mut elements = Vec::new();
        let mut fields = Vec::new();
        for (key, val) in iter.into_iter() {
            elements.push(ShapeElement {
                flag_implicit: false,
                flag_link_property: false,
                flag_link: false,
                cardinality: Some(Cardinality::AtMostOne),
                name: key.to_string(),
            });
            fields.push(Some(val.into()));
        }
        SparseObject {
            shape: ObjectShape::new(elements),
            fields,
        }
    }
    /// Create an empty sparse object
    pub fn empty() -> SparseObject {
        SparseObject {
            shape: ObjectShape::new(Vec::new()),
            fields: Vec::new(),
        }
    }
    pub fn pairs(&self) -> impl Iterator<Item=(&str, Option<&Value>)> {
        self.shape.0.elements.iter().zip(&self.fields).filter_map(|(el, opt)| {
            opt.as_ref().map(|opt| (&*el.name, opt.as_ref()))
        })
    }
}

impl PartialEq for SparseObject {
    fn eq(&self, other: &SparseObject) -> bool {
        let mut num = 0;
        let o = &other.shape.0.elements;
        for (el, value) in self.shape.0.elements.iter().zip(&self.fields) {
            if let Some(value) = value {
                num += 1;
                if let Some(pos) = o.iter().position(|e| e.name == el.name) {
                    if other.fields[pos].as_ref() != Some(value) {
                        return false;
                    }
                }
            }
        }
        let other_num = other.fields.iter().filter(|e| e.is_some()).count();
        return num == other_num;
    }
}

impl From<String> for Value {
    fn from(s: String) -> Value {
        Value::Str(s)
    }
}

impl From<i16> for Value {
    fn from(s: i16) -> Value {
        Value::Int16(s)
    }
}

impl From<i32> for Value {
    fn from(s: i32) -> Value {
        Value::Int32(s)
    }
}

impl From<i64> for Value {
    fn from(s: i64) -> Value {
        Value::Int64(s)
    }
}
